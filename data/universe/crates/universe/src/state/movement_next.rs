use std::sync::LazyLock;

use arrow::{
    array::{AsArray, RecordBatch},
    datatypes::{Float64Type, Int64Type},
};
use arrow_schema::DataType;
use datafusion::{
    common::JoinType,
    functions::core::expr_ext::FieldAccessor as _,
    functions_aggregate::{expr_fn::first_value, min_max::min},
    functions_window::expr_fn::row_number,
    prelude::{DataFrame, Expr, cast, col, lit},
};
use fast_paths::{FastGraph, InputGraph};
use futures::StreamExt as _;
use geo::Point;

use crate::{
    Journey,
    PersonId,
    Result,
    SimulationContext,
    functions::{h3_longlatash3, st_distance},
    test_utils::print_frame,
    // test_utils::print_frame,
};

pub struct MovementHandler {
    node_index: Vec<RecordBatch>,
    graph: FastGraph,
}

const NODE_INDEX_RESOLUTION: LazyLock<Expr> = LazyLock::new(|| lit(9_i8));

impl MovementHandler {
    pub(crate) async fn try_new(
        ctx: &SimulationContext,
        site_cells: Vec<i64>,
        resolution: Expr,
    ) -> Result<Self> {
        // let mut graphs = HashMap::new();

        // for cell_id in site_cells {
        //     let mut graph = InputGraph::new();
        //     let cell_index = CellIndex::try_from(cell_id as u64)?;

        //     let node_index = ctx
        //         .system()
        //         .routing_nodes()
        //         .await?
        //         .select([
        //             col("properties").field("osmid").alias("osmid"),
        //             h3_longlatash3()
        //                 .call(vec![
        //                     col("geometry").field("x"),
        //                     col("geometry").field("y"),
        //                     resolution.clone(),
        //                 ])
        //                 .alias("routing_cell"),
        //         ])?
        //         .filter(col("routing_cell").eq(lit(cell_id)))?
        //         .sort(vec![col("osmid").sort(true, false)])?
        //         .select([
        //             col("osmid"),
        //             cast(row_number() - lit(1_i64), DataType::Int64).alias("node_id"),
        //         ])?
        //         .cache()
        //         .await?;

        //     let edges = ctx
        //         .system()
        //         .routing_edges()
        //         .await?
        //         .select([
        //             col("properties").field("osmid_source").alias("source"),
        //             col("properties").field("osmid_target").alias("target"),
        //             col("properties").field("length").alias("length"),
        //         ])?
        //         .join_on(
        //             node_index.clone(),
        //             JoinType::Right,
        //             [col("source").eq(col("osmid"))],
        //         )?
        //         .select([col("node_id").alias("source"), col("target"), col("length")])?
        //         .join_on(
        //             node_index.clone(),
        //             JoinType::Right,
        //             [col("target").eq(col("osmid"))],
        //         )?
        //         .select([col("source"), col("node_id").alias("target"), col("length")])?;

        //     let mut edge_stream = edges.execute_stream().await?;
        //     while let Some(batch) = edge_stream.next().await {
        //         process_batch(&mut graph, batch?)?;
        //     }

        //     graph.freeze();
        //     graphs.insert(cell_index, fast_paths::prepare(&graph));
        // }

        let node_index = ctx
            .system()
            .routing_nodes()
            .await?
            .select([
                col("properties").field("osmid").alias("osmid"),
                h3_longlatash3()
                    .call(vec![
                        col("geometry").field("x"),
                        col("geometry").field("y"),
                        NODE_INDEX_RESOLUTION.clone(),
                    ])
                    .alias("node_index"),
                col("geometry"),
            ])?
            .sort(vec![col("osmid").sort(true, false)])?
            .select([
                cast(row_number() - lit(1_i64), DataType::Int64).alias("node_id"),
                col("osmid"),
                col("node_index"),
                col("geometry"),
            ])?
            .cache()
            .await?;

        let edges = ctx
            .system()
            .routing_edges()
            .await?
            .select([
                col("properties").field("osmid_source").alias("source"),
                col("properties").field("osmid_target").alias("target"),
                col("properties").field("length").alias("length"),
            ])?
            .join_on(
                node_index.clone(),
                JoinType::Right,
                [col("source").eq(col("osmid"))],
            )?
            .select([col("node_id").alias("source"), col("target"), col("length")])?
            .join_on(
                node_index.clone(),
                JoinType::Right,
                [col("target").eq(col("osmid"))],
            )?
            .select([col("source"), col("node_id").alias("target"), col("length")])?;

        let mut graph = InputGraph::new();
        let mut edge_stream = edges.execute_stream().await?;
        while let Some(batch) = edge_stream.next().await {
            process_batch(&mut graph, batch?)?;
        }
        graph.freeze();

        Ok(Self {
            node_index: node_index.collect().await?,
            graph: fast_paths::prepare(&graph),
        })
    }

    fn node_index(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.node_index.iter().cloned())?)
    }

    /// Plan journeys for persons moving through the world.
    ///
    /// The journey_data DataFrame should have the following columns:
    ///
    /// ```ignore
    /// {
    ///   person_id: bytes(16)
    ///   origin: {
    ///     x: float64
    ///     y: float64
    ///   }
    ///   destination: {
    ///     x: float64
    ///     y: float64
    ///   }
    /// }
    /// ```
    pub async fn plan_journeys(
        &mut self,
        ctx: &SimulationContext,
        journey_data: DataFrame,
    ) -> Result<Vec<(PersonId, Point, Journey)>> {
        let journey_data = journey_data.cache().await?;
        let new_journey_count = journey_data.clone().count().await?;
        println!("Planning {} new journeys", new_journey_count);

        let origin_ids = journey_data
            .clone()
            .select([
                col("person_id"),
                col("origin"),
                h3_longlatash3()
                    .call(vec![
                        col("origin").field("x"),
                        col("origin").field("y"),
                        NODE_INDEX_RESOLUTION.clone(),
                    ])
                    .alias("origin_index"),
            ])?
            .join_on(
                self.node_index(ctx)?.select([
                    col("node_index"),
                    col("node_id"),
                    col("geometry"),
                ])?,
                JoinType::Left,
                [col("origin_index").eq(col("node_index"))],
            )?
            .select([
                col("person_id"),
                col("node_id"),
                st_distance()
                    .call(vec![col("geometry"), col("origin")])
                    .alias("distance"),
            ])?
            .aggregate(
                vec![col("person_id")],
                vec![
                    first_value(col("node_id"), vec![col("distance").sort(true, false)])
                        .alias("node_id"),
                ],
            )?
            .select(vec![
                col("person_id").alias("person_id_origin"),
                col("node_id").alias("node_id_origin"),
            ])?;

        let destination_ids = journey_data
            .clone()
            .select([
                col("person_id"),
                col("destination"),
                h3_longlatash3()
                    .call(vec![
                        col("destination").field("x"),
                        col("destination").field("y"),
                        NODE_INDEX_RESOLUTION.clone(),
                    ])
                    .alias("destination_index"),
            ])?
            .join_on(
                self.node_index(ctx)?.select([
                    col("node_index"),
                    col("node_id"),
                    col("geometry"),
                ])?,
                JoinType::Left,
                [col("destination_index").eq(col("node_index"))],
            )?
            // TODO: fix distance calculations and aggreagtes. Currently the distqance calculations are
            // commented out because they don't work as expected. We do see an error with arrow extension types.
            // probably need to manually adjust field metadata for geoarrow types.
            .select([
                col("person_id"),
                col("node_id"),
                // st_distance()
                //     .call(vec![col("geometry"), col("destination")])
                //     .alias("distance"),
            ])?
            .aggregate(
                vec![col("person_id")],
                vec![min(col("node_id")).alias("node_id")],
                // vec![
                //     first_value(col("node_id"), vec![col("distance").sort(true, false)])
                //         .alias("node_id"),
                // ],
            )?
            .select(vec![
                col("person_id").alias("person_id_destination"),
                col("node_id").alias("node_id_destination"),
            ])?;

        let journey_data = journey_data
            .join_on(
                origin_ids,
                JoinType::Left,
                [col("person_id").eq(col("person_id_origin"))],
            )?
            .join_on(
                destination_ids,
                JoinType::Left,
                [col("person_id").eq(col("person_id_destination"))],
            )?
            .select_columns(&["person_id", "node_id_origin", "node_id_destination"])?;

        let mut calculator = fast_paths::create_calculator(&self.graph);

        print_frame(&journey_data).await?;

        let mut journey_stream = journey_data.execute_stream().await?;
        while let Some(Ok(batch)) = journey_stream.next().await {
            let person_ids = batch
                .column(0)
                .as_fixed_size_binary()
                .iter()
                .flat_map(|maybe_id| maybe_id.and_then(|id| uuid::Uuid::from_slice(id).ok()))
                .map(PersonId::from)
                .collect::<Vec<_>>();

            let origin_indices = batch
                .column(1)
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>();

            let destination_indices = batch
                .column(2)
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>();

            for ((person_id, origin_index), destination_index) in person_ids
                .into_iter()
                .zip(origin_indices.into_iter())
                .zip(destination_indices.into_iter())
            {
                if let (Some(origin_idx), Some(destination_idx)) = (origin_index, destination_index)
                {
                    let path = calculator.calc_path(
                        &self.graph,
                        origin_idx as usize,
                        destination_idx as usize,
                    );
                    if let Some(path) = path {
                        println!("found path!!!");
                    } else {
                        println!("no path found");
                    }
                    // if let Some(path) = path {
                    //     let legs = path
                    //         .get_nodes()
                    //         .iter()
                    //         .tuple_windows()
                    //         .flat_map(|(a, b)| {
                    //             let edge = self.routing.edge_map.get(&(*a, *b)).unwrap();
                    //             let edge = self.routing.edge(*edge);
                    //             let legs = edge
                    //                 .geometry()
                    //                 .unwrap()
                    //                 .to_line_string()
                    //                 .points()
                    //                 .tuple_windows()
                    //                 .filter_map(|(p0, p1)| {
                    //                     let distance = LatLng::new(p0.y(), p0.x())
                    //                         .ok()?
                    //                         .distance_m(LatLng::new(p1.y(), p1.x()).ok()?);
                    //                     Some(JourneyLeg {
                    //                         destination: p1,
                    //                         distance_m: distance.round().abs() as usize,
                    //                     })
                    //                 })
                    //                 .collect::<Vec<_>>();
                    //             legs.into_iter()
                    //         })
                    //         .collect();
                    //     let journey = Journey {
                    //         legs,
                    //         transport: Default::default(),
                    //         current_leg_index: 0,
                    //         current_leg_progress: 0.0,
                    //     };
                    // }
                }
            }
        }

        Ok(vec![])
    }

    // pub fn plan(
    //     &self,
    //     router: &mut PathCalculator,
    //     origin: impl AsRef<Uuid>,
    //     destination: impl AsRef<Uuid>,
    // ) -> Option<Journey> {
    //     let origin_id = self.routing.node_map.get_index_of(origin.as_ref())?;
    //     let destination_id = self.routing.node_map.get_index_of(destination.as_ref())?;
    //     let path = router.calc_path(&self.graph, origin_id, destination_id)?;
    //     Some(
    //         path.get_nodes()
    //             .iter()
    //             .tuple_windows()
    //             .flat_map(|(a, b)| {
    //                 let edge = self.routing.edge_map.get(&(*a, *b)).unwrap();
    //                 let edge = self.routing.edge(*edge);
    //                 let legs = edge
    //                     .geometry()
    //                     .unwrap()
    //                     .to_line_string()
    //                     .points()
    //                     .tuple_windows()
    //                     .filter_map(|(p0, p1)| {
    //                         let distance = LatLng::new(p0.y(), p0.x())
    //                             .ok()?
    //                             .distance_m(LatLng::new(p1.y(), p1.x()).ok()?);
    //                         Some(JourneyLeg {
    //                             destination: p1,
    //                             distance_m: distance.round().abs() as usize,
    //                         })
    //                     })
    //                     .collect::<Vec<_>>();
    //                 legs.into_iter()
    //             })
    //             .collect(),
    //     )
    // }
}

fn process_batch(graph: &mut InputGraph, batch: RecordBatch) -> Result<()> {
    let edge_iter = batch
        .column(0)
        .as_primitive::<Int64Type>()
        .iter()
        .zip(batch.column(1).as_primitive::<Int64Type>().iter())
        .zip(batch.column(2).as_primitive::<Float64Type>().iter());
    for ((source, target), length) in edge_iter {
        match (source, target, length) {
            (Some(source), Some(target), Some(length)) => {
                graph.add_edge(
                    source as usize,
                    target as usize,
                    length.round().abs() as usize,
                );
            }
            _ => {
                println!(
                    "Skipping edge with missing data: source={:?}, target={:?}, length={:?}",
                    source, target, length
                );
            }
        }
    }
    Ok(())
}

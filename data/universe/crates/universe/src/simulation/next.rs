use std::sync::Arc;

use arrow::{
    array::{AsArray, RecordBatch},
    compute::concat_batches,
    datatypes::Int64Type,
    util::pretty::print_batches,
};
use arrow_schema::DataType;
use datafusion::{
    common::{HashSet, JoinType},
    functions::core::expr_ext::FieldAccessor,
    functions_aggregate::{count::count_all, expr_fn::array_agg},
    functions_window::expr_fn::row_number,
    logical_expr::ScalarUDF,
    prelude::{DataFrame, array_element, array_length, cast, col, lit},
    scalar::ScalarValue,
};
use itertools::Itertools;

use crate::{
    EventsHelper, MovementHandler, ObjectLabel, PersonRole, PersonStatusFlag, Result,
    SimulationContext,
    agents::{KitchenHandler, PopulationHandler, functions::create_order},
    functions::h3_longlatash3,
};

pub struct SimulationRunnerBuilder {
    ctx: SimulationContext,

    create_orders: Option<Box<dyn Fn(RecordBatch) -> Arc<ScalarUDF>>>,
}

impl From<SimulationContext> for SimulationRunnerBuilder {
    fn from(ctx: SimulationContext) -> Self {
        Self {
            ctx,
            create_orders: None,
        }
    }
}

impl SimulationRunnerBuilder {
    pub fn new(ctx: SimulationContext) -> Self {
        ctx.into()
    }

    pub fn with_create_orders(
        mut self,
        create_orders: Box<dyn Fn(RecordBatch) -> Arc<ScalarUDF>>,
    ) -> Self {
        self.create_orders = Some(create_orders);
        self
    }

    pub async fn build(self) -> Result<SimulationRunner> {
        let batches = self
            .ctx
            .snapshots()
            .objects()
            .await?
            .filter(col("label").eq(lit(ObjectLabel::MenuItem.as_ref())))?
            .select([
                col("parent_id").alias("brand_id"),
                col("id").alias("menu_item_id"),
            ])?
            .collect()
            .await?;
        let order_choices = concat_batches(batches[0].schema_ref(), &batches)?;

        let create_orders = if let Some(create) = self.create_orders {
            create(order_choices)
        } else {
            create_order(order_choices)
        };

        let population = PopulationHandler::try_new(&self.ctx, create_orders).await?;
        let kitchens = KitchenHandler::try_new(&self.ctx).await?;

        // TODO: have a more explicit strategy for using h3 to henerate routing data.
        let resolution = lit(5_i8);
        let site_cell_ids = kitchens
            .sites(&self.ctx)?
            .select([h3_longlatash3()
                .call(vec![col("longitude"), col("latitude"), resolution.clone()])
                .alias("site_cell")])?
            .collect()
            .await?
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().iter())
            .flatten()
            .collect_vec();
        let movement = MovementHandler::try_new(&self.ctx, site_cell_ids, resolution).await?;

        Ok(SimulationRunner {
            ctx: self.ctx,
            population,
            kitchens,
            movement,
        })
    }
}

pub struct SimulationRunner {
    pub(crate) ctx: SimulationContext,

    pub(crate) population: PopulationHandler,
    pub(crate) kitchens: KitchenHandler,

    movement: MovementHandler,
}

impl SimulationRunner {
    pub async fn run(&mut self, steps: usize) -> Result<()> {
        tracing::info!(
            target: "caspers::simulation",
            "statrting simulation run for {} steps ({} / {})",
            steps,
            self.ctx.simulation_id(),
            self.ctx.snapshot_id()
        );

        for _ in 0..steps {
            self.step().await?;
        }

        Ok(())
    }

    pub async fn step(&mut self) -> Result<()> {
        let mut events = EventsHelper::empty(&self.ctx)?;

        self.population.advance_journeys(&self.ctx).await?;

        if let Some(picked_up_orders) = self.assign_couriers().await? {
            let picked_up_orders = picked_up_orders.cache().await?;
            let person_ids = picked_up_orders
                .clone()
                .select([col("assigned_courier")])?
                .collect()
                .await?
                .iter()
                .flat_map(|b| b.column(0).as_fixed_size_binary().iter().flatten())
                .map(|val| lit(ScalarValue::FixedSizeBinary(16, Some(val.to_vec()))))
                .collect_vec();
            self.population
                .set_person_status(&self.ctx, person_ids, PersonStatusFlag::Delivering)
                .await?;

            let journey_data = picked_up_orders.clone().select([
                col("assigned_courier").alias("person_id"),
                col("origin").alias("origin"),
                col("destination"),
            ])?;
            let journeys = self.movement.plan_journeys(&self.ctx, journey_data).await?;

            println!("new journeys: {:?}", journeys);

            self.population.start_journeys(&self.ctx, journeys).await?;
            // println!("assigned orders to couriers ----------------->");

            events = events.union(EventsHelper::order_picked_up(
                picked_up_orders.clone().select([
                    col("site_id"),
                    col("assigned_courier").alias("courier_id"),
                    col("order_id"),
                    self.ctx.current_time_expr().alias("timestamp"),
                ])?,
            )?)?;
        };

        let orders = self
            .population
            .create_orders(&self.ctx)
            .await?
            .cache()
            .await?;

        let orders_count = orders.clone().count().await?;
        let kitchen_events = if orders_count > 0 {
            self.kitchens.step(&self.ctx, Some(orders)).await?
        } else {
            self.kitchens.step(&self.ctx, None).await?
        };
        events = events.union(kitchen_events)?;

        self.ctx.step_time();
        self.send_events(events).await?;

        Ok(())
    }

    /// Assign courires to ready order for pickup.
    ///
    /// The returned DataFrame has the following schema:
    ///
    /// ```ignore
    /// {
    ///   site_id: bytes(16)
    ///   order_id: bytes(16)
    ///   submitted_at: timestamp[ms]
    ///   start_position: {
    ///     x: float64
    ///     y: float64
    ///   }
    ///   destination: {
    ///     x: float64
    ///     y: float64
    ///   }
    ///   assigned_courier: bytes(16)
    /// }
    /// ```
    async fn assign_couriers(&mut self) -> Result<Option<DataFrame>> {
        // TODO: more explicitly handle this when we consolidate location handling.
        let resolution = lit(9_i8);
        let Some(order_for_pickup) = self.kitchens.ready_orders(&self.ctx)? else {
            return Ok(None);
        };

        let order_for_pickup = order_for_pickup
            .select([
                col("site_id"),
                col("order_id"),
                col("submitted_at"),
                col("origin"),
                col("destination"),
                h3_longlatash3()
                    .call(vec![
                        col("origin").field("x"),
                        col("origin").field("y"),
                        resolution.clone(),
                    ])
                    .alias("location"),
            ])?
            .sort(vec![
                col("location").sort(true, false),
                col("submitted_at").sort(true, false),
            ])?
            .select([
                col("site_id"),
                col("order_id"),
                col("submitted_at"),
                col("origin"),
                col("destination"),
                col("location"),
                row_number().alias("driver_queue_pos"),
            ])?
            .cache()
            .await?;

        let batches = order_for_pickup.clone().collect().await?;
        let mut locations = HashSet::new();
        for batch in batches.iter() {
            for loc in batch
                .column_by_name("location")
                .expect("location")
                .as_primitive::<Int64Type>()
                .iter()
                .flatten()
            {
                locations.insert(loc);
            }
        }
        let locations = locations.into_iter().map(lit).collect_vec();

        if locations.is_empty() {
            return Ok(None);
        }

        let available_couriers = self
            .population
            .population(&self.ctx)?
            // TODO: This should filter on check-in once we have a check-in status implemented.
            .filter(
                col("role")
                    .eq(lit(PersonRole::Courier.as_ref()))
                    .and(col("status").eq(lit(PersonStatusFlag::Idle.as_ref()))),
            )?
            .select([
                col("id").alias("person_id"),
                h3_longlatash3()
                    .call(vec![
                        col("position").field("x"),
                        col("position").field("y"),
                        resolution,
                    ])
                    .alias("location"),
            ])?
            .filter(col("location").in_list(locations, false))?
            .aggregate(
                vec![col("location")],
                vec![array_agg(col("person_id")).alias("couriers")],
            )?
            .select([col("location").alias("location_queue"), col("couriers")])?;

        let assigned_orders = order_for_pickup
            .join_on(
                available_couriers,
                JoinType::Left,
                [col("location").eq(col("location_queue"))],
            )?
            .select([
                col("site_id"),
                col("order_id"),
                col("submitted_at"),
                col("origin"),
                col("destination"),
                col("driver_queue_pos"),
                col("couriers"),
            ])?
            .filter(col("driver_queue_pos").lt_eq(array_length(col("couriers"))))?
            .select([
                col("site_id"),
                col("order_id"),
                col("submitted_at"),
                col("origin"),
                col("destination"),
                array_element(
                    col("couriers"),
                    cast(col("driver_queue_pos"), DataType::Int32),
                )
                .alias("assigned_courier"),
            ])?;

        Ok(Some(assigned_orders))
    }

    async fn send_events(&self, events: DataFrame) -> Result<()> {
        let aggreagte = events.aggregate(vec![col("type")], vec![count_all()])?;
        let type_count = aggreagte.clone().count().await?;
        if type_count > 0 {
            let agg_batches = aggreagte.collect().await?;
            print_batches(&agg_batches)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn ctx(&self) -> &SimulationContext {
        &self.ctx
    }

    #[cfg(test)]
    pub(crate) fn population(&self) -> &PopulationHandler {
        &self.population
    }

    #[cfg(test)]
    pub(crate) fn advance_time(&mut self) {
        self.ctx.step_time();
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty::print_batches;
    use geo::Point;
    use rstest::*;

    use super::*;
    use crate::{
        Journey, PersonId,
        test_utils::{print_frame, runner},
    };

    #[rstest]
    #[tokio::test]
    async fn test_simulation_step(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let mut runner = runner.await?;

        print_batches(&runner.kitchens.orders)?;
        print_batches(&runner.kitchens.order_lines)?;

        runner.run(100).await?;

        print_batches(&runner.population.journeys)?;

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_simulation_runner(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let mut runner = runner.await?;

        let orders = runner.population.create_orders(&runner.ctx).await?;

        print_frame(&orders).await?;

        let person_id = PersonId::new();
        let start_position = Point::new(0.0, 0.0);
        let journey: Journey = vec![
            (Point::new(1.0, 1.0), 10000_usize),
            (Point::new(2.2, 2.0), 200_usize),
        ]
        .into_iter()
        .collect();

        runner
            .population
            .start_journeys(&runner.ctx, vec![(person_id, start_position, journey)])
            .await?;

        print_frame(&runner.population.journeys(&runner.ctx)?).await?;

        runner.population.advance_journeys(&runner.ctx).await?;

        print_frame(&runner.population.journeys(&runner.ctx)?).await?;

        runner.population.advance_journeys(&runner.ctx).await?;

        print_frame(&runner.population.journeys(&runner.ctx)?).await?;

        Ok(())
    }
}

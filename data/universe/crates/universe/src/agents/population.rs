use std::sync::{Arc, LazyLock};

use arrow::{
    array::{
        AsArray as _, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, LargeListBuilder,
        RecordBatch, StringViewBuilder, StructBuilder, TimestampMillisecondBuilder, UInt64Builder,
    },
    compute::concat_batches,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion::{
    functions::core::expr_ext::FieldAccessor as _,
    logical_expr::ScalarUDF,
    prelude::{DataFrame, Expr, array_element, array_length, case, cast, col, lit, random, round},
    scalar::ScalarValue,
};
use geo::Point;
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::PointArray;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_schema::{Dimension, PointType};
use h3o::{LatLng, Resolution};
use tracing::{Level, instrument};
use uuid::Uuid;

use crate::{
    BrandId, EntityView as _, EventPayload, MenuItemId, ObjectLabel, OrderCreatedPayload, PersonId,
    PersonRole, PersonStatusFlag, Result, SimulationContext, SiteId, State,
    agents::functions::create_order,
    state::{Journey, Transport},
};

pub struct PopulationRunner {
    create_orders: Arc<ScalarUDF>,
}

impl PopulationRunner {
    pub async fn try_new(ctx: &SimulationContext) -> Result<Self> {
        let batches = ctx
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
        let create_orders = create_order(order_choices);
        Ok(PopulationRunner { create_orders })
    }

    #[instrument(
        name = "step_population",
        level = Level::TRACE,
        skip(self, ctx, state),
        fields(
            caspers.site_id = site_id.to_string()
        )
    )]
    pub(crate) async fn step(
        &self,
        ctx: &SimulationContext,
        site_id: &SiteId,
        state: &State,
    ) -> Result<impl Iterator<Item = EventPayload>> {
        let site = state.objects().site(site_id)?;
        let props = site.properties()?;
        let lat_lng = LatLng::new(props.latitude, props.longitude)?;
        let ts = state.current_time().timestamp_millis();

        let idle_people = state
            .population()
            .idle_people_in_cell(ctx, lat_lng.to_cell(Resolution::Six), &PersonRole::Customer)
            .await?
            .collect()
            .await?;

        let idle_people = ctx.ctx().read_batches(idle_people)?;

        let orders = idle_people
            .select(vec![
                col("id"),
                self.create_orders
                    .call(vec![
                        lit(ScalarValue::TimestampMillisecond(
                            Some(ts),
                            Some("UTC".into()),
                        )),
                        col("state"),
                    ])
                    .alias("order"),
                col("position"),
            ])?
            .filter(col("order").is_not_null())?
            .select_columns(&["id", "order", "position"])?
            .collect()
            .await?;

        let orders = orders.into_iter().flat_map(|o| {
            let positions: PointArray = (
                o.column(2).as_struct(),
                PointType::new(Dimension::XY, Default::default()),
            )
                .try_into()
                .unwrap();
            let orders_iter = o
                .column(0)
                .as_fixed_size_binary()
                .iter()
                .zip(o.column(1).as_list::<i32>().iter())
                .zip(positions.iter());
            let mut orders = Vec::new();

            for ((person_id, order), pos) in orders_iter {
                if let (Some(person_id), Some(order), Some(Ok(pos))) = (person_id, order, pos) {
                    let items = order
                        .as_fixed_size_list()
                        .iter()
                        .flat_map(|it| {
                            it.map(|it2| {
                                let arr = it2.as_fixed_size_binary();
                                (
                                    BrandId::from(Uuid::from_slice(arr.value(0)).unwrap()),
                                    MenuItemId::from(Uuid::from_slice(arr.value(1)).unwrap()),
                                )
                            })
                        })
                        .collect();
                    orders.push(EventPayload::OrderCreated(OrderCreatedPayload {
                        site_id: *site_id,
                        person_id: Uuid::from_slice(person_id).unwrap().into(),
                        items,
                        destination: pos.to_point(),
                    }));
                }
            }

            orders
        });

        Ok(orders)
    }
}

// ============================================================================
// PopulationHandler - Journey tracking using DataFusion
// ============================================================================

static JOURNEY_STATE: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("person_id", DataType::FixedSizeBinary(16), false),
        Field::new("transport", DataType::Utf8View, false),
        Field::new(
            "origin",
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        ),
        Field::new(
            "journey_legs",
            DataType::LargeList(
                Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new(
                                "destination",
                                DataType::Struct(
                                    vec![
                                        Field::new("x", DataType::Float64, false),
                                        Field::new("y", DataType::Float64, false),
                                    ]
                                    .into(),
                                ),
                                false,
                            ),
                            Field::new("distance_m", DataType::UInt64, false),
                        ]
                        .into(),
                    ),
                    true,
                )
                .into(),
            ),
            false,
        ),
        Field::new("current_leg_index", DataType::UInt64, false),
        Field::new("current_leg_progress", DataType::Float64, false),
        Field::new(
            "start_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "position_history",
            DataType::LargeList(
                Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new(
                                "position",
                                DataType::Struct(
                                    vec![
                                        Field::new("x", DataType::Float64, false),
                                        Field::new("y", DataType::Float64, false),
                                    ]
                                    .into(),
                                ),
                                false,
                            ),
                            Field::new(
                                "timestamp",
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                                false,
                            ),
                        ]
                        .into(),
                    ),
                    true,
                )
                .into(),
            ),
            false,
        ),
        Field::new("is_complete", DataType::Boolean, false),
    ]))
});

struct JourneyBuilder {
    person_id: FixedSizeBinaryBuilder,
    transport: StringViewBuilder,
    start_position_x: Float64Builder,
    start_position_y: Float64Builder,
    journey_legs: LargeListBuilder<StructBuilder>,
    current_leg_index: UInt64Builder,
    current_leg_progress: Float64Builder,
    start_time: TimestampMillisecondBuilder,
    position_history: LargeListBuilder<StructBuilder>,
    is_complete: BooleanBuilder,
}

impl JourneyBuilder {
    fn new() -> Self {
        // Create the struct builder for journey_legs
        let leg_struct_builder = StructBuilder::new(
            vec![
                Field::new(
                    "destination",
                    DataType::Struct(
                        vec![
                            Field::new("x", DataType::Float64, false),
                            Field::new("y", DataType::Float64, false),
                        ]
                        .into(),
                    ),
                    false,
                ),
                Field::new("distance_m", DataType::UInt64, false),
            ],
            vec![
                Box::new(StructBuilder::new(
                    vec![
                        Field::new("x", DataType::Float64, false),
                        Field::new("y", DataType::Float64, false),
                    ],
                    vec![
                        Box::new(Float64Builder::new()),
                        Box::new(Float64Builder::new()),
                    ],
                )),
                Box::new(UInt64Builder::new()),
            ],
        );

        // Create the struct builder for position_history
        let history_struct_builder = StructBuilder::new(
            vec![
                Field::new(
                    "position",
                    DataType::Struct(
                        vec![
                            Field::new("x", DataType::Float64, false),
                            Field::new("y", DataType::Float64, false),
                        ]
                        .into(),
                    ),
                    false,
                ),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
            ],
            vec![
                Box::new(StructBuilder::new(
                    vec![
                        Field::new("x", DataType::Float64, false),
                        Field::new("y", DataType::Float64, false),
                    ],
                    vec![
                        Box::new(Float64Builder::new()),
                        Box::new(Float64Builder::new()),
                    ],
                )),
                Box::new(TimestampMillisecondBuilder::new().with_timezone("UTC")),
            ],
        );

        Self {
            person_id: FixedSizeBinaryBuilder::new(16),
            transport: StringViewBuilder::new(),
            start_position_x: Float64Builder::new(),
            start_position_y: Float64Builder::new(),
            journey_legs: LargeListBuilder::new(leg_struct_builder),
            current_leg_index: UInt64Builder::new(),
            current_leg_progress: Float64Builder::new(),
            start_time: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            position_history: LargeListBuilder::new(history_struct_builder),
            is_complete: BooleanBuilder::new(),
        }
    }

    fn add_journey(
        &mut self,
        person_id: &PersonId,
        start_position: &Point,
        journey: &Journey,
        start_time: DateTime<Utc>,
    ) -> Result<()> {
        // Add person_id
        self.person_id
            .append_value(AsRef::<[u8]>::as_ref(person_id))?;

        // Add transport (for now, we'll use the default transport as a string)
        self.transport
            .append_value(format!("{:?}", Transport::default()));

        // Add start_position
        self.start_position_x.append_value(start_position.x());
        self.start_position_y.append_value(start_position.y());

        // Add journey_legs
        let legs = journey.full_journey();
        for leg in legs {
            let leg_struct = self.journey_legs.values();

            // Add destination
            let dest_struct = leg_struct.field_builder::<StructBuilder>(0).unwrap();
            dest_struct
                .field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(leg.destination.x());
            dest_struct
                .field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(leg.destination.y());
            dest_struct.append(true);

            // Add distance_m
            leg_struct
                .field_builder::<UInt64Builder>(1)
                .unwrap()
                .append_value(leg.distance_m as u64);

            leg_struct.append(true);
        }
        self.journey_legs.append(true);

        // Add current_leg_index (start at 0)
        self.current_leg_index.append_value(0);

        // Add current_leg_progress (start at 0.0)
        self.current_leg_progress.append_value(0.0);

        // Add start_time
        self.start_time.append_value(start_time.timestamp_millis());

        // Initialize position_history with start_position
        let history_struct = self.position_history.values();

        // Add position
        let pos_struct = history_struct.field_builder::<StructBuilder>(0).unwrap();
        pos_struct
            .field_builder::<Float64Builder>(0)
            .unwrap()
            .append_value(start_position.x());
        pos_struct
            .field_builder::<Float64Builder>(1)
            .unwrap()
            .append_value(start_position.y());
        pos_struct.append(true);

        // Add timestamp
        history_struct
            .field_builder::<TimestampMillisecondBuilder>(1)
            .unwrap()
            .append_value(start_time.timestamp_millis());

        history_struct.append(true);
        self.position_history.append(true);

        // Add is_complete (false initially, unless journey has no legs)
        self.is_complete.append_value(legs.is_empty());

        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        let start_position_x = self.start_position_x.finish();
        let start_position_y = self.start_position_y.finish();

        // Manually create the struct array
        let start_position_array = arrow::array::StructArray::new(
            vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
            ]
            .into(),
            vec![Arc::new(start_position_x), Arc::new(start_position_y)],
            None,
        );

        Ok(RecordBatch::try_new(
            JOURNEY_STATE.clone(),
            vec![
                Arc::new(self.person_id.finish()),
                Arc::new(self.transport.finish()),
                Arc::new(start_position_array),
                Arc::new(self.journey_legs.finish()),
                Arc::new(self.current_leg_index.finish()),
                Arc::new(self.current_leg_progress.finish()),
                Arc::new(self.start_time.finish()),
                Arc::new(self.position_history.finish()),
                Arc::new(self.is_complete.finish()),
            ],
        )?)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PopulationStats {
    pub active_journeys: usize,
    pub completed_journeys: usize,
}

impl std::ops::Add for PopulationStats {
    type Output = PopulationStats;

    fn add(self, other: PopulationStats) -> PopulationStats {
        PopulationStats {
            active_journeys: self.active_journeys + other.active_journeys,
            completed_journeys: self.completed_journeys + other.completed_journeys,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PopulationHandler {
    create_orders: Arc<ScalarUDF>,

    population: Vec<RecordBatch>,
    pub(crate) journeys: Vec<RecordBatch>,
}

impl PopulationHandler {
    pub(crate) async fn try_new(
        ctx: &SimulationContext,
        create_orders: Arc<ScalarUDF>,
    ) -> Result<Self> {
        let population = ctx.snapshots().population().await?.collect().await?;
        Ok(PopulationHandler {
            create_orders,
            population,
            journeys: vec![RecordBatch::new_empty(JOURNEY_STATE.clone())],
        })
    }

    pub(crate) fn population(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.population.iter().cloned())?)
    }

    pub(crate) fn journeys(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.journeys.iter().cloned())?)
    }

    pub(crate) async fn create_orders(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        // TODO: adjust timestamp passed to create order function to local time. Internally
        // we always process against UTC so we just need adjust the current time to local time
        // and "pretend" its this time in UTC. The the timestamp passed to the uuidv7 function
        // should NOT be adjusted however, as we just need it to be globally ordered...
        let current_time = lit(ScalarValue::TimestampMillisecond(
            Some(ctx.current_time().timestamp_millis()),
            Some("UTC".into()),
        ));

        let step_duration = lit(ScalarValue::Float64(Some(
            ctx.time_step().as_millis() as f64
        )));

        let submitted_at_expression = current_time.clone()
            + cast(
                round(vec![step_duration * random()]),
                DataType::Duration(TimeUnit::Millisecond),
            );

        let new_orders = self
            .population(ctx)?
            .filter(
                col("status")
                    .eq(lit(PersonStatusFlag::Idle.as_ref()))
                    .and(col("role").eq(lit(PersonRole::Customer.as_ref()))),
            )?
            .select(vec![
                col("id").alias("person_id"),
                submitted_at_expression.alias("submitted_at"),
                col("position").alias("destination"),
                self.create_orders
                    .call(vec![current_time.clone(), col("state")])
                    .alias("items"),
            ])?
            .filter(col("items").is_not_null())?
            .select([
                col("person_id"),
                col("submitted_at"),
                col("destination"),
                col("items"),
            ])?;

        Ok(new_orders)
    }

    pub(crate) async fn set_person_status(
        &mut self,
        ctx: &SimulationContext,
        person_ids: Vec<Expr>,
        status: PersonStatusFlag,
    ) -> Result<()> {
        self.population = self
            .population(ctx)?
            .select(vec![
                col("id"),
                col("role"),
                case(col("id").in_list(person_ids.clone(), false))
                    .when(lit(true), lit(status.as_ref()))
                    .otherwise(col("status"))?
                    .alias("status"),
                col("properties"),
                col("position"),
                col("state"),
            ])?
            .collect()
            .await?;
        Ok(())
    }

    pub(crate) async fn start_journeys(
        &mut self,
        ctx: &SimulationContext,
        new_journeys: impl IntoIterator<Item = (PersonId, Point, Journey)>,
    ) -> Result<()> {
        let mut builder = JourneyBuilder::new();
        let current_time = ctx.current_time();

        for (person_id, start_position, journey) in new_journeys {
            builder.add_journey(&person_id, &start_position, &journey, *current_time)?;
        }

        let new_batch = builder.finish()?;

        // Only update if we have new journeys
        if new_batch.num_rows() > 0 {
            self.journeys = self
                .journeys(ctx)?
                .union(ctx.ctx().read_batch(new_batch)?)?
                .collect()
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn get_stats(&self, ctx: &SimulationContext) -> Result<PopulationStats> {
        // Handle empty journeys
        if self.journeys.is_empty() || self.journeys.iter().all(|b| b.num_rows() == 0) {
            return Ok(PopulationStats {
                active_journeys: 0,
                completed_journeys: 0,
            });
        }

        let journeys = self.journeys(ctx)?;

        // Count active and completed journeys
        let stats_df = journeys
            .aggregate(
                vec![],
                vec![
                    datafusion::functions_aggregate::expr_fn::count(
                        datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
                            None,
                            vec![(
                                Box::new(col("is_complete").eq(lit(false))),
                                Box::new(lit(1_i64)),
                            )],
                            None,
                        )),
                    )
                    .alias("active"),
                    datafusion::functions_aggregate::expr_fn::count(
                        datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
                            None,
                            vec![(
                                Box::new(col("is_complete").eq(lit(true))),
                                Box::new(lit(1_i64)),
                            )],
                            None,
                        )),
                    )
                    .alias("completed"),
                ],
            )?
            .collect()
            .await?;

        let batch = &stats_df[0];
        let active = batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0) as usize;
        let completed = batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0) as usize;

        Ok(PopulationStats {
            active_journeys: active,
            completed_journeys: completed,
        })
    }

    pub(crate) async fn advance_journeys(&mut self, ctx: &SimulationContext) -> Result<()> {
        // Early return if no journeys to process
        if self.journeys.is_empty() || self.journeys.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }

        let base_journeys = self.journeys(ctx)?;

        // Filter for non-complete journeys
        let active_journeys = base_journeys
            .clone()
            .filter(col("is_complete").eq(lit(false)))?;

        let time_step_seconds = lit(ctx.time_step().as_secs_f64());

        // Calculate distance to travel (simplified: using default transport velocity)
        // For now, we'll use a constant velocity. Later this can be made dynamic with UDFs
        let velocity_m_s = lit(Transport::default().default_velocity_m_s());
        let distance_to_travel = velocity_m_s * time_step_seconds;

        // Get current leg info (1-indexed for array_element)
        let current_leg_idx_expr = cast(col("current_leg_index") + lit(1_u64), DataType::Int64);
        let current_leg = array_element(col("journey_legs"), current_leg_idx_expr.clone());
        let current_leg_distance = current_leg.clone().field("distance_m");

        // Calculate remaining distance in current leg
        let remaining_in_leg = cast(current_leg_distance.clone(), DataType::Float64)
            * (lit(1.0) - col("current_leg_progress"));

        // Determine if we complete the current leg
        let completes_leg = distance_to_travel.clone().gt_eq(remaining_in_leg.clone());

        // Calculate new leg index and progress
        let new_leg_index = datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
            None,
            vec![(
                Box::new(completes_leg.clone()),
                Box::new(col("current_leg_index") + lit(1_u64)),
            )],
            Some(Box::new(col("current_leg_index"))),
        ));

        let new_progress = datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
            None,
            vec![(Box::new(completes_leg.clone()), Box::new(lit(0.0)))],
            Some(Box::new(
                col("current_leg_progress")
                    + (distance_to_travel.clone()
                        / cast(current_leg_distance.clone(), DataType::Float64)),
            )),
        ));

        // Determine if journey is complete
        let total_legs = array_length(col("journey_legs"));
        let is_now_complete = new_leg_index.clone().gt_eq(total_legs.clone());

        // Calculate current position for interpolation
        // Get destination of current leg
        let current_dest_x = current_leg.clone().field("destination").field("x");
        let current_dest_y = current_leg.clone().field("destination").field("y");

        // Get previous position (either start_position or previous leg's destination)
        let prev_leg_idx_expr = cast(col("current_leg_index"), DataType::Int64);
        let prev_leg = array_element(col("journey_legs"), prev_leg_idx_expr.clone());
        let prev_dest_x = prev_leg.clone().field("destination").field("x");
        let prev_dest_y = prev_leg.clone().field("destination").field("y");

        // Use start_position if current_leg_index == 0, otherwise use previous leg destination
        let start_x = datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
            None,
            vec![(
                Box::new(col("current_leg_index").eq(lit(0_u64))),
                Box::new(col("origin").field("x")),
            )],
            Some(Box::new(prev_dest_x)),
        ));

        let start_y = datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
            None,
            vec![(
                Box::new(col("current_leg_index").eq(lit(0_u64))),
                Box::new(col("origin").field("y")),
            )],
            Some(Box::new(prev_dest_y)),
        ));

        // For now, we'll skip updating position_history via DataFusion as it's complex
        // We'll handle this in a separate step or in Rust code
        // TODO: Implement position_history updates

        let updated_journeys = active_journeys.select(vec![
            col("person_id"),
            col("transport"),
            col("origin"),
            col("journey_legs"),
            new_leg_index.alias("current_leg_index"),
            new_progress.alias("current_leg_progress"),
            col("start_time"),
            col("position_history"), // For now, keeping as-is
            is_now_complete.alias("is_complete"),
        ])?;

        let updated_journey_count = updated_journeys.clone().count().await?;
        println!("Advanced {} active journeys", updated_journey_count);

        // Union with already complete journeys
        let complete_journeys = base_journeys.filter(col("is_complete").eq(lit(true)))?;

        self.journeys = updated_journeys.union(complete_journeys)?.collect().await?;

        Ok(())
    }

    pub(crate) async fn get_completed_journeys(
        &mut self,
        ctx: &SimulationContext,
    ) -> Result<Option<DataFrame>> {
        // Filter for completed journeys
        let completed = self
            .journeys(ctx)?
            .filter(col("is_complete").eq(lit(true)))?
            .collect()
            .await?;

        let completed_count = completed.iter().map(|b| b.num_rows()).sum::<usize>();
        if completed_count == 0 {
            return Ok(None);
        }

        // Remove completed journeys from active journeys
        self.journeys = self
            .journeys(ctx)?
            .filter(col("is_complete").eq(lit(false)))?
            .collect()
            .await?;

        Ok(Some(ctx.ctx().read_batches(completed)?))
    }

    pub(crate) async fn get_current_positions(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        let journeys = self.journeys(ctx)?;

        // Calculate current position based on current_leg_index and current_leg_progress
        let current_leg_idx_expr = cast(col("current_leg_index") + lit(1_u64), DataType::Int64);
        let current_leg = array_element(col("journey_legs"), current_leg_idx_expr.clone());

        let current_dest_x = current_leg.clone().field("destination").field("x");
        let current_dest_y = current_leg.clone().field("destination").field("y");

        // Get previous position
        let prev_leg_idx_expr = cast(col("current_leg_index"), DataType::Int64);
        let prev_leg = array_element(col("journey_legs"), prev_leg_idx_expr.clone());
        let prev_dest_x = prev_leg.clone().field("destination").field("x");
        let prev_dest_y = prev_leg.clone().field("destination").field("y");

        let start_x = datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
            None,
            vec![(
                Box::new(col("current_leg_index").eq(lit(0_u64))),
                Box::new(col("origin").field("x")),
            )],
            Some(Box::new(prev_dest_x)),
        ));

        let start_y = datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
            None,
            vec![(
                Box::new(col("current_leg_index").eq(lit(0_u64))),
                Box::new(col("origin").field("y")),
            )],
            Some(Box::new(prev_dest_y)),
        ));

        let current_pos_x =
            start_x.clone() + (current_dest_x - start_x.clone()) * col("current_leg_progress");
        let current_pos_y =
            start_y.clone() + (current_dest_y - start_y.clone()) * col("current_leg_progress");

        // Create position struct
        Ok(journeys.select(vec![
            col("person_id"),
            datafusion::functions::core::expr_fn::named_struct(vec![
                lit("x"),
                current_pos_x,
                lit("y"),
                current_pos_y,
            ])
            .alias("position"),
        ])?)
    }
}

#[cfg(test)]
mod population_tests {
    use rstest::*;

    use super::*;
    use crate::{SimulationRunner, test_utils::runner};

    #[rstest]
    #[tokio::test]
    async fn test_start_journeys(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let runner = runner.await?;
        let mut handler = runner.population().clone();

        let person_id = PersonId::new();
        let start_position = Point::new(-0.1553777, 51.5453468);
        let journey: Journey = vec![
            (Point::new(-0.1556396, 51.5455222), 100_usize),
            (Point::new(-0.1556897, 51.5455559), 200_usize),
        ]
        .into_iter()
        .collect();

        handler
            .start_journeys(runner.ctx(), vec![(person_id, start_position, journey)])
            .await?;

        let stats = handler.get_stats(runner.ctx()).await?;
        assert_eq!(stats.active_journeys, 1);
        assert_eq!(stats.completed_journeys, 0);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_empty_journeys(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let runner = runner.await?;
        let mut handler = runner.population().clone();

        // Process without any journeys
        for _ in 0..5 {
            handler.advance_journeys(runner.ctx()).await?;
        }

        let stats = handler.get_stats(runner.ctx()).await?;
        assert_eq!(stats.active_journeys, 0);
        assert_eq!(stats.completed_journeys, 0);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_journey_advancement(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let runner = runner.await?;
        let mut handler = runner.population().clone();

        let person_id = PersonId::new();
        let start_position = Point::new(-0.1553777, 51.5453468);

        // Create a short journey with small distances
        let journey: Journey = vec![
            (Point::new(-0.1556396, 51.5455222), 10_usize),
            (Point::new(-0.1556897, 51.5455559), 10_usize),
        ]
        .into_iter()
        .collect();

        handler
            .start_journeys(runner.ctx(), vec![(person_id, start_position, journey)])
            .await?;

        let stats_before = handler.get_stats(runner.ctx()).await?;
        assert_eq!(stats_before.active_journeys, 1);

        // Advance the journey
        handler.advance_journeys(runner.ctx()).await?;

        let stats_after = handler.get_stats(runner.ctx()).await?;
        // Journey should still be active (not completed yet)
        assert_eq!(stats_after.active_journeys, 1);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_journey_completion(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let mut runner = runner.await?;
        let mut handler = runner.population().clone();

        let person_id = PersonId::new();
        let start_position = Point::new(-0.1553777, 51.5453468);

        // Create a very short journey
        let journey: Journey = vec![(Point::new(-0.1553778, 51.5453469), 1_usize)]
            .into_iter()
            .collect();

        handler
            .start_journeys(runner.ctx(), vec![(person_id, start_position, journey)])
            .await?;

        // Advance many times to ensure completion
        for _ in 0..100 {
            handler.advance_journeys(runner.ctx()).await?;
            runner.advance_time();
        }

        let stats = handler.get_stats(runner.ctx()).await?;

        // Should have completed the journey
        assert!(
            stats.completed_journeys > 0,
            "Expected journey to be completed"
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_current_positions(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let runner = runner.await?;
        let mut handler = runner.population().clone();

        let person_id = PersonId::new();
        let start_position = Point::new(0.0, 0.0);
        let journey: Journey = vec![(Point::new(1.0, 1.0), 100_usize)]
            .into_iter()
            .collect();

        handler
            .start_journeys(runner.ctx(), vec![(person_id, start_position, journey)])
            .await?;

        let positions = handler.get_current_positions(runner.ctx()).await?;
        let collected = positions.collect().await?;

        assert_eq!(collected.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_completed_journeys(#[future] runner: Result<SimulationRunner>) -> Result<()> {
        let mut runner = runner.await?;
        let mut handler = runner.population().clone();

        let person_id = PersonId::new();
        let start_position = Point::new(-0.1553777, 51.5453468);
        let journey: Journey = vec![(Point::new(-0.1553778, 51.5453469), 1_usize)]
            .into_iter()
            .collect();

        handler
            .start_journeys(runner.ctx(), vec![(person_id, start_position, journey)])
            .await?;

        // Advance until completion
        for _ in 0..100 {
            handler.advance_journeys(runner.ctx()).await?;
            runner.advance_time();
        }

        // Get completed journeys
        let completed = handler.get_completed_journeys(runner.ctx()).await?;
        assert!(completed.is_some(), "Should have completed journeys");

        // Verify they're removed from active
        let stats = handler.get_stats(runner.ctx()).await?;
        assert_eq!(stats.active_journeys, 0);

        Ok(())
    }
}

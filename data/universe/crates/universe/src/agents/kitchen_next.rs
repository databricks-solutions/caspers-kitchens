use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, AsArray, DictionaryArray, FixedSizeBinaryBuilder, Int64Builder,
    LargeListBuilder, RecordBatch, StringBuilder, StringViewBuilder, StructBuilder, UInt64Builder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{Int16Type, Int64Type, UInt64Type};
use arrow::util::pretty::print_batches;
use arrow_schema::extension::Uuid;
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion::common::JoinType;
use datafusion::functions::core::expr_ext::FieldAccessor as _;
use datafusion::functions_aggregate::expr_fn::{array_agg, bool_and, count, first_value, max, min};
use datafusion::prelude::{
    DataFrame, Expr, SessionContext, array_element, array_has, array_length, array_max, case, cast,
    coalesce, col, concat, lit, make_array, named_struct, power, random, round,
};
use datafusion::scalar::ScalarValue;
use futures::StreamExt as _;
use itertools::Itertools as _;
use tracing::{Level, instrument};
use uuid::{ContextV7, Timestamp};

use super::OrderLine;
use crate::error::Result;
use crate::functions::{h3_longlatash3, uuidv7};
use crate::models::{KitchenStation, Station};
use crate::state::{OrderLineStatus, State};
use crate::{Brand, Error, EventPayload, EventsHelper, ObjectLabel, OrderStatus, parse_json};
use crate::{SimulationContext, idents::*};

#[cfg(test)]
#[path = "kitchen_tests.rs"]
pub(crate) mod tests;

static INSTRUCTIONS_FIELD: LazyLock<DataType> = LazyLock::new(|| {
    DataType::LargeList(
        Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("required_station", DataType::Utf8View, false),
                    Field::new("expected_duration", DataType::Int64, false),
                ]
                .into(),
            ),
            true,
        )
        .into(),
    )
});

static ORDER_STATE: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("person_id", DataType::FixedSizeBinary(16), false),
        Field::new("order_id", DataType::FixedSizeBinary(16), true),
        Field::new("site_id", DataType::FixedSizeBinary(16), true),
        Field::new(
            "submitted_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
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
        Field::new("status", DataType::Utf8, false),
    ]))
});

static ORDER_LINE_STATE: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("order_id", DataType::FixedSizeBinary(16), false),
        Field::new("order_line_id", DataType::FixedSizeBinary(16), false),
        Field::new("menu_item_id", DataType::FixedSizeBinary(16), false),
        Field::new("kitchen_id", DataType::FixedSizeBinary(16), true),
        Field::new(
            "submitted_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("current_step", DataType::UInt64, false),
        Field::new("total_steps", DataType::UInt64, true), // nullable because array_length returns nullable
        Field::new("assigned_to", DataType::FixedSizeBinary(16), true),
        Field::new(
            "step_completion_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("is_complete", DataType::Boolean, false),
    ]))
});

static MENU_ITEM_PROPERTIES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        // Field::new("name", DataType::Utf8View, false),
        // Field::new("description", DataType::Utf8View, false),
        Field::new("price", DataType::Float64, false),
        Field::new(
            "instructions",
            DataType::LargeList(
                Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            // Field::new("step", DataType::Utf8View, false),
                            // Field::new("description", DataType::Utf8View, false),
                            Field::new("required_station", DataType::Utf8View, false),
                            Field::new("expected_duration", DataType::Int64, false),
                        ]
                        .into(),
                    ),
                    false,
                )
                .into(),
            ),
            false,
        ),
    ]))
});

static STATION_PROPERTIES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        // Field::new("name", DataType::Utf8View, false),
        Field::new("station_type", DataType::Utf8View, false),
    ]))
});

static SITE_PROPERTIES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("name", DataType::Utf8View, false),
        Field::new("longitude", DataType::Float64, false),
        Field::new("latitude", DataType::Float64, false),
    ]))
});

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KitchenStats {
    pub queued: usize,
    pub in_progress: usize,
    pub completed: usize,
    pub idle_stations: usize,
    pub total_stations: usize,
}

impl std::ops::Add for KitchenStats {
    type Output = KitchenStats;

    fn add(self, other: KitchenStats) -> KitchenStats {
        KitchenStats {
            queued: self.queued + other.queued,
            in_progress: self.in_progress + other.in_progress,
            completed: self.completed + other.completed,
            idle_stations: self.idle_stations + other.idle_stations,
            total_stations: self.total_stations + other.total_stations,
        }
    }
}

/// Handle kitchen operations across all sites
///
/// This struct is responsible for managing kitchen operations across all sites.
/// It provides methods for processing order lines, managing kitchen resources,
/// and tracking statistics.
#[derive(Clone)]
pub(crate) struct KitchenHandler {
    pub(crate) sites: Vec<RecordBatch>,
    pub(crate) kitchens: Vec<RecordBatch>,
    pub(crate) stations: Vec<RecordBatch>,
    /// metadata required to process order lines
    pub(crate) menu_items: Vec<RecordBatch>,

    /// orders currently tracked across sites
    pub(crate) orders: Vec<RecordBatch>,
    /// individual order lines processed by kitchens
    pub(crate) order_lines: Vec<RecordBatch>,
}

impl KitchenHandler {
    pub(crate) async fn try_new(ctx: &SimulationContext) -> Result<Self> {
        let objects = ctx
            .snapshots()
            .objects()
            .await?
            .filter(col("label").in_list(
                vec![
                    lit(ObjectLabel::Site.as_ref()),
                    lit(ObjectLabel::Kitchen.as_ref()),
                    lit(ObjectLabel::Station.as_ref()),
                    lit(ObjectLabel::Brand.as_ref()),
                    lit(ObjectLabel::MenuItem.as_ref()),
                ],
                false,
            ))?
            .cache()
            .await?;
        let sites = extract_sites(objects.clone()).await?;
        let (brand_ids, menu_items) = extract_menu_items(objects.clone()).await?;
        let (stations, kitchens) = extract_kitchen_station(ctx, objects, brand_ids).await?;
        Ok(KitchenHandler {
            sites,
            kitchens,
            stations,
            menu_items,
            // TODO: load this from the snapshot
            orders: vec![RecordBatch::new_empty(ORDER_STATE.clone())],
            order_lines: vec![RecordBatch::new_empty(ORDER_LINE_STATE.clone())],
        })
    }

    /// Caspers Ghost Kitchen sites
    ///
    /// The site data has the following schema:
    ///
    /// ```ignore
    /// {
    ///   site_id: bytes
    ///   name: string
    ///   longitude: float64
    ///   latitude: float64
    /// }
    /// ```
    ///
    /// ### Arguments
    ///
    /// - `ctx`: The simulation context.
    ///
    /// ### Returns
    ///
    /// A `Result` containing a [`DataFrame`] with the site data.
    pub(crate) fn sites(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.sites.iter().cloned())?)
    }

    /// Kitchens installed across sites.
    ///
    /// The kitchen data has the following schema:
    ///
    /// ```ignore
    /// {
    ///   site_id: bytes
    ///   kitchen_id: bytes
    ///   accepted_brands: [bytes]
    /// }
    /// ```
    ///
    /// ### Arguments
    ///
    /// - `ctx`: The simulation context.
    ///
    /// ### Returns
    ///
    /// A `Result` containing a [`DataFrame`] with the kitchen data.
    pub(crate) fn kitchens(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.kitchens.iter().cloned())?)
    }

    /// Stations installed in kitchens across sites
    ///
    /// The station data has the following schema:
    ///
    /// ```ignore
    /// {
    ///   site_id: bytes
    ///   kitchen_id: bytes
    ///   station_id: bytes
    ///   station_type: string
    /// }
    /// ```
    ///
    /// ### Arguments
    ///
    /// - `ctx`: The simulation context.
    ///
    /// ### Returns
    ///
    /// A `Result` containing a [`DataFrame`] with the station data.
    pub(crate) fn stations(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.stations.iter().cloned())?)
    }

    /// Menu items available for ordering.
    ///
    /// The menu items data has the following schema:
    ///
    /// ```ignore
    /// {
    ///   brand_id: bytes
    ///   menu_item_id: bytes
    ///   item_price: float64
    ///   instructions: [
    ///     {
    ///       required_station: string
    ///       expected_duration: int32
    ///     }
    ///   ]
    /// }
    /// ```
    pub(crate) fn menu_items(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.menu_items.iter().cloned())?)
    }

    pub(crate) fn orders(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.orders.iter().cloned())?)
    }

    pub(crate) fn order_lines(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        Ok(ctx.ctx().read_batches(self.order_lines.iter().cloned())?)
    }

    pub(crate) async fn step(
        &mut self,
        ctx: &SimulationContext,
        incoming_orders: Option<DataFrame>,
    ) -> Result<DataFrame> {
        let mut events = EventsHelper::empty(ctx)?;

        if let Some(orders) = incoming_orders {
            events = events.union(self.prepare_order_lines(ctx, orders).await?)?;
        }
        events = events.union(self.process_order_lines(ctx).await?)?;

        events = events.union(self.update_order_status(ctx).await?)?;

        Ok(events)
    }

    /// Get orders ready for pickup by couriers
    ///
    /// ### Arguments
    ///
    /// * `ctx` - The simulation context.
    ///
    /// ### Returns
    ///
    /// A [`DataFrame`] containing delivery information with the following schema.
    ///
    /// ```ignore
    /// {
    ///   person_id: bytes
    ///   site_id: bytes
    ///   order_id: bytes
    ///   start_position: {
    ///     x: float64
    ///     y: float64
    ///   }
    ///   destiniation: {
    ///     x: float64
    ///     y: float64
    ///   }
    /// }
    /// ```
    pub(crate) fn ready_orders(&self, ctx: &SimulationContext) -> Result<Option<DataFrame>> {
        if self.orders.is_empty() || self.orders.iter().map(|b| b.num_rows()).sum::<usize>() < 1 {
            return Ok(None);
        };
        Ok(Some(
            self.orders(ctx)?
                .filter(col("status").eq(lit(OrderStatus::Ready.as_ref())))?
                .join_on(
                    self.sites(ctx)?.select([
                        col("site_id").alias("site_id2"),
                        named_struct(vec![lit("x"), col("longitude"), lit("y"), col("latitude")])
                            .alias("origin"),
                    ])?,
                    JoinType::Left,
                    [col("site_id").eq(col("site_id2"))],
                )?
                .select([
                    col("person_id"),
                    col("site_id"),
                    col("order_id"),
                    col("submitted_at"),
                    col("origin"),
                    col("destination"),
                ])?,
        ))
    }

    /// Set the status for the given orders.
    ///
    /// This function updates the status of the given orders to the specified status.
    ///
    /// ### Arguments
    ///
    /// * `ctx` - The simulation context.
    /// * `order_ids` - The IDs of the orders to update.
    /// * `status` - The new status for the orders.
    ///
    /// ### Returns
    ///
    /// A `Result` indicating success or failure.
    pub(crate) async fn set_order_status(
        &mut self,
        ctx: &SimulationContext,
        order_ids: Vec<Expr>,
        status: OrderStatus,
    ) -> Result<()> {
        self.orders = self
            .orders(ctx)?
            .select(vec![
                col("person_id"),
                col("order_id"),
                col("site_id"),
                col("submitted_at"),
                col("destination"),
                case(col("order_id").in_list(order_ids.clone(), false))
                    .when(lit(true), lit(status.as_ref()))
                    .otherwise(col("status"))?,
            ])?
            .collect()
            .await?;
        Ok(())
    }

    /// Prepare new orders and order lines for processing.
    ///
    /// This processes the raw orders from the population and crates individual
    /// order lines based on the menu items selected in the order. Each order
    /// is assigned to a site based on the delivery destination of the order.
    ///
    /// Each generated order line contains the recipe/processing information
    /// required to drive the order line through the kitchen.
    ///
    /// ## Arguments
    ///
    /// * `ctx`: The simulation context.
    /// * `orders`: The new orders to prepare.
    ///
    /// ## Returns
    ///
    /// A DataFrame containing events raised during order preparation.
    /// The raised events may contain:
    ///
    /// * [OrderCreated](crate::SimulationEvent::OrderCreated)
    async fn prepare_order_lines(
        &mut self,
        ctx: &SimulationContext,
        orders: DataFrame,
    ) -> Result<DataFrame> {
        let resolution = lit(5_i8);

        // assign order to sites based on the H3 index of the order destination
        let orders = orders
            .select([
                col("person_id"),
                uuidv7().call(vec![col("submitted_at")]).alias("order_id"),
                col("submitted_at"),
                col("destination"),
                col("items"),
                h3_longlatash3()
                    .call(vec![
                        col("destination").field("x"),
                        col("destination").field("y"),
                        resolution.clone(),
                    ])
                    .alias("destination_cell"),
            ])?
            .join_on(
                self.sites(ctx)?.select([
                    col("site_id"),
                    h3_longlatash3()
                        .call(vec![col("longitude"), col("latitude"), resolution.clone()])
                        .alias("site_cell"),
                ])?,
                JoinType::Left,
                [col("destination_cell").eq(col("site_cell"))],
            )?
            .select([
                col("site_id"),
                col("person_id"),
                col("order_id"),
                col("submitted_at"),
                col("destination"),
                col("items"),
            ])?
            // NOTE: we need to materialize here, to have
            // consistent order ids after cloning the frame. also perormance.
            .cache()
            .await?;

        let orders_count = orders.clone().count().await?;
        if orders_count == 0 {
            return EventsHelper::empty(ctx);
        }

        // flatten orders into lines
        let lines = unnest_orders(ctx, orders.clone(), *ctx.current_time()).await?;
        let lines_count = lines.clone().count().await?;
        if lines_count == 0 {
            return EventsHelper::empty(ctx);
        }

        let menu_items = self.menu_items(ctx)?.select([
            col("menu_item_id").alias("menu_item_id2"),
            col("brand_id"),
            col("instructions"),
        ])?;

        let lines = lines
            // append site ids and order submission timestamp to allow
            // routing order lines to kitchens at the correct site and time.
            .join_on(
                orders.clone().select([
                    col("site_id"),
                    col("order_id").alias("order_id2"),
                    col("submitted_at"),
                ])?,
                JoinType::Left,
                [col("order_id").eq(col("order_id2"))],
            )?
            .drop_columns(&["order_id2"])?
            // append brand Id and instructions. We need the brand ID to limit suitable kitchens
            // and the instructions to track progress when processing the line items.
            .join_on(
                menu_items,
                JoinType::Left,
                [col("menu_item_id").eq(col("menu_item_id2"))],
            )?
            .drop_columns(&["menu_item_id2"])?;

        let assigned_lines = self.assign_order_lines_to_kitchens(ctx, lines).await?;

        self.order_lines = self
            .order_lines(ctx)?
            .union(assigned_lines)?
            .collect()
            .await?;

        let new_order_events = EventsHelper::order_created(orders.clone())?;

        self.orders = orders
            .select([
                col("person_id"),
                col("order_id"),
                col("site_id"),
                col("submitted_at"),
                col("destination"),
                lit(OrderStatus::Submitted.as_ref()).alias("status"),
            ])?
            .union(self.orders(ctx)?)?
            .collect()
            .await?;

        Ok(new_order_events)
    }

    /// Process order lines.
    ///
    /// This method processes order lines by advancing steps and marking completed ones.
    ///
    /// ## Arguments
    ///
    /// * `ctx`: The simulation context.
    ///
    /// ## Returns
    ///
    /// A `Result` containing the processed order lines. The data frame contains events
    /// raised during the processing of order lines. This includes the following:
    ///
    /// - processing step finished [OrderLineStepFinished](crate::SimulationEvent::OrderLineStepFinished)
    /// - order line status updated [OrderLineUpdated](crate::SimulationEvent::OrderLineUpdated)
    async fn process_order_lines(&mut self, ctx: &SimulationContext) -> Result<DataFrame> {
        let mut events = EventsHelper::empty(ctx)?;

        // Early return if no order lines to process
        if self.order_lines.is_empty() || self.order_lines.iter().all(|b| b.num_rows() == 0) {
            return Ok(events);
        }

        events = events.union(self.prepare_steps(ctx).await?)?;

        let current_time = ctx.current_time_expr();

        // Step 1: Advance steps for completed work
        // For order lines where step_completion_time has passed, increment current_step
        let lines_to_advance = self
            .order_lines(ctx)?
            .filter(
                col("is_complete")
                    .is_false()
                    .and(col("step_completion_time").is_not_null())
                    .and(col("step_completion_time").lt_eq(current_time.clone())),
            )?
            .select([
                col("order_line_id").alias("order_line_id_adv"),
                (col("current_step") + lit(1_u64)).alias("next_step"),
                col("total_steps").alias("total_steps_adv"),
            ])?
            .cache()
            .await?;

        let step_finished_events = lines_to_advance
            .clone()
            .join_on(
                self.order_lines(ctx)?,
                JoinType::Left,
                [col("order_line_id_adv").eq(col("order_line_id"))],
            )?
            .filter(
                col("current_step")
                    .gt(lit(1_i32))
                    .and(col("next_step").lt_eq(col("total_steps"))),
            )?
            .select([
                col("step_completion_time").alias("timestamp"),
                col("order_line_id"),
                col("assigned_to").alias("station_id"),
                col("current_step").alias("step_index"),
            ])?;
        events = events.union(EventsHelper::step_finished(step_finished_events)?)?;

        let order_lines_ready_events = lines_to_advance
            .clone()
            .join_on(
                self.order_lines(ctx)?,
                JoinType::Left,
                [col("order_line_id_adv").eq(col("order_line_id"))],
            )?
            .filter(col("next_step").gt(col("total_steps")))?
            .select([
                col("step_completion_time").alias("timestamp"),
                col("order_line_id"),
                col("kitchen_id"),
            ])?;
        events = events.union(EventsHelper::order_line_ready(order_lines_ready_events)?)?;

        // Update order lines: advance steps and mark completed ones
        let advanced_order_lines = self
            .order_lines(ctx)?
            .join_on(
                lines_to_advance,
                JoinType::Left,
                [col("order_line_id").eq(col("order_line_id_adv"))],
            )?
            .select([
                col("order_id"),
                col("order_line_id"),
                col("menu_item_id"),
                col("kitchen_id"),
                col("submitted_at"),
                coalesce(vec![col("next_step"), col("current_step")]).alias("current_step"),
                col("total_steps"),
                // Clear assigned_to and step_completion_time for lines that need to advance
                Expr::Case(datafusion::logical_expr::Case::new(
                    None,
                    vec![(
                        Box::new(col("next_step").is_not_null()),
                        Box::new(lit(ScalarValue::FixedSizeBinary(16, None))),
                    )],
                    Some(Box::new(col("assigned_to"))),
                ))
                .alias("assigned_to"),
                Expr::Case(datafusion::logical_expr::Case::new(
                    None,
                    vec![(
                        Box::new(col("next_step").is_not_null()),
                        Box::new(lit(ScalarValue::TimestampMillisecond(
                            None,
                            Some("UTC".into()),
                        ))),
                    )],
                    Some(Box::new(col("step_completion_time"))),
                ))
                .alias("step_completion_time"),
            ])?
            .select(vec![
                col("order_id"),
                col("order_line_id"),
                col("menu_item_id"),
                col("kitchen_id"),
                col("submitted_at"),
                col("current_step"),
                col("total_steps"),
                col("assigned_to"),
                col("step_completion_time"),
                col("current_step")
                    .gt(col("total_steps"))
                    .alias("is_complete"),
            ])?;

        self.order_lines = advanced_order_lines.collect().await?;

        Ok(events)
    }

    async fn update_order_status(&mut self, ctx: &SimulationContext) -> Result<DataFrame> {
        let completed_orders = self
            .order_lines(ctx)?
            .join_on(
                self.orders(ctx)?
                    .filter(col("status").eq(lit(OrderStatus::Submitted.as_ref())))?
                    .select([col("order_id").alias("order_id_aux"), col("site_id")])?,
                JoinType::Inner,
                [col("order_id").eq(col("order_id_aux"))],
            )?
            .aggregate(
                vec![col("site_id"), col("order_id").alias("order_id_updated")],
                vec![bool_and(col("is_complete")).alias("all_complete")],
            )?
            .filter(col("all_complete").eq(lit(true)))?
            .cache()
            .await?;

        // update the order status to completed
        let updated_orders = self
            .orders(ctx)?
            .join_on(
                completed_orders
                    .clone()
                    .select_columns(&["order_id_updated", "all_complete"])?,
                JoinType::Left,
                [col("order_id").eq(col("order_id_updated"))],
            )?
            .select([
                col("person_id"),
                col("order_id"),
                col("site_id"),
                col("submitted_at"),
                col("destination"),
                case(col("all_complete"))
                    .when(lit(true), lit(OrderStatus::Ready.as_ref()))
                    .otherwise(col("status"))?
                    .alias("status"),
            ])?;

        self.orders = updated_orders.collect().await?;

        let completed_orders = completed_orders.select([
            col("site_id"),
            col("order_id_updated").alias("order_id"),
            ctx.current_time_expr().alias("timestamp"),
        ])?;

        let order_ready_events = EventsHelper::order_ready(completed_orders)?;

        Ok(order_ready_events)
    }

    /// Assign order lines to kitchens.
    ///
    /// ## Arguments
    ///
    /// * `ctx`: The simulation context.
    /// * `order_lines`: The new order lines to assign.
    ///
    /// ## Returns
    ///
    /// A DataFrame containing the assigned order lines.
    async fn assign_order_lines_to_kitchens(
        &self,
        ctx: &SimulationContext,
        order_lines: DataFrame,
    ) -> Result<DataFrame> {
        let matched_kitchens = order_lines
            .clone()
            .join_on(
                self.kitchens(ctx)?.select([
                    col("site_id").alias("site_id2"),
                    col("kitchen_id"),
                    col("accepted_brands"),
                ])?,
                JoinType::Inner,
                [
                    col("site_id").eq(col("site_id2")),
                    array_has(col("accepted_brands"), col("brand_id")),
                ],
            )?
            .select([col("order_line_id"), col("kitchen_id")])?;

        let kitchen_assignments = matched_kitchens.aggregate(
            vec![col("order_line_id")],
            vec![array_agg(col("kitchen_id")).alias("kitchen_ids")],
        )?;

        // TODO: improve routing data generation and drop this.
        // https://github.com/chefcaspers/management/issues/33
        let assigned_count = kitchen_assignments.clone().count().await?;
        if assigned_count == 0 {
            tracing::error!("dropping some orders it seems");
            return Ok(ctx
                .ctx()
                .read_batch(RecordBatch::new_empty(ORDER_LINE_STATE.clone()))?);
        }

        let kitchen_line_counts = self
            .order_lines(ctx)?
            // .filter(col("kitchen_id").is_not_null())?
            .aggregate(
                vec![col("kitchen_id")],
                vec![count(col("order_line_id")).alias("line_count")],
            )?;

        let mut kitchen_stats = HashMap::new();
        let mut count_stream = kitchen_line_counts.execute_stream().await?;
        // initialize kitchen stats with the number of active lines in each kitchen
        while let Some(Ok(batch)) = count_stream.next().await {
            let kitchen_id = batch.column(0).as_fixed_size_binary().iter().flatten();
            let line_count = batch.column(1).as_primitive::<Int64Type>().iter().flatten();
            for (kitchen_id, line_count) in kitchen_id.zip(line_count) {
                kitchen_stats.insert(kitchen_id.to_vec(), line_count as usize);
            }
        }
        // add any kitchens that are not in the line count stream
        for batch in &self.kitchens {
            for kitchen_id in batch.column(1).as_fixed_size_binary().iter().flatten() {
                if !kitchen_stats.contains_key(kitchen_id) {
                    kitchen_stats.insert(kitchen_id.to_vec(), 0);
                }
            }
        }

        let mut assigned_batches = Vec::new();
        let mut order_stream = kitchen_assignments.execute_stream().await?;
        while let Some(Ok(mut batch)) = order_stream.next().await {
            let candidates = batch.remove_column(1);
            let assigned = do_assign(&candidates, &mut kitchen_stats)?;
            assigned_batches.push(RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("order_line_id2", DataType::FixedSizeBinary(16), false),
                    Field::new("kitchen_id", assigned.data_type().clone(), true),
                ])),
                vec![batch.remove_column(0), assigned],
            )?);
        }

        Ok(order_lines
            .join_on(
                ctx.ctx().read_batches(assigned_batches)?,
                JoinType::Left,
                [col("order_line_id").eq(col("order_line_id2"))],
            )?
            .drop_columns(&["order_line_id2"])?
            .select([
                col("order_id"),
                col("order_line_id"),
                col("menu_item_id"),
                col("kitchen_id"),
                col("submitted_at"),
                lit(ScalarValue::UInt64(Some(1))).alias("current_step"),
                array_length(col("instructions")).alias("total_steps"),
                lit(ScalarValue::FixedSizeBinary(16, None)).alias("assigned_to"),
                lit(ScalarValue::TimestampMillisecond(None, Some("UTC".into())))
                    .alias("step_completion_time"),
                lit(ScalarValue::Boolean(Some(false))).alias("is_complete"),
            ])?)
    }

    /// Assign steps to stations.
    ///
    /// This method assigns individual steps to stations within a kitchen
    /// (if available) for processing.
    ///
    /// ## Arguments
    ///
    /// * `ctx`: The simulation context.
    async fn assign_steps_to_stations(&mut self, ctx: &SimulationContext) -> Result<DataFrame> {
        // Get lines that need assignment (no completion time or not complete)
        let current_time = ctx.current_time_expr();
        let to_update = self
            .order_lines(ctx)?
            .filter(
                col("is_complete").is_false().and(
                    col("step_completion_time")
                        .is_null()
                        .or(col("step_completion_time").lt_eq(current_time.clone())),
                ),
            )?
            .join_on(
                self.menu_items(ctx)?.select([
                    col("menu_item_id").alias("menu_item_id2"),
                    col("instructions"),
                ])?,
                JoinType::Left,
                [col("menu_item_id").eq(col("menu_item_id2"))],
            )?
            .drop_columns(&["menu_item_id2"])?;

        // Compute completion time for current step
        let scale_factor_fn = power(
            lit(ScalarValue::Float64(Some(1_f64))) + random()
                - lit(ScalarValue::Float64(Some(0.3_f64))),
            lit(ScalarValue::Int32(Some(2_i32))),
        );

        // Convert expected duration (in seconds) to milliseconds
        let duration_in_seconds = round(vec![
            array_element(
                col("instructions"),
                cast(col("current_step"), DataType::Int64),
            )
            .field("expected_duration")
                * scale_factor_fn,
        ]);

        // Add duration to timestamp
        let base_timestamp = array_max(make_array(vec![current_time.clone(), col("submitted_at")]));

        // Cast to int64 millis, add duration in millis, then cast back to timestamp
        let completion_ts_expression = cast(
            cast(base_timestamp, DataType::Int64) + (duration_in_seconds * lit(1000_i64)),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        );

        let to_update = to_update
            .select([
                col("order_line_id"),
                col("kitchen_id"),
                col("submitted_at"),
                array_element(
                    col("instructions"),
                    cast(col("current_step"), DataType::Int64),
                )
                .field("required_station")
                .alias("step_station"),
                completion_ts_expression.alias("next_completion_time"),
            ])?
            .cache()
            .await?;

        let available_stations = self.available_stations(ctx).await?;
        let station_assignments = to_update
            .clone()
            .join_on(
                available_stations,
                JoinType::Left,
                [col("kitchen_id")
                    .eq(col("kitchen_id2"))
                    .and(col("station_type").eq(col("step_station")))],
            )?
            // first group by station id. This might assign the same order to multiple stations
            .aggregate(
                vec![col("station_id")],
                vec![
                    first_value(
                        col("order_line_id"),
                        vec![col("submitted_at").sort(false, false)],
                    )
                    .alias("order_line_id"),
                ],
            )?
            // then aggregate by order line id to assign a specific station to each order line
            .aggregate(
                vec![col("order_line_id").alias("order_line_id2")],
                vec![first_value(col("station_id"), vec![]).alias("station_id")],
            )?
            .filter(col("station_id").is_not_null())?;

        // Join to_update with station assignments to get station_id for each order_line_id
        let station_updates = to_update
            .clone()
            .select([col("order_line_id"), col("next_completion_time")])?
            .join_on(
                station_assignments,
                JoinType::Inner,
                [col("order_line_id").eq(col("order_line_id2"))],
            )?
            .select([
                col("order_line_id").alias("order_line_id2"),
                col("station_id"),
                col("next_completion_time"),
            ])?
            .cache()
            .await?;

        let step_started_events = station_updates
            .clone()
            .join_on(
                self.order_lines(ctx)?,
                JoinType::Left,
                [col("order_line_id").eq(col("order_line_id2"))],
            )?
            .select([
                current_time.alias("timestamp"),
                col("order_line_id"),
                col("current_step").alias("step_index"),
                col("station_id"),
            ])?;

        let events = EventsHelper::step_started(step_started_events)?;

        let updated = self
            .order_lines(ctx)?
            .join_on(
                station_updates,
                JoinType::Left,
                [col("order_line_id").eq(col("order_line_id2"))],
            )?
            .select([
                col("order_id"),
                col("order_line_id"),
                col("menu_item_id"),
                col("kitchen_id"),
                col("submitted_at"),
                col("current_step"),
                col("total_steps"),
                coalesce(vec![col("station_id"), col("assigned_to")]).alias("assigned_to"),
                coalesce(vec![
                    col("next_completion_time"),
                    col("step_completion_time"),
                ])
                .alias("step_completion_time"),
                col("is_complete"),
            ])?;

        self.order_lines = updated.collect().await?;

        Ok(events)
    }

    /// Get kitchen stations available for processing orders
    async fn available_stations(&self, ctx: &SimulationContext) -> Result<DataFrame> {
        // Select available stations for processing orders
        let in_use_stations = self
            .order_lines(ctx)?
            .filter(col("assigned_to").is_not_null())?
            .aggregate(
                vec![col("assigned_to")],
                vec![count(col("order_line_id")).alias("order_line_count")],
            )?
            .collect()
            .await?;
        let n_stations = in_use_stations.iter().map(|b| b.num_rows()).sum::<usize>();
        if n_stations == 0 {
            return Ok(self.stations(ctx)?.select([
                col("kitchen_id").alias("kitchen_id2"),
                col("station_id"),
                col("station_type"),
            ])?);
        }
        Ok(self
            .stations(ctx)?
            .join_on(
                ctx.ctx().read_batches(in_use_stations)?,
                JoinType::Left,
                [col("station_id").eq(col("assigned_to"))],
            )?
            .filter(
                col("order_line_count")
                    .lt(lit(1_i32))
                    .or(col("order_line_count").is_null()),
            )?
            .select([
                col("kitchen_id").alias("kitchen_id2"),
                col("station_id"),
                col("station_type"),
            ])?)
    }

    /// Prepare steps for processing.
    ///
    /// This method prepares steps for processing by assigning them to stations.
    /// Line items assigned to a suitable kitchen station for processing.
    ///
    /// ## Arguments
    ///
    /// * `ctx`: The simulation context.
    async fn prepare_steps(&mut self, ctx: &SimulationContext) -> Result<DataFrame> {
        let mut events = EventsHelper::empty(ctx)?;

        let mut curr_stats = self.get_stats(ctx).await?;
        let mut steps = 0;
        // TODO: see if we can make the assignments in a single pass.
        loop {
            let started_events = self.assign_steps_to_stations(ctx).await?;
            let new_stats = self.get_stats(ctx).await?;
            if new_stats == curr_stats {
                break;
            }
            events = started_events.union(events)?;
            curr_stats = new_stats;
            steps += 1;
            if steps > 10 {
                break;
            }
        }

        Ok(events)
    }

    /// Get statistics about the kitchen state
    pub(crate) async fn get_stats(&self, ctx: &SimulationContext) -> Result<KitchenStats> {
        let total_stations = self.stations.iter().map(|b| b.num_rows()).sum();

        // Handle empty order lines
        if self.order_lines.is_empty() || self.order_lines.iter().all(|b| b.num_rows() == 0) {
            return Ok(KitchenStats {
                queued: 0,
                in_progress: 0,
                completed: 0,
                idle_stations: total_stations,
                total_stations,
            });
        }

        let order_lines = self.order_lines(ctx)?;

        // Count queued (not assigned), in progress (assigned), and completed items
        let stats_df = order_lines
            .aggregate(
                vec![],
                vec![
                    count(Expr::Case(datafusion::logical_expr::Case::new(
                        None,
                        vec![(
                            Box::new(
                                col("assigned_to")
                                    .is_null()
                                    .and(col("is_complete").eq(lit(false))),
                            ),
                            Box::new(lit(1_i64)),
                        )],
                        None,
                    )))
                    .alias("queued"),
                    count(Expr::Case(datafusion::logical_expr::Case::new(
                        None,
                        vec![(
                            Box::new(
                                col("assigned_to")
                                    .is_not_null()
                                    .and(col("is_complete").eq(lit(false))),
                            ),
                            Box::new(lit(1_i64)),
                        )],
                        None,
                    )))
                    .alias("in_progress"),
                    count(Expr::Case(datafusion::logical_expr::Case::new(
                        None,
                        vec![(
                            Box::new(col("is_complete").eq(lit(true))),
                            Box::new(lit(1_i64)),
                        )],
                        None,
                    )))
                    .alias("completed"),
                ],
            )?
            .collect()
            .await?;

        let idle_stations = total_stations; // TODO: compute actual idle stations

        let batch = &stats_df[0];
        let queued = batch.column(0).as_primitive::<Int64Type>().value(0) as usize;
        let in_progress = batch.column(1).as_primitive::<Int64Type>().value(0) as usize;
        let completed = batch.column(2).as_primitive::<Int64Type>().value(0) as usize;

        Ok(KitchenStats {
            queued,
            in_progress,
            completed,
            idle_stations,
            total_stations,
        })
    }
}

async fn extract_kitchen_station(
    ctx: &SimulationContext,
    objects: DataFrame,
    brand_ids: Vec<Expr>,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    let kitchens = objects
        .clone()
        .filter(col("label").eq(lit(ObjectLabel::Kitchen.as_ref())))?
        .select([
            col("parent_id").alias("site_id"),
            col("id").alias("kitchen_id"),
            make_array(brand_ids).alias("accepted_brands"),
        ])?
        .cache()
        .await?;
    let stations: Vec<_> = objects
        .filter(col("label").eq(lit(ObjectLabel::Station.as_ref())))?
        .select([
            col("parent_id").alias("kitchen_id2"),
            col("id").alias("station_id"),
            col("properties"),
        ])?
        .collect()
        .await?
        .into_iter()
        .map(|batch| parse_properties(batch, STATION_PROPERTIES_SCHEMA.clone()))
        .try_collect()?;
    let stations = kitchens
        .clone()
        .select_columns(&["site_id", "kitchen_id"])?
        .join_on(
            ctx.ctx().read_batches(stations)?,
            JoinType::Left,
            [col("kitchen_id").eq(col("kitchen_id2"))],
        )?
        .drop_columns(&["kitchen_id2"])?
        .sort_by(vec![col("site_id"), col("kitchen_id")])?
        .collect()
        .await?;
    let kitchens = kitchens.collect().await?;
    Ok((stations, kitchens))
}

async fn extract_sites(objects: DataFrame) -> Result<Vec<RecordBatch>> {
    let sites = objects
        .filter(col("label").eq(lit(ObjectLabel::Site.as_ref())))?
        .select([col("id").alias("site_id"), col("properties")])?;
    let sites = sites
        .select([col("site_id"), col("properties")])?
        .collect()
        .await?
        .into_iter()
        .map(|batch| parse_properties(batch, SITE_PROPERTIES_SCHEMA.clone()))
        .try_collect()?;
    Ok(sites)
}

async fn extract_menu_items(objects: DataFrame) -> Result<(Vec<Expr>, Vec<RecordBatch>)> {
    let menu_items = objects
        .filter(col("label").eq(lit(ObjectLabel::MenuItem.as_ref())))?
        .select([
            col("parent_id").alias("brand_id"),
            col("id").alias("menu_item_id"),
            col("properties"),
        ])?;
    let brand_ids = menu_items
        .clone()
        .select_columns(&["brand_id"])?
        .collect()
        .await?
        .into_iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_fixed_size_binary()
                .iter()
                .flat_map(|it| it.map(|i| i.to_vec()))
                .collect_vec()
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|id| lit(ScalarValue::FixedSizeBinary(16, Some(id))))
        .collect();
    let menu_items = menu_items
        .select([col("brand_id"), col("menu_item_id"), col("properties")])?
        .collect()
        .await?
        .into_iter()
        .map(|batch| parse_properties(batch, MENU_ITEM_PROPERTIES_SCHEMA.clone()))
        .try_collect()?;
    Ok((brand_ids, menu_items))
}

/// Parse the 'properties' field of a record batch into the passed schema
///
/// All parsed fields will be added as top level fields to the record batch.
/// The existing properties will be removed from the record batch.
fn parse_properties(mut batch: RecordBatch, properties_schema: SchemaRef) -> Result<RecordBatch> {
    let properties_idx = batch.schema().index_of("properties")?;
    let properties_col = batch.remove_column(properties_idx);
    let properties = parse_json(&properties_col, properties_schema)?;
    let fields: Fields = batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .chain(properties.schema().fields().iter().cloned())
        .collect();
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .cloned()
        .chain(properties.columns().iter().cloned())
        .collect();
    Ok::<_, Error>(RecordBatch::try_new(Schema::new(fields).into(), columns)?)
}

fn do_assign(candidates: &dyn Array, stats: &mut HashMap<Vec<u8>, usize>) -> Result<ArrayRef> {
    let candidates = candidates.as_list::<i32>();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(candidates.len(), 16);

    for c in candidates.iter() {
        if let Some(candidate) = c {
            if let Some(kitchen_id) = candidate
                .as_fixed_size_binary()
                .iter()
                .flatten()
                .flat_map(|id| Some((*stats.get(id)?, id.to_vec())))
                .min_by_key(|x| x.0)
            {
                builder.append_value(&kitchen_id.1)?;
                if let Some(curr) = stats.get_mut(&kitchen_id.1) {
                    *curr += 1;
                } else {
                    stats.insert(kitchen_id.1.to_vec(), 1);
                }
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

//TODO: we should really be using the unnests operator for this.
// but there are some not-impl errors when building the logical plan.
// So for now we do the unnesting manually.
async fn unnest_orders(
    ctx: &SimulationContext,
    orders: DataFrame,
    current_time: DateTime<Utc>,
) -> Result<DataFrame> {
    let orders = orders
        .select_columns(&["order_id", "items"])?
        .collect()
        .await?;

    let context = ContextV7::new();
    let ts = Timestamp::from_unix(
        &context,
        current_time.timestamp() as u64,
        current_time.timestamp_subsec_nanos(),
    );

    Ok(ctx.ctx().read_batch(unnest_orders_inner(orders, ts)?)?)
}

pub(crate) fn unnest_orders_inner(orders: Vec<RecordBatch>, ts: Timestamp) -> Result<RecordBatch> {
    let mut builder = OrderLineBuilder::new();
    for ord in orders.into_iter() {
        let order_ids = ord.column(0).as_fixed_size_binary().iter();
        let menu_items = ord.column(1).as_list::<i32>().iter();
        for (order_id, menu_items) in order_ids.zip(menu_items) {
            if let (Some(order_id), Some(menu_items)) = (order_id, menu_items) {
                for ids in menu_items.as_fixed_size_list().iter().flatten() {
                    let ids = ids.as_fixed_size_binary();
                    let order_line_id = uuid::Uuid::new_v7(ts);
                    builder.add_value(order_id, order_line_id, ids.value(1))?;
                }
            }
        }
    }
    builder.finish()
}

struct OrderLineBuilder {
    order_id: FixedSizeBinaryBuilder,
    order_line_id: FixedSizeBinaryBuilder,
    menu_item_id: FixedSizeBinaryBuilder,
}

static ORDER_LINE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("order_id", DataType::FixedSizeBinary(16), false),
        Field::new("order_line_id", DataType::FixedSizeBinary(16), false),
        Field::new("menu_item_id", DataType::FixedSizeBinary(16), false),
    ]))
});

impl OrderLineBuilder {
    fn new() -> Self {
        Self {
            order_id: FixedSizeBinaryBuilder::new(16),
            order_line_id: FixedSizeBinaryBuilder::new(16),
            menu_item_id: FixedSizeBinaryBuilder::new(16),
        }
    }

    fn add_value(
        &mut self,
        order_id: impl AsRef<[u8]>,
        order_line_id: impl AsRef<[u8]>,
        menu_item_id: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.order_id.append_value(order_id)?;
        self.order_line_id.append_value(order_line_id)?;
        self.menu_item_id.append_value(menu_item_id)?;
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            ORDER_LINE_SCHEMA.clone(),
            vec![
                Arc::new(self.order_id.finish()),
                Arc::new(self.order_line_id.finish()),
                Arc::new(self.menu_item_id.finish()),
            ],
        )?)
    }
}

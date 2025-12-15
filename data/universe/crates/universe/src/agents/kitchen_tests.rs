use arrow::array::{
    BooleanBuilder, FixedSizeListBuilder, ListBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::TimestampMillisecondType;
use datafusion::prelude::named_struct;
use geo::Point;
use geoarrow::array::PointBuilder;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::{Dimension, PointType};
use h3o::LatLng;
use rand::Rng as _;
use rand::rngs::ThreadRng;
use rstest::*;
use uuid::{ContextV7, Timestamp};

use crate::agents::functions::{OrderSpec, create_order_fixed};
use crate::test_utils::{builder, print_frame, runner_fixed, setup_test_simulation};
use crate::{RecordBatch, Result, SimulationRunner, SimulationRunnerBuilder};

use super::*;

struct OrderBuilder {
    menu_items: RecordBatch,
    rng: ThreadRng,

    order_id: FixedSizeBinaryBuilder,
    person_id: FixedSizeBinaryBuilder,
    submitted_at: TimestampMillisecondBuilder,
    destination: PointBuilder,
    items: ListBuilder<FixedSizeListBuilder<FixedSizeBinaryBuilder>>,
}

impl OrderBuilder {
    fn new(menu_items: RecordBatch) -> Self {
        Self {
            menu_items,
            rng: rand::rng(),
            order_id: FixedSizeBinaryBuilder::new(16),
            person_id: FixedSizeBinaryBuilder::new(16),
            submitted_at: TimestampMillisecondBuilder::new(),
            destination: PointBuilder::new(PointType::new(Dimension::XY, Default::default())),
            items: ListBuilder::new(FixedSizeListBuilder::new(
                FixedSizeBinaryBuilder::new(16),
                2,
            )),
        }
    }

    fn add_order(
        &mut self,
        person_id: &PersonId,
        submitted_at: DateTime<Utc>,
        destination: &Point,
        n_items: usize,
    ) -> Result<()> {
        let order_id = OrderId::new();

        self.order_id.append_value(order_id)?;
        self.person_id.append_value(person_id)?;

        self.submitted_at
            .append_value(submitted_at.timestamp_millis());
        self.destination.push_point(Some(destination));

        let random_vec: Vec<usize> = (0..n_items)
            .map(|_| self.rng.random_range(0..self.menu_items.num_rows()))
            .collect();

        let brand_ids = self.menu_items.column(0).as_fixed_size_binary();
        let item_ids = self.menu_items.column(1).as_fixed_size_binary();

        for idx in random_vec {
            self.items
                .values()
                .values()
                .append_value(brand_ids.value(idx))?;
            self.items
                .values()
                .values()
                .append_value(item_ids.value(idx))?;
            self.items.values().append(true);
        }
        self.items.append(true);

        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.order_id.finish()),
            Arc::new(self.person_id.finish()),
            Arc::new(self.submitted_at.finish()),
            self.destination.finish().into_array_ref(),
            Arc::new(self.items.finish()),
        ];
        let names = &[
            "order_id",
            "person_id",
            "submitted_at",
            "destination",
            "items",
        ];
        let fields: Fields = columns
            .iter()
            .zip(names.iter())
            .map(|(col, name)| Field::new(*name, col.data_type().clone(), col.is_nullable()))
            .collect();
        let orders = RecordBatch::try_new(Schema::new(fields).into(), columns)?;

        let context = ContextV7::new();
        let current_time = Utc::now();
        let ts = Timestamp::from_unix(
            &context,
            current_time.timestamp() as u64,
            current_time.timestamp_subsec_nanos(),
        );

        Ok(orders)
    }
}

struct OrderLinesView<'a> {
    order_lines: &'a RecordBatch,
}

impl<'a> OrderLinesView<'a> {
    fn new(order_lines: &'a RecordBatch) -> Self {
        Self { order_lines }
    }

    pub fn assigned_to(&self, idx: usize) -> Option<&[u8]> {
        let arr = self
            .order_lines
            .column_by_name("assigned_to")
            .expect("assigned_to column not defined")
            .as_fixed_size_binary();
        arr.is_valid(idx).then_some(arr.value(idx))
    }

    pub fn step_completion_time(&self, idx: usize) -> Option<i64> {
        let arr = self
            .order_lines
            .column_by_name("step_completion_time")
            .expect("step_completion_time column not defined")
            .as_primitive::<TimestampMillisecondType>();
        arr.is_valid(idx).then_some(arr.value(idx))
    }

    pub fn current_step(&self, idx: usize) -> Option<u64> {
        let arr = self
            .order_lines
            .column_by_name("current_step")
            .expect("current_step column not defined")
            .as_primitive::<UInt64Type>();
        arr.is_valid(idx).then_some(arr.value(idx))
    }
}

#[fixture]
async fn prepare_simulation(
    #[default(OrderSpec::Once(vec![1]))] spec: OrderSpec,
    #[future] builder: Result<SimulationRunnerBuilder>,
) -> Result<(SimulationRunner, DataFrame)> {
    let create_order = Box::new(move |menu_items| create_order_fixed(menu_items, spec.clone()));
    let mut builder = builder.await?.with_create_orders(create_order);

    let mut simulation = builder.build().await?;

    let orders = simulation
        .population()
        .create_orders(simulation.ctx())
        .await?
        .cache()
        .await?;

    Ok((simulation, orders))
}

#[rstest]
#[tokio::test]
async fn test_step_assignment(
    #[future] prepare_simulation: Result<(SimulationRunner, DataFrame)>,
) -> Result<()> {
    let (simulation, orders) = prepare_simulation.await?;
    let mut handler = simulation.kitchens.clone();

    handler
        .prepare_order_lines(simulation.ctx(), orders)
        .await?;

    let stats = handler.get_stats(simulation.ctx()).await?;
    assert_eq!(stats.queued, 1);
    assert_eq!(stats.in_progress, 0);
    assert_eq!(stats.completed, 0);

    handler.prepare_steps(simulation.ctx()).await?;

    let stats = handler.get_stats(simulation.ctx()).await?;
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.in_progress, 1);
    assert_eq!(stats.completed, 0);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_order_progress(
    #[future] prepare_simulation: Result<(SimulationRunner, DataFrame)>,
) -> Result<()> {
    let (mut simulation, orders) = prepare_simulation.await?;
    let mut handler = simulation.kitchens.clone();

    // prepare lines for processing and assign lines to kitchen
    handler
        .prepare_order_lines(simulation.ctx(), orders)
        .await?;
    // prepare steps for processing
    handler.prepare_steps(simulation.ctx()).await?;

    // get the completion time for the order line
    let view = OrderLinesView::new(&handler.order_lines[0]);
    let completion_time = view.step_completion_time(0).unwrap();
    let completion_time = DateTime::from_timestamp_millis(completion_time).unwrap();

    // advance time until the completion time is reached
    while simulation.ctx().current_time() < &completion_time {
        simulation.advance_time();
    }

    handler.process_order_lines(simulation.ctx()).await?;

    let view = OrderLinesView::new(&handler.order_lines[0]);
    assert_eq!(view.current_step(0).unwrap(), 2);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_empty_orders(
    #[future]
    #[with(OrderSpec::Never)]
    prepare_simulation: Result<(SimulationRunner, DataFrame)>,
) -> Result<()> {
    let (mut simulation, orders) = prepare_simulation.await?;
    let mut handler = simulation.kitchens.clone();

    // Process multiple steps without generating orders
    for _ in 0..5 {
        handler.process_order_lines(simulation.ctx()).await?;
        simulation.advance_time();
    }

    // Verify no errors occur and state remains valid
    let stats = handler.get_stats(simulation.ctx()).await?;
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.in_progress, 0);
    assert_eq!(stats.completed, 0);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_order_completion(
    #[future]
    #[with(OrderSpec::Once(vec![10]))]
    prepare_simulation: Result<(SimulationRunner, DataFrame)>,
) -> Result<()> {
    let (mut simulation, orders) = prepare_simulation.await?;
    let mut handler = simulation.kitchens.clone();

    handler
        .prepare_order_lines(simulation.ctx(), orders)
        .await?;

    // Generate and process orders through step_next (which includes processing)
    // Do this several times to allow completion
    for i in 0..50 {
        // Just process existing orders
        handler.process_order_lines(simulation.ctx()).await?;
        simulation.advance_time();
    }

    // Check for completed orders
    let stats = handler.get_stats(simulation.ctx()).await?;

    // Should have some items in progress or completed
    assert!(
        stats.in_progress + stats.completed > 0,
        "Expected some order lines in progress or completed"
    );

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_step_progression(
    #[future]
    #[with(OrderSpec::Once(vec![10]))]
    runner_fixed: Result<SimulationRunner>,
) -> Result<()> {
    let mut simulation = runner_fixed.await?;

    // Generate orders
    simulation.step().await?;

    let mut handler = simulation.kitchens.clone();

    // Get initial order line data
    let initial = handler
        .order_lines(simulation.ctx())?
        .select([
            col("order_line_id"),
            col("current_step"),
            col("total_steps"),
        ])?
        .collect()
        .await?;

    let initial_count: usize = initial.iter().map(|b| b.num_rows()).sum();
    assert!(initial_count > 0, "Should have generated some orders");

    // All should start at step 1
    for batch in &initial {
        let steps = batch.column(1).as_primitive::<UInt64Type>();
        for step in steps.iter().flatten() {
            assert_eq!(step, 1, "All orders should start at step 1");
        }
    }

    // Advance time significantly to trigger step completion
    for _ in 0..100 {
        handler.process_order_lines(simulation.ctx()).await?;
        simulation.advance_time();
    }

    // Check stats - should still have the orders we created
    let stats = handler.get_stats(simulation.ctx()).await?;
    println!("Step progression stats: {:?}", stats);

    let total_lines = stats.queued + stats.in_progress + stats.completed;
    assert_eq!(
        total_lines, initial_count,
        "Should have same number of order lines, not lose them during processing"
    );

    Ok(())
}

#[tokio::test]
async fn test_prepare_empty_orders() -> Result<(), Box<dyn std::error::Error>> {
    let mut simulation = setup_test_simulation(None).await?;
    let mut handler = KitchenHandler::try_new(simulation.ctx()).await?;

    // Create empty dataframes
    let empty_orders = simulation
        .ctx()
        .ctx()
        .read_batches(vec![RecordBatch::new_empty(Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::FixedSizeBinary(16), false),
            Field::new("site_id", DataType::FixedSizeBinary(16), false),
            Field::new(
                "submitted_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
        ])))])?;

    // Should not error on empty batches
    handler
        .prepare_order_lines(simulation.ctx(), empty_orders)
        .await?;

    let stats = handler.get_stats(simulation.ctx()).await?;
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.in_progress, 0);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_unreachable_orders(
    #[future] prepare_simulation: Result<(SimulationRunner, DataFrame)>,
) -> Result<()> {
    let (mut simulation, orders) = prepare_simulation.await?;
    let mut handler = simulation.kitchens.clone();

    let unreachable_order = orders.select([
        col("person_id"),
        col("order_id"),
        col("submitted_at"),
        named_struct(vec![lit("x"), lit(0.0_f64), lit("y"), lit(0.0_f64)]).alias("destination"),
        col("items"),
    ])?;

    print_frame(&unreachable_order).await?;

    let result = handler
        .prepare_order_lines(simulation.ctx(), unreachable_order)
        .await;

    assert!(result.is_ok());

    Ok(())
}

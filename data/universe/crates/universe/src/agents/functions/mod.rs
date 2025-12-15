use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::logical_expr::ScalarUDF;

pub use self::create_order::fixed::OrderSpec;

mod create_order;

pub fn create_order(choices: RecordBatch) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(create_order::CreateOrder::new(
        choices,
    )))
}

pub fn create_order_fixed(choices: RecordBatch, spec: OrderSpec) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(
        create_order::fixed::CreateOrderFixed::new(choices, spec),
    ))
}

use std::hash::Hasher;
use std::sync::Arc;
use std::{any::Any, sync::LazyLock};

use arrow::array::{
    AsArray, FixedSizeBinaryBuilder, FixedSizeListBuilder, ListBuilder, RecordBatch,
};
use arrow::datatypes::DataType;
use arrow_schema::{Field, TimeUnit};
use chrono::{DateTime, Timelike, Utc};
use datafusion::common::{Result, exec_err, plan_datafusion_err};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    scalar_doc_sections::DOC_SECTION_STRUCT,
};
use datafusion::scalar::ScalarValue;
use rand::Rng as _;

pub(super) mod fixed;

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_STRUCT,
        "Randomly generate order by people.",
        "create_order(timestamp_expr)",
    )
    .with_argument(
        "timestamp_expr",
        "Datetime expression corresponiding to time of day when decision is made.",
    )
    .build()
});

#[derive(Debug, PartialEq)]
pub struct CreateOrder {
    signature: Signature,
    menu_items: RecordBatch,
}

impl std::hash::Hash for CreateOrder {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl std::cmp::Eq for CreateOrder {}

impl CreateOrder {
    pub fn new(menu_items: RecordBatch) -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    DataType::Utf8View,
                ],
                Volatility::Volatile,
            ),
            menu_items,
        }
    }
}

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

impl ScalarUDFImpl for CreateOrder {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "create_order"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_fixed_size_list(
            "item",
            Field::new("item", DataType::FixedSizeBinary(16), true),
            2,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            mut args,
            number_rows,
            ..
        } = args;
        let mut rng = rand::rng();

        let sigma_sq = 0.4_f64;

        let state = args
            .pop()
            .ok_or_else(|| plan_datafusion_err!("create_order expects 2 arguments"))?;
        let datetime = args
            .pop()
            .ok_or_else(|| plan_datafusion_err!("create_order expects 2 arguments"))?;

        let brand_ids = self.menu_items.column(0).as_fixed_size_binary();
        let item_ids = self.menu_items.column(1).as_fixed_size_binary();

        let order_builder = FixedSizeListBuilder::new(FixedSizeBinaryBuilder::new(16), 2);
        let mut lb = ListBuilder::new(order_builder);

        match (datetime, state) {
            (ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(Some(time), _)), _) => {
                let Some(date_time) = DateTime::<Utc>::from_timestamp_millis(time) else {
                    return exec_err!("Invalid timestamp (create_orders)");
                };
                let current_minutes = (date_time.hour() * 60 + date_time.minute()) as f64 / 60.0;
                let prob = 0.01
                    * (bell(current_minutes, 12.0, sigma_sq)
                        + bell(current_minutes, 18.0, sigma_sq));

                for _ in 0..number_rows {
                    if rng.random_bool(prob) {
                        let count: usize = rng.random_range(1..6);
                        let random_vec: Vec<usize> = (0..count)
                            .map(|_| rng.random_range(0..self.menu_items.num_rows()))
                            .collect();
                        for idx in random_vec {
                            lb.values().values().append_value(brand_ids.value(idx))?;
                            lb.values().values().append_value(item_ids.value(idx))?;
                            lb.values().append(true);
                        }
                        lb.append(true);
                    } else {
                        lb.append_null();
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(lb.finish())))
            }
            _ => exec_err!("Only scalar timestamps are currently supported (create_orders)"),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_doc())
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // The function preserves the order of its argument.
        Ok(input[0].sort_properties)
    }
}

fn bell(x: f64, mu: f64, sigma_sq: f64) -> f64 {
    use std::f64::consts::{E, PI};

    let exponent = -(x - mu).powi(2) / (2.0 * sigma_sq);
    1.0 / (2.0 * PI * sigma_sq).powf(2.0) * E.powf(exponent)
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;
    use datafusion::{
        logical_expr::ScalarUDF,
        prelude::{SessionContext, col, lit},
    };

    use super::*;

    #[tokio::test]
    async fn test_create_order() -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = crate::PopulationData::builder();
        builder.add_site(10, 51.518898098201326, -0.13381370382489707)?;
        let population_data = builder.finish()?;

        let mut brand_ids = FixedSizeBinaryBuilder::new(16);
        brand_ids.append_value(uuid::Uuid::new_v4().as_bytes())?;
        brand_ids.append_value(uuid::Uuid::new_v4().as_bytes())?;
        let brand_ids = Arc::new(brand_ids.finish());

        let mut item_ids = FixedSizeBinaryBuilder::new(16);
        item_ids.append_value(uuid::Uuid::new_v4().as_bytes())?;
        item_ids.append_value(uuid::Uuid::new_v4().as_bytes())?;
        let item_ids = Arc::new(item_ids.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("brand_id", DataType::FixedSizeBinary(16), false),
            Field::new("item_id", DataType::FixedSizeBinary(16), false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![brand_ids, item_ids])?;
        let func = ScalarUDF::new_from_impl(CreateOrder::new(batch));

        let ctx = SessionContext::new();

        let df = ctx.read_batch(population_data)?;
        let df = df.select(vec![
            col("id"),
            func.call(vec![
                lit(ScalarValue::TimestampMillisecond(
                    Some(1761675872000),
                    Some("UTC".into()),
                )),
                col("state"),
            ])
            .alias("order"),
        ])?;

        let batches = df.collect().await?;

        assert!(!batches.is_empty());

        Ok(())
    }
}

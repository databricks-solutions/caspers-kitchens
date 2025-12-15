use std::hash::Hasher;
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicI64};
use std::{any::Any, sync::LazyLock};

use arrow::array::{
    AsArray, FixedSizeBinaryBuilder, FixedSizeListBuilder, ListBuilder, RecordBatch,
};
use arrow::datatypes::DataType;
use arrow_schema::{Field, TimeUnit};
use datafusion::common::Result;
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    scalar_doc_sections::DOC_SECTION_STRUCT,
};
use rand::Rng as _;

#[derive(Debug, PartialEq, Clone)]
pub enum OrderSpec {
    Never,
    Once(Vec<usize>),
}

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_STRUCT,
        "Generate orders according to fixed specification.",
        "create_order(timestamp_expr)",
    )
    .with_argument(
        "timestamp_expr",
        "Datetime expression corresponiding to time of day when decision is made.",
    )
    .build()
});

#[derive(Debug)]
pub struct CreateOrderFixed {
    signature: Signature,
    menu_items: RecordBatch,
    spec: OrderSpec,
    call_count: AtomicI64,
}

impl std::hash::Hash for CreateOrderFixed {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl PartialEq for CreateOrderFixed {
    fn eq(&self, other: &Self) -> bool {
        self.menu_items.eq(&other.menu_items) && self.signature.eq(&other.signature)
    }
}

impl std::cmp::Eq for CreateOrderFixed {}

impl CreateOrderFixed {
    pub fn new(menu_items: RecordBatch, spec: OrderSpec) -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    DataType::Utf8View,
                ],
                Volatility::Volatile,
            ),
            menu_items,
            spec,
            call_count: AtomicI64::new(0),
        }
    }
}

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

impl ScalarUDFImpl for CreateOrderFixed {
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
        let ScalarFunctionArgs { number_rows, .. } = args;
        let mut rng = rand::rng();

        let brand_ids = self.menu_items.column(0).as_fixed_size_binary();
        let item_ids = self.menu_items.column(1).as_fixed_size_binary();

        let order_builder = FixedSizeListBuilder::new(FixedSizeBinaryBuilder::new(16), 2);
        let mut lb = ListBuilder::new(order_builder);

        match &self.spec {
            OrderSpec::Never => {
                for _ in 0..number_rows {
                    lb.append_null();
                }
            }
            OrderSpec::Once(orders) => {
                if self.call_count.fetch_add(1, Ordering::Relaxed) == 0 {
                    for idx in 0..number_rows {
                        if idx < orders.len() {
                            let random_lines: Vec<usize> = (0..orders[idx])
                                .map(|_| rng.random_range(0..self.menu_items.num_rows()))
                                .collect();
                            for line_idx in random_lines {
                                lb.values()
                                    .values()
                                    .append_value(brand_ids.value(line_idx))?;
                                lb.values()
                                    .values()
                                    .append_value(item_ids.value(line_idx))?;
                                lb.values().append(true);
                            }
                            lb.append(true);
                        } else {
                            lb.append_null();
                        }
                    }
                } else {
                    for _ in 0..number_rows {
                        lb.append_null();
                    }
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(lb.finish())))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_doc())
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // The function preserves the order of its argument.
        Ok(input[0].sort_properties)
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;
    use datafusion::{
        logical_expr::ScalarUDF,
        prelude::{SessionContext, col, lit},
        scalar::ScalarValue,
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
        let func =
            ScalarUDF::new_from_impl(CreateOrderFixed::new(batch, OrderSpec::Once(vec![1_usize])));

        let ctx = SessionContext::new();

        let df = ctx.read_batch(population_data.clone())?;
        let df = df.select(vec![
            col("id"),
            func.call(vec![
                lit(ScalarValue::TimestampMillisecond(None, Some("UTC".into()))),
                col("state"),
            ])
            .alias("order"),
        ])?;
        let order_count = df.filter(col("order").is_not_null())?.count().await?;
        assert_eq!(order_count, 1);

        let df = ctx.read_batch(population_data)?;
        let df = df.select(vec![
            col("id"),
            func.call(vec![
                lit(ScalarValue::TimestampMillisecond(None, Some("UTC".into()))),
                col("state"),
            ])
            .alias("order"),
        ])?;
        let order_count = df.filter(col("order").is_not_null())?.count().await?;
        assert_eq!(order_count, 0);

        Ok(())
    }
}

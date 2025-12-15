use std::sync::Arc;
use std::{any::Any, sync::LazyLock};

use arrow::array::AsArray;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::datatypes::{DataType, TimestampMillisecondType};
use arrow_schema::TimeUnit;
use chrono::{DateTime, Utc};
use datafusion::common::Result;
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    scalar_doc_sections::DOC_SECTION_STRUCT,
};
use datafusion::scalar::ScalarValue;
use uuid::{ContextV7, Timestamp};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UuidV7 {
    signature: Signature,
}

impl UuidV7 {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some("UTC".into()),
                )],
                Volatility::Volatile,
            ),
        }
    }
}

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_STRUCT,
        "Generate a version 7 (time-ordered) UUID",
        "uuidv7(<timestamp>)",
    )
    .with_argument("timestamp", "Optional; The timestamp to use for the UUID")
    .build()
});

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

/// Implement the ScalarUDFImpl trait for AddOne
impl ScalarUDFImpl for UuidV7 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uuidv7"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::FixedSizeBinary(16))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let context = ContextV7::new();

        match args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(Some(ts), _))) => {
                let timestamp = DateTime::<Utc>::from_timestamp_millis(*ts).unwrap();
                let ts = Timestamp::from_unix(
                    &context,
                    timestamp.timestamp() as u64,
                    timestamp.timestamp_subsec_nanos(),
                );
                let mut builder = FixedSizeBinaryBuilder::with_capacity(number_rows, 16);
                for _ in 0..number_rows {
                    builder.append_value(uuid::Uuid::new_v7(ts))?;
                }
                let uuids = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(uuids)))
            }
            Some(ColumnarValue::Array(lon)) => {
                let times = lon.as_primitive::<TimestampMillisecondType>();
                let mut builder = FixedSizeBinaryBuilder::with_capacity(number_rows, 16);
                for maybe_ts in times.iter() {
                    if let Some(timestamp) =
                        maybe_ts.and_then(DateTime::<Utc>::from_timestamp_millis)
                    {
                        let ts = Timestamp::from_unix(
                            &context,
                            timestamp.timestamp() as u64,
                            timestamp.timestamp_subsec_nanos(),
                        );
                        builder.append_value(uuid::Uuid::new_v7(ts))?;
                    } else {
                        builder.append_value(uuid::Uuid::now_v7())?;
                    }
                }
                let uuids = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(uuids)))
            }
            _ => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(number_rows, 16);
                for _ in 0..number_rows {
                    builder.append_value(uuid::Uuid::now_v7())?;
                }
                let uuids = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(uuids)))
            }
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

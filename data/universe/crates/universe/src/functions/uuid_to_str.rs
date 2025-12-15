use std::sync::Arc;
use std::{any::Any, sync::LazyLock};

use arrow::array::{AsArray, LargeStringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{Result, exec_datafusion_err};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    scalar_doc_sections::DOC_SECTION_STRUCT,
};
use datafusion::scalar::ScalarValue;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UuidToString {
    signature: Signature,
}

impl UuidToString {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::FixedSizeBinary(16)], Volatility::Stable),
        }
    }
}

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_STRUCT,
        "Convert a binary UUID to its string (hyphenated) representation",
        "uuid_to_str(<uuid>)",
    )
    .with_argument("uuid", "UUID in FixedSizeBinary(16)")
    .build()
});

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

/// Implement the ScalarUDFImpl trait for AddOne
impl ScalarUDFImpl for UuidToString {
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
        Ok(DataType::LargeUtf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        // NOTE: a string UUID with hyphens requires 36 bytes
        let mut builder = LargeStringBuilder::with_capacity(number_rows, number_rows * 36);

        match args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(16, maybe_uuid))) => {
                let uuid = maybe_uuid
                    .as_ref()
                    .and_then(|uuid| uuid::Uuid::from_slice(uuid).ok())
                    .map(|uuid| uuid.hyphenated().to_string());
                for _ in 0..number_rows {
                    builder.append_option(uuid.as_ref());
                }
                let uuids = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(uuids)))
            }
            Some(ColumnarValue::Array(uuids))
                if uuids.data_type() == &DataType::FixedSizeBinary(16) =>
            {
                let uuids = uuids.as_fixed_size_binary();
                for maybe_uuid in uuids.iter() {
                    let uuid = maybe_uuid
                        .as_ref()
                        .and_then(|uuid| uuid::Uuid::from_slice(uuid).ok())
                        .map(|uuid| uuid.hyphenated().to_string());
                    builder.append_option(uuid.as_ref());
                }
                let uuids = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(uuids)))
            }
            _ => Err(exec_datafusion_err!(
                "uuid_to_str only supports FixedSizeBinary(16) data."
            )),
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

use std::sync::Arc;
use std::{any::Any, sync::LazyLock};

use arrow::array::{AsArray, Int64Array};
use arrow::datatypes::{DataType, Float64Type, Int8Type};
use datafusion::common::{Result, plan_datafusion_err};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    scalar_doc_sections::DOC_SECTION_STRUCT,
};
use datafusion::scalar::ScalarValue;
use h3o::{LatLng, Resolution};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LongLatAsH3 {
    signature: Signature,
}

impl LongLatAsH3 {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64, DataType::Int8],
                Volatility::Immutable,
            ),
        }
    }
}

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_STRUCT,
        "Returns the H3 cell ID (as a BIGINT) corresponding to the provided longitude and latitude at the specified resolution.",
        "h3_longlatash3(longitude_expr, latitude_expr, resolution_expr)",
    )
    .with_argument("arg1", "The struct column to convert to json")
    .build()
});

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

/// Implement the ScalarUDFImpl trait for AddOne
impl ScalarUDFImpl for LongLatAsH3 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "h3_longlatash3"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;

        let resolution = args
            .pop()
            .ok_or_else(|| plan_datafusion_err!("h3_longlatash3 expects 3 arguments"))?;
        let latitude = args
            .pop()
            .ok_or_else(|| plan_datafusion_err!("h3_longlatash3 expects 3 arguments"))?;
        let longitude = args
            .pop()
            .ok_or_else(|| plan_datafusion_err!("h3_longlatash3 expects 3 arguments"))?;

        match (longitude, latitude, resolution) {
            (
                ColumnarValue::Scalar(ScalarValue::Float64(Some(lon))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(lat))),
                ColumnarValue::Scalar(ScalarValue::Int8(Some(res))),
            ) => {
                let Ok(lat_lng) = LatLng::new(lat, lon) else {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)));
                };
                let resolution = int_to_res(res)?;
                let cell = lat_lng.to_cell(resolution);
                let cell_id = u64::from(cell) as i64;
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(cell_id))))
            }
            (ColumnarValue::Array(lon), ColumnarValue::Array(lat), ColumnarValue::Array(res)) => {
                let longitudes = lon.as_primitive::<Float64Type>();
                let latitudes = lat.as_primitive::<Float64Type>();
                let resolutions = res.as_primitive::<Int8Type>();
                let results: Int64Array = longitudes
                    .into_iter()
                    .zip(latitudes)
                    .zip(resolutions)
                    .map(|((lon, lat), res)| {
                        let lat_lng = LatLng::new(lat?, lon?).ok()?;
                        let resolution = int_to_res(res?).ok()?;
                        let cell = lat_lng.to_cell(resolution);
                        Some(u64::from(cell) as i64)
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (
                ColumnarValue::Array(lon),
                ColumnarValue::Array(lat),
                ColumnarValue::Scalar(ScalarValue::Int8(Some(res))),
            ) => {
                let longitudes = lon.as_primitive::<Float64Type>();
                let latitudes = lat.as_primitive::<Float64Type>();
                let resolution = int_to_res(res)?;
                let results: Int64Array = longitudes
                    .into_iter()
                    .zip(latitudes)
                    .map(|(lon, lat)| {
                        let lat_lng = LatLng::new(lat?, lon?).ok()?;
                        let cell = lat_lng.to_cell(resolution);
                        Some(u64::from(cell) as i64)
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            _ => Err(plan_datafusion_err!("h3_longlatash3 expects 3 arguments")),
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

fn int_to_res(res: i8) -> Result<Resolution> {
    match res {
        0 => Ok(Resolution::Zero),
        1 => Ok(Resolution::One),
        2 => Ok(Resolution::Two),
        3 => Ok(Resolution::Three),
        4 => Ok(Resolution::Four),
        5 => Ok(Resolution::Five),
        6 => Ok(Resolution::Six),
        7 => Ok(Resolution::Seven),
        8 => Ok(Resolution::Eight),
        9 => Ok(Resolution::Nine),
        10 => Ok(Resolution::Ten),
        11 => Ok(Resolution::Eleven),
        12 => Ok(Resolution::Twelve),
        13 => Ok(Resolution::Thirteen),
        14 => Ok(Resolution::Fourteen),
        15 => Ok(Resolution::Fifteen),
        _ => Err(plan_datafusion_err!("Invalid resolution")),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Float64Array, Int8Array, RecordBatch};
    use datafusion::{
        assert_batches_eq,
        logical_expr::ScalarUDF,
        prelude::{SessionContext, col, lit},
    };

    use super::*;

    // https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/h3_longlatash3#examples
    #[tokio::test]
    async fn test_int_to_res() -> Result<(), Box<dyn std::error::Error>> {
        let long: ArrayRef = Arc::new(Float64Array::from(vec![100.0, -122.4783, -122.4783]));
        let lat: ArrayRef = Arc::new(Float64Array::from(vec![45.0, 37.8199, 37.8199]));
        let res: ArrayRef = Arc::new(Int8Array::from(vec![6, 13, 16]));
        let batch = RecordBatch::try_from_iter(vec![("long", long), ("lat", lat), ("res", res)])?;

        let long_lat_as_h3 = ScalarUDF::from(LongLatAsH3::new());

        let ctx = SessionContext::new();
        ctx.register_udf(long_lat_as_h3.clone());
        ctx.register_batch("t", batch)?;

        let df = ctx.table("t").await?;
        let df = df.select(vec![
            long_lat_as_h3
                .call(vec![lit(100.0_f64), lit(45.0_f64), lit(6_i8)])
                .alias("cell_id"),
        ])?;

        let batches = df.collect().await?;
        let expected = vec![
            "+--------------------+",
            "| cell_id            |",
            "+--------------------+",
            "| 604116085645508607 |",
            "| 604116085645508607 |",
            "| 604116085645508607 |",
            "+--------------------+",
        ];
        assert_batches_eq!(&expected, &batches);

        let df = ctx.table("t").await?;
        let df = df.select(vec![
            long_lat_as_h3
                .call(vec![lit(-122.4783_f64), lit(37.8199_f64), lit(13_i8)])
                .alias("cell_id"),
        ])?;

        let batches = df.collect().await?;
        let expected = vec![
            "+--------------------+",
            "| cell_id            |",
            "+--------------------+",
            "| 635714569676958015 |",
            "| 635714569676958015 |",
            "| 635714569676958015 |",
            "+--------------------+",
        ];
        assert_batches_eq!(&expected, &batches);

        let df = ctx.table("t").await?;
        let df = df.select(vec![
            long_lat_as_h3
                .call(vec![col("long"), col("lat"), col("res")])
                .alias("cell_id"),
        ])?;

        let batches = df.collect().await?;
        let expected = vec![
            "+--------------------+",
            "| cell_id            |",
            "+--------------------+",
            "| 604116085645508607 |",
            "| 635714569676958015 |",
            "|                    |",
            "+--------------------+",
        ];
        assert_batches_eq!(&expected, &batches);

        let df = ctx.table("t").await?;
        let df = df.select(vec![
            long_lat_as_h3
                .call(vec![col("long"), col("lat"), lit(13_i8)])
                .alias("cell_id"),
        ])?;

        let batches = df.collect().await?;
        let expected = vec![
            "+--------------------+",
            "| cell_id            |",
            "+--------------------+",
            "| 635641282996486335 |",
            "| 635714569676958015 |",
            "| 635714569676958015 |",
            "+--------------------+",
        ];
        assert_batches_eq!(&expected, &batches);

        Ok(())
    }
}

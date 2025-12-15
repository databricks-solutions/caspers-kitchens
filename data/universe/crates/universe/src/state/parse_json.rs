use arrow::{
    array::{Array, AsArray, RecordBatch},
    json::{ReaderBuilder, reader::Decoder},
};
use arrow_schema::{DataType, SchemaRef};

use crate::{Error, Result};

fn parse_one_record(decoder: &mut Decoder, bytes: &[u8]) -> Result<()> {
    let existing_len = decoder.len();
    let decoded_bytes = decoder.decode(bytes)?;
    assert_eq!(decoded_bytes, bytes.len()); // all bytes consumed
    assert_eq!(decoder.len(), existing_len + 1); // exactly one record produced
    assert!(!decoder.has_partial_record()); // the record was complete
    Ok(())
}

pub(crate) fn parse_json(json_data: &dyn Array, schema: SchemaRef) -> Result<RecordBatch> {
    if json_data.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    fn map_str(value: Option<&str>) -> Option<&[u8]> {
        value.map(|v| v.as_bytes())
    }
    let value_iter: Box<dyn Iterator<Item = Option<&[u8]>>> = match json_data.data_type() {
        DataType::Utf8 => Box::new(json_data.as_string::<i32>().iter().map(map_str)),
        DataType::LargeUtf8 => Box::new(json_data.as_string::<i64>().iter().map(map_str)),
        DataType::Utf8View => Box::new(json_data.as_string_view().iter().map(map_str)),
        DataType::Binary => Box::new(json_data.as_binary::<i32>().iter()),
        DataType::LargeBinary => Box::new(json_data.as_binary::<i64>().iter()),
        DataType::BinaryView => Box::new(json_data.as_binary_view().iter()),
        _ => {
            return Err(Error::InvalidData(format!(
                "Unsupported data type for JSON parsing: {:?}",
                json_data.data_type()
            )));
        }
    };
    let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;
    for record in value_iter {
        parse_one_record(&mut decoder, record.unwrap_or("{}".as_bytes()))?;
    }
    Ok(decoder
        .flush()?
        .unwrap_or_else(|| RecordBatch::new_empty(schema)))
}

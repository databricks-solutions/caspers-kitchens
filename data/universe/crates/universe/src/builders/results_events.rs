use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, LargeStringBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use uuid::{ContextV7, Timestamp, Uuid};

use crate::{Event, EventPayload};

static EVENT_PREFIX: &str = "io.caspers";
static DEFAULT_SOURCE: &str = "caspers/universe/default";
static DEFAULT_SPECVERSION: &str = "1.0";
static DEFAULT_CONTENT_TYPE: &str = "application/json";

pub(crate) static EVENTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false),
        Field::new("source", DataType::LargeUtf8, false),
        Field::new("specversion", DataType::LargeUtf8, false),
        Field::new("type", DataType::LargeUtf8, false),
        Field::new("datacontenttype", DataType::LargeUtf8, false),
        Field::new("time", DataType::LargeUtf8, false),
        Field::new("data", DataType::LargeUtf8, false),
    ]))
});

pub struct EventDataBuilder {
    id: FixedSizeBinaryBuilder,
    source: LargeStringBuilder,
    specversion: LargeStringBuilder,
    type_: LargeStringBuilder,
    datacontenttype: LargeStringBuilder,
    time: LargeStringBuilder,
    data: LargeStringBuilder,

    context: ContextV7,
}

impl Default for EventDataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EventDataBuilder {
    pub fn new() -> Self {
        Self {
            id: FixedSizeBinaryBuilder::new(16),
            source: LargeStringBuilder::new(),
            specversion: LargeStringBuilder::new(),
            type_: LargeStringBuilder::new(),
            datacontenttype: LargeStringBuilder::new(),
            time: LargeStringBuilder::new(),
            data: LargeStringBuilder::new(),
            context: ContextV7::new(),
        }
    }

    pub fn add_event(&mut self, event: &Event) -> Result<()> {
        let ts = Timestamp::from_unix(
            &self.context,
            event.timestamp.timestamp() as u64,
            event.timestamp.timestamp_subsec_nanos(),
        );
        let uuid = Uuid::new_v7(ts);

        self.id.append_value(uuid)?;
        self.source.append_value(DEFAULT_SOURCE);
        self.specversion.append_value(DEFAULT_SPECVERSION);
        self.type_.append_value(self.event_type(&event.payload));
        self.datacontenttype.append_value(DEFAULT_CONTENT_TYPE);
        self.time.append_value(event.timestamp.to_rfc3339());
        self.data
            .append_value(serde_json::to_string(&event.payload).unwrap());
        Ok(())
    }

    fn event_type(&self, event: &EventPayload) -> String {
        match event {
            EventPayload::OrderCreated(_) => format!("{}.orders.created", EVENT_PREFIX),
            EventPayload::OrderUpdated(_) => format!("{}.orders.updated", EVENT_PREFIX),
            EventPayload::OrderLineUpdated(_) => format!("{}.orders.line_updated", EVENT_PREFIX),
            EventPayload::PersonUpdated(_) => format!("{}.persons.updated", EVENT_PREFIX),
        }
    }

    pub fn build(mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.source.finish()),
            Arc::new(self.specversion.finish()),
            Arc::new(self.type_.finish()),
            Arc::new(self.datacontenttype.finish()),
            Arc::new(self.time.finish()),
            Arc::new(self.data.finish()),
        ];
        Ok(RecordBatch::try_new(EVENTS_SCHEMA.clone(), arrays)?)
    }
}

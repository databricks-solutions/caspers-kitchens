use std::sync::{Arc, LazyLock};

use arrow::array::{RecordBatch, StringViewBuilder, TimestampMillisecondBuilder};
use arrow_schema::extension::Json;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::TableReference;
use uuid::Uuid;

use crate::{Error, Result};

pub(in crate::context) static SYSTEM_SCHEMA_NAME: &str = "system";
pub(crate) static ROUTING_NODES_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", SYSTEM_SCHEMA_NAME, "routing_nodes"));
pub(crate) static ROUTING_EDGES_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", SYSTEM_SCHEMA_NAME, "routing_edges"));
pub(in crate::context) static SNAPSHOT_META_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", SYSTEM_SCHEMA_NAME, "snapshots"));
pub(in crate::context) static SIMULATION_META_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", SYSTEM_SCHEMA_NAME, "simulations"));

pub(crate) static SNAPSHOT_META_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8View, false),
        Field::new("simulation_id", DataType::Utf8View, false),
        Field::new(
            "simulation_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("properties", DataType::Utf8View, true).with_extension_type(Json::default()),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ]))
});

pub(crate) struct SnapshotMetaBuilder {
    id: StringViewBuilder,
    simulation_id: StringViewBuilder,
    simulation_time: TimestampMillisecondBuilder,
    properties: StringViewBuilder,
    created_at: TimestampMillisecondBuilder,
}

impl SnapshotMetaBuilder {
    pub fn new() -> Self {
        Self {
            id: StringViewBuilder::new(),
            simulation_id: StringViewBuilder::new(),
            simulation_time: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            properties: StringViewBuilder::new(),
            created_at: TimestampMillisecondBuilder::new().with_timezone("UTC"),
        }
    }

    pub fn add_snapshot(
        &mut self,
        id: &Uuid,
        simulation_id: &Uuid,
        simulation_time: DateTime<Utc>,
        properties: Option<String>,
    ) {
        self.id.append_value(id.to_string());
        self.simulation_id.append_value(simulation_id.to_string());
        self.simulation_time
            .append_value(simulation_time.timestamp_millis());
        self.properties.append_option(properties);
        self.created_at.append_value(Utc::now().timestamp_millis());
    }

    pub fn build(mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            SNAPSHOT_META_SCHEMA.clone(),
            vec![
                Arc::new(self.id.finish()),
                Arc::new(self.simulation_id.finish()),
                Arc::new(self.simulation_time.finish()),
                Arc::new(self.properties.finish()),
                Arc::new(self.created_at.finish()),
            ],
        )?)
    }
}

pub(crate) static SIMULATION_META_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8View, false),
        Field::new("properties", DataType::Utf8View, true).with_extension_type(Json::default()),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ]))
});

pub(crate) struct SimulationMetaBuilder {
    id: StringViewBuilder,
    properties: StringViewBuilder,
    created_at: TimestampMillisecondBuilder,
}

impl SimulationMetaBuilder {
    pub fn new() -> Self {
        Self {
            id: StringViewBuilder::new(),
            properties: StringViewBuilder::new(),
            created_at: TimestampMillisecondBuilder::new().with_timezone("UTC"),
        }
    }

    pub fn add_simulation(&mut self, id: &Uuid, properties: Option<String>) {
        self.id.append_value(id.to_string());
        self.properties.append_option(properties);
        self.created_at.append_value(Utc::now().timestamp_millis());
    }

    pub fn build(mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            SIMULATION_META_SCHEMA.clone(),
            vec![
                Arc::new(self.id.finish()),
                Arc::new(self.properties.finish()),
                Arc::new(self.created_at.finish()),
            ],
        )?)
    }
}

pub struct SystemSchema<'a> {
    pub(super) ctx: &'a SessionContext,
}

impl<'a> SystemSchema<'a> {
    pub(in crate::context) fn new(ctx: &'a SessionContext) -> Self {
        Self { ctx }
    }

    fn ctx(&self) -> &SessionContext {
        self.ctx
    }

    pub(crate) async fn routing_nodes(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str] = &["location", "id", "properties", "geometry"];
        self.select_table(&ROUTING_NODES_REF, COLUMNS).await
    }

    pub(crate) async fn routing_edges(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str] = &["location", "source", "target", "properties", "geometry"];
        self.select_table(&ROUTING_EDGES_REF, COLUMNS).await
    }

    pub async fn simulations(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str] = &["id", "properties", "created_at"];
        self.select_table(&SIMULATION_META_REF, COLUMNS).await
    }

    pub async fn snapshots(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str] = &[
            "id",
            "simulation_id",
            "simulation_time",
            "properties",
            "created_at",
        ];
        self.select_table(&SNAPSHOT_META_REF, COLUMNS).await
    }

    async fn select_table(
        &self,
        table_ref: &TableReference,
        columns: &[&str],
    ) -> Result<DataFrame> {
        let schema = {
            let state = self.ctx().state_ref();
            state.read().schema_for_ref(table_ref.clone())?
        };
        let Some(table) = schema.table(table_ref.table()).await? else {
            return Err(Error::internal(format!(
                "Table '{}' not registered",
                table_ref
            )));
        };
        Ok(self.ctx().read_table(table)?.select_columns(columns)?)
    }
}

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaBuilder};
use chrono::{DateTime, Utc};
use datafusion::catalog::CatalogProvider;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{DataFrame, Expr, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use url::Url;
use uuid::Uuid;

pub(crate) use self::schemas::system::{ROUTING_EDGES_REF, ROUTING_NODES_REF};
pub(crate) use self::storage::storage_catalog;
use crate::context::memory::in_memory_catalog;
use crate::context::schemas::SystemSchema;
use crate::{Error, ObjectData, OrderData, PopulationData, Result, State, resolve_url};

use self::schemas::{SIMULATION_META_REF, SimulationMetaBuilder, create_snapshot};

mod memory;
mod schemas;
pub(crate) mod storage;

#[derive(Default)]
pub struct SimulationContextBuilder {
    simulation_id: Option<Uuid>,
    snapshot_id: Option<Uuid>,

    working_directory: Option<Url>,
    use_in_memory: bool,

    object_data: Option<ObjectData>,
    population_data: Option<RecordBatch>,

    simulation_start_time: Option<DateTime<Utc>>,
    simulation_time_step: Option<Duration>,
}

impl SimulationContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_simulation_id(mut self, simulation_id: impl Into<Option<Uuid>>) -> Self {
        self.simulation_id = simulation_id.into();
        self
    }

    pub fn with_snapshot_id(mut self, snapshot_id: impl Into<Option<Uuid>>) -> Self {
        self.snapshot_id = snapshot_id.into();
        self
    }

    pub fn with_working_directory(mut self, working_directory: impl Into<Option<Url>>) -> Self {
        self.working_directory = working_directory.into();
        self
    }

    pub fn with_simulation_start_time(
        mut self,
        simulation_start_time: impl Into<Option<DateTime<Utc>>>,
    ) -> Self {
        self.simulation_start_time = simulation_start_time.into();
        self
    }

    pub fn with_simulation_time_step(
        mut self,
        simulation_time_step: impl Into<Option<Duration>>,
    ) -> Self {
        self.simulation_time_step = simulation_time_step.into();
        self
    }

    pub fn with_use_in_memory(mut self, use_in_memory: bool) -> Self {
        self.use_in_memory = use_in_memory;
        self
    }

    pub fn with_object_data(mut self, objects_provider: ObjectData) -> Self {
        self.object_data = Some(objects_provider);
        self
    }

    pub fn with_population_data(mut self, population_data: RecordBatch) -> Self {
        self.population_data = Some(population_data);
        self
    }

    fn session(&self) -> (SessionContext, Uuid) {
        let simulation_id = self.simulation_id.unwrap_or_else(Uuid::now_v7);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_session_id(simulation_id.to_string())
            .build();

        let ctx = SessionContext::new_with_state(state);

        (ctx, simulation_id)
    }

    pub async fn load_snapshots(&self) -> Result<DataFrame> {
        let (ctx, _) = self.session();
        let Some(working_directory) = &self.working_directory else {
            return Err(Error::internal("System location not set"));
        };
        let catalog = storage_catalog(working_directory)?;
        ctx.register_catalog("caspers", catalog);
        let system = SystemSchema::new(&ctx);

        let df = system.snapshots().await?;
        if let Some(simulation_id) = self.simulation_id {
            return Ok(df
                .filter(
                    col("simulation_id")
                        .eq(lit(ScalarValue::Utf8View(Some(simulation_id.to_string())))),
                )?
                .sort(vec![col("id").sort(false, false)])?);
        }
        Ok(df)
    }

    pub async fn load_simulations(&self) -> Result<DataFrame> {
        let (ctx, _) = self.session();

        let Some(working_directory) = &self.working_directory else {
            return Err(Error::internal("System location not set"));
        };
        let catalog = storage_catalog(working_directory)?;
        ctx.register_catalog("caspers", catalog);
        let system = SystemSchema::new(&ctx);
        system.simulations().await
    }

    pub async fn build(self) -> Result<SimulationContext> {
        let (ctx, simulation_id) = self.session();

        let catalog = self.build_catalog(&ctx).await?;
        ctx.register_catalog("caspers", catalog);

        let snapshot_id = if let Some(snapshot_id) = self.snapshot_id {
            snapshot_id
        } else {
            Uuid::now_v7()
        };

        let mut sim_ctx = SimulationContext {
            ctx,
            simulation_id,
            snapshot_id,
            current_time: self.simulation_start_time.unwrap_or_else(Utc::now),
            time_step: self
                .simulation_time_step
                .unwrap_or_else(|| Duration::new(60, 0)),
        };

        // TODO: this is a but of a backdoor to allow for initializing a simulation
        // with some data. Idelly this would move to somewhere more separated.
        match (self.population_data, self.object_data) {
            (None, None) => (),
            (Some(population_data), Some(object_data)) => {
                let population = sim_ctx.ctx().read_batch(population_data)?;
                let population_data = PopulationData::try_new(population).await?;
                let sim_state = State::new(
                    &Default::default(),
                    object_data,
                    population_data,
                    OrderData::empty(),
                    Default::default(),
                );
                sim_ctx.write_snapshot(&sim_state).await?;
            }
            _ => {
                return Err(Error::internal(
                    "To initialize simulation, both population and object data are required",
                ));
            }
        }

        // if no id was assigned, we created a new simulation and now need to register it
        if self.simulation_id.is_none() {
            let mut builder = SimulationMetaBuilder::new();
            builder.add_simulation(&simulation_id, None);
            let batch = builder.build()?;
            let df = sim_ctx.ctx().read_batch(batch)?;
            let write_options =
                DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
            df.write_table(SIMULATION_META_REF.to_string().as_str(), write_options)
                .await?;
        }

        Ok(sim_ctx)
    }

    async fn build_catalog(&self, _ctx: &SessionContext) -> Result<Arc<dyn CatalogProvider>> {
        if let Some(working_directory) = &self.working_directory {
            let catalog_location = resolve_url(working_directory.into())?;
            storage_catalog(&catalog_location)
        } else if self.use_in_memory {
            in_memory_catalog()
        } else {
            Err(Error::internal("Results location is not provided"))
        }
    }
}

pub struct SimulationContext {
    simulation_id: Uuid,
    snapshot_id: Uuid,
    current_time: DateTime<Utc>,
    time_step: Duration,
    ctx: SessionContext,
}

impl SimulationContext {
    pub fn builder() -> SimulationContextBuilder {
        SimulationContextBuilder::default()
    }

    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn snapshot_id(&self) -> &Uuid {
        &self.snapshot_id
    }

    pub fn simulation_id(&self) -> &Uuid {
        &self.simulation_id
    }

    pub fn current_time(&self) -> &DateTime<Utc> {
        &self.current_time
    }

    pub(crate) fn current_time_expr(&self) -> Expr {
        static TZ: LazyLock<Arc<str>> = LazyLock::new(|| "UTC".into());
        lit(ScalarValue::TimestampMillisecond(
            Some(self.current_time().timestamp_millis()),
            Some(TZ.clone()),
        ))
    }

    pub fn time_step(&self) -> &Duration {
        &self.time_step
    }

    pub(crate) fn step_time(&mut self) {
        self.current_time += self.time_step;
    }

    pub fn system(&self) -> schemas::SystemSchema<'_> {
        schemas::SystemSchema::new(&self.ctx)
    }

    pub fn snapshots(&self) -> schemas::SnapshotsSchema<'_> {
        schemas::SnapshotsSchema::new(self)
    }

    pub fn results(&self) -> schemas::ResultsSchema<'_> {
        schemas::ResultsSchema::new(self)
    }

    /// Write the current simulation state to a snapshot.
    ///
    /// This method creates a new snapshot with the current simulation state
    /// and updates the simulation context to track the new snapshot ID.
    pub async fn write_snapshot(&mut self, state: &State) -> Result<()> {
        let snapshot_id = create_snapshot(state, self).await?;
        self.snapshot_id = snapshot_id;
        Ok(())
    }

    async fn scan(&self, table_ref: &TableReference) -> Result<DataFrame> {
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
        Ok(self.ctx().read_table(table)?)
    }

    /// Read a table filtered by the current simulation ID and snapshot ID.
    async fn scan_scoped(&self, table_ref: &TableReference) -> Result<DataFrame> {
        tracing::debug!(target: "caspers::simulation::context", "Scanning table '{}'", table_ref);

        let table = self.scan(table_ref).await?;
        let predicate = col("simulation_id")
            .eq(lit(ScalarValue::Utf8View(Some(
                self.simulation_id.to_string(),
            ))))
            .and(col("snapshot_id").eq(lit(ScalarValue::Utf8View(Some(
                self.snapshot_id.to_string(),
            )))));
        Ok(table
            .filter(predicate)?
            .drop_columns(&["simulation_id", "snapshot_id"])?)
    }

    fn extend_df(&self, df: DataFrame) -> Result<DataFrame> {
        let sim_id = ScalarValue::Utf8View(Some(self.simulation_id.to_string()));
        let sn_id = ScalarValue::Utf8View(Some(self.snapshot_id.to_string()));
        Ok(df
            .with_column("simulation_id", lit(sim_id))?
            .with_column("snapshot_id", lit(sn_id))?)
    }
}

fn wrap_schema(schema: &Schema) -> SchemaRef {
    static SIMULATION_ID_FIELD: LazyLock<FieldRef> =
        LazyLock::new(|| Field::new("simulation_id", DataType::Utf8View, false).into());
    static SNAPSHOT_ID_FIELD: LazyLock<FieldRef> =
        LazyLock::new(|| Field::new("snapshot_id", DataType::Utf8View, false).into());
    let mut builder = SchemaBuilder::new();
    for field in schema.fields() {
        builder.push(field.clone());
    }
    builder.push(SIMULATION_ID_FIELD.clone());
    builder.push(SNAPSHOT_ID_FIELD.clone());
    builder.finish().into()
}

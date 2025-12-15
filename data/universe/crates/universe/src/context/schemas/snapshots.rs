use std::sync::LazyLock;

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{DataFrame, lit};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use itertools::Itertools;
use uuid::Uuid;

use crate::context::SimulationContext;
use crate::{Result, State};

use super::system::{SNAPSHOT_META_REF, SnapshotMetaBuilder};

pub struct SnapshotsSchema<'a> {
    ctx: &'a SimulationContext,
}

pub(in crate::context) static SNAPSHOTS_SCHEMA_NAME: &str = "snapshots";
pub(in crate::context) static POPULATION_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", SNAPSHOTS_SCHEMA_NAME, "population"));
pub(in crate::context) static OBJECTS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", SNAPSHOTS_SCHEMA_NAME, "objects"));
pub(in crate::context) static ORDERS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", SNAPSHOTS_SCHEMA_NAME, "orders"));
pub(in crate::context) static ORDER_LINES_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", SNAPSHOTS_SCHEMA_NAME, "order_lines"));

impl<'a> SnapshotsSchema<'a> {
    pub(in crate::context) fn new(ctx: &'a SimulationContext) -> Self {
        Self { ctx }
    }

    pub async fn objects(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str; 5] = &["id", "parent_id", "label", "name", "properties"];
        Ok(self
            .ctx
            .scan_scoped(&OBJECTS_REF)
            .await?
            .select_columns(COLUMNS)?)
    }

    pub async fn population(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str; 6] = &["id", "role", "status", "properties", "position", "state"];
        Ok(self
            .ctx
            .scan_scoped(&POPULATION_REF)
            .await?
            .select_columns(COLUMNS)?)
    }

    pub async fn orders(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str] = &["id", "site_id", "customer_id", "destination", "status"];
        Ok(self
            .ctx
            .scan_scoped(&ORDERS_REF)
            .await?
            .select_columns(COLUMNS)?)
    }

    pub async fn order_lines(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str] = &["id", "order_id", "brand_id", "menu_item_id", "status"];
        Ok(self
            .ctx
            .scan_scoped(&ORDER_LINES_REF)
            .await?
            .select_columns(COLUMNS)?)
    }
}

pub(crate) async fn create_snapshot(state: &State, ctx: &SimulationContext) -> Result<Uuid> {
    let snapshot_id = Uuid::now_v7();
    let id_val = ScalarValue::Utf8View(Some(snapshot_id.to_string()));
    let sim_id_val = ScalarValue::Utf8View(Some(ctx.simulation_id.to_string()));

    let append_cols = |df: DataFrame| -> Result<DataFrame> {
        Ok(df
            .with_column("simulation_id", lit(sim_id_val.clone()))?
            .with_column("snapshot_id", lit(id_val.clone()))?)
    };

    let mut tasks_defs = vec![];

    let batch_objects = state.objects().objects();
    if batch_objects.num_rows() > 0 {
        let df_objects = ctx.ctx().read_batch(batch_objects.clone())?;
        tasks_defs.push((OBJECTS_REF.to_string(), append_cols(df_objects)?))
    }

    let batch_population = state.population().snapshot();
    if batch_population.num_rows() > 0 {
        let df_population = ctx.ctx().read_batch(batch_population.clone())?;
        tasks_defs.push((POPULATION_REF.to_string(), append_cols(df_population)?))
    }

    let batch_orders = state.orders().batch_orders();
    if batch_orders.num_rows() > 0 {
        let df_orders = ctx.ctx().read_batch(batch_orders.clone())?;
        tasks_defs.push((ORDERS_REF.to_string(), append_cols(df_orders)?))
    }

    let batch_order_lines = state.orders().batch_lines();
    if batch_order_lines.num_rows() > 0 {
        let df_order_lines = ctx.ctx().read_batch(batch_order_lines.clone())?;
        tasks_defs.push((ORDER_LINES_REF.to_string(), append_cols(df_order_lines)?))
    }

    let mut batch_sn = SnapshotMetaBuilder::new();
    batch_sn.add_snapshot(&snapshot_id, &ctx.simulation_id, state.current_time(), None);
    let batch_snapshot = batch_sn.build()?;
    let df_sn = ctx.ctx().read_batch(batch_snapshot)?;
    tasks_defs.push((SNAPSHOT_META_REF.to_string(), df_sn));

    let write_table = async |df: DataFrame, table_name: String| {
        let write_options =
            DataFrameWriteOptions::default().with_insert_operation(InsertOp::Append);
        df.write_table(table_name.as_str(), write_options).await
    };

    let tasks = tasks_defs
        .into_iter()
        .map(|(table_name, df)| write_table(df, table_name))
        .collect::<Vec<_>>();

    let _results: Vec<_> = futures::future::join_all(tasks)
        .await
        .into_iter()
        .try_collect()?;

    Ok(snapshot_id)
}

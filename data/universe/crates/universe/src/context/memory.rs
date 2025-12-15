use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::catalog::{
    CatalogProvider, MemTable, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    TableProvider,
};

use crate::builders::{
    EVENTS_SCHEMA, METRICS_SCHEMA, OBJECTS_SCHEMA, ORDER_LINE_SCHEMA, ORDER_SCHEMA,
    POPULATION_SCHEMA,
};
use crate::context::wrap_schema;
use crate::{Result, RoutingData};

use super::schemas::{
    EVENTS_REF, METRICS_REF, OBJECTS_REF, ORDER_LINES_REF, ORDERS_REF, POPULATION_REF,
    RESULTS_SCHEMA_NAME, ROUTING_EDGES_REF, ROUTING_NODES_REF, SIMULATION_META_REF,
    SIMULATION_META_SCHEMA, SNAPSHOT_META_REF, SNAPSHOT_META_SCHEMA, SNAPSHOTS_SCHEMA_NAME,
    SYSTEM_SCHEMA_NAME,
};

pub fn in_memory_catalog() -> Result<Arc<dyn CatalogProvider>> {
    let system_schema = Arc::new(MemorySchemaProvider::new());
    register_system(system_schema.as_ref())?;

    let snapshots_schema = Arc::new(MemorySchemaProvider::new());
    register_snapshots(snapshots_schema.as_ref())?;

    let results_schema = Arc::new(MemorySchemaProvider::new());
    register_results(results_schema.as_ref())?;

    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema(SYSTEM_SCHEMA_NAME, system_schema)?;
    catalog.register_schema(SNAPSHOTS_SCHEMA_NAME, snapshots_schema)?;
    catalog.register_schema(RESULTS_SCHEMA_NAME, results_schema)?;

    Ok(catalog)
}

fn register_system(schema: &dyn SchemaProvider) -> Result<()> {
    schema.register_table(
        ROUTING_NODES_REF.table().into(),
        mem_table(RoutingData::nodes_schema())?,
    )?;
    schema.register_table(
        ROUTING_EDGES_REF.table().into(),
        mem_table(RoutingData::edges_schema())?,
    )?;
    schema.register_table(
        SIMULATION_META_REF.table().into(),
        mem_table(SIMULATION_META_SCHEMA.clone())?,
    )?;
    schema.register_table(
        SNAPSHOT_META_REF.table().into(),
        mem_table(SNAPSHOT_META_SCHEMA.clone())?,
    )?;

    Ok(())
}

fn register_snapshots(schema: &dyn SchemaProvider) -> Result<()> {
    schema.register_table(
        POPULATION_REF.table().to_string(),
        mem_table(wrap_schema(&POPULATION_SCHEMA))?,
    )?;
    schema.register_table(
        OBJECTS_REF.table().to_string(),
        mem_table(wrap_schema(&OBJECTS_SCHEMA))?,
    )?;
    schema.register_table(
        ORDERS_REF.table().to_string(),
        mem_table(wrap_schema(&ORDER_SCHEMA))?,
    )?;
    schema.register_table(
        ORDER_LINES_REF.table().to_string(),
        mem_table(wrap_schema(&ORDER_LINE_SCHEMA))?,
    )?;

    Ok(())
}

fn register_results(schema: &dyn SchemaProvider) -> Result<()> {
    schema.register_table(
        METRICS_REF.table().to_string(),
        mem_table(wrap_schema(&METRICS_SCHEMA))?,
    )?;
    schema.register_table(
        EVENTS_REF.table().to_string(),
        mem_table(wrap_schema(&EVENTS_SCHEMA))?,
    )?;

    Ok(())
}

fn mem_table(schema: SchemaRef) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(MemTable::try_new(
        schema.clone(),
        vec![vec![RecordBatch::new_empty(schema)]],
    )?))
}

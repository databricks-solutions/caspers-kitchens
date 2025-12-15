use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider,
};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use url::Url;

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

pub fn storage_catalog(catalog_location: &Url) -> Result<Arc<dyn CatalogProvider>> {
    let system_schema = Arc::new(MemorySchemaProvider::new());
    let system_location = catalog_location.join(&format!("{}/", SYSTEM_SCHEMA_NAME))?;
    register_system(system_schema.as_ref(), &system_location)?;

    let snapshots_schema = Arc::new(MemorySchemaProvider::new());
    let snapshots_location = catalog_location.join(&format!("{}/", SNAPSHOTS_SCHEMA_NAME))?;
    register_snapshots(snapshots_schema.as_ref(), &snapshots_location)?;

    let results_schema = Arc::new(MemorySchemaProvider::new());
    let results_location = catalog_location.join(&format!("{}/", RESULTS_SCHEMA_NAME))?;
    register_results(results_schema.as_ref(), &results_location)?;

    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema(SYSTEM_SCHEMA_NAME, system_schema)?;
    catalog.register_schema(SNAPSHOTS_SCHEMA_NAME, snapshots_schema)?;
    catalog.register_schema(RESULTS_SCHEMA_NAME, results_schema)?;

    Ok(catalog)
}

pub(crate) fn register_system(schema: &dyn SchemaProvider, system_location: &Url) -> Result<()> {
    let nodes_path = system_location.join(&format!("{}/", ROUTING_NODES_REF.table()))?;
    let routing_nodes = parquet_provider(&nodes_path, RoutingData::nodes_schema())?;
    schema.register_table(ROUTING_NODES_REF.table().into(), routing_nodes)?;

    let edge_path = system_location.join(&format!("{}/", ROUTING_EDGES_REF.table()))?;
    let routing_edges = parquet_provider(&edge_path, RoutingData::edges_schema())?;
    schema.register_table(ROUTING_EDGES_REF.table().into(), routing_edges)?;

    let simulations_path = system_location.join(&format!("{}/", SIMULATION_META_REF.table()))?;
    let simulations = json_provider(&simulations_path, SIMULATION_META_SCHEMA.clone())?;
    schema.register_table(SIMULATION_META_REF.table().into(), simulations)?;

    let snapshots_path = system_location.join(&format!("{}/", SNAPSHOT_META_REF.table()))?;
    let snapshots = json_provider(&snapshots_path, SNAPSHOT_META_SCHEMA.clone())?;
    schema.register_table(SNAPSHOT_META_REF.table().into(), snapshots)?;

    Ok(())
}

fn register_snapshots(schema: &dyn SchemaProvider, snapshots_path: &Url) -> Result<()> {
    let population_path = snapshots_path.join(&format!("{}/", POPULATION_REF.table()))?;
    tracing::debug!(target: "caspers::simulation::context", "registering '{}' @ {}", *POPULATION_REF, population_path);
    let population_snapshot = parquet_provider(&population_path, wrap_schema(&POPULATION_SCHEMA))?;
    schema.register_table(POPULATION_REF.table().to_string(), population_snapshot)?;

    let objects_path = snapshots_path.join(&format!("{}/", OBJECTS_REF.table()))?;
    tracing::debug!(target: "caspers::simulation::context", "registering '{}' @ {}", *OBJECTS_REF, objects_path);
    let objects_snapshot = parquet_provider(&objects_path, wrap_schema(&OBJECTS_SCHEMA))?;
    schema.register_table(OBJECTS_REF.table().to_string(), objects_snapshot)?;

    let orders_path = snapshots_path.join(&format!("{}/", ORDERS_REF.table()))?;
    tracing::debug!(target: "caspers::simulation::context", "registering '{}' @ {}", *ORDERS_REF, orders_path);
    let orders_snapshot = parquet_provider(&orders_path, ORDER_SCHEMA.clone())?;
    schema.register_table(ORDERS_REF.table().to_string(), orders_snapshot)?;

    let order_lines_path = snapshots_path.join(&format!("{}/", ORDER_LINES_REF.table()))?;
    tracing::debug!(target: "caspers::simulation::context", "registering '{}' @ {}", *ORDER_LINES_REF, order_lines_path);
    let order_lines_snapshot = parquet_provider(&order_lines_path, ORDER_LINE_SCHEMA.clone())?;
    schema.register_table(ORDER_LINES_REF.table().to_string(), order_lines_snapshot)?;

    Ok(())
}

fn register_results(schema: &dyn SchemaProvider, results_path: &Url) -> Result<()> {
    let metrics_path = results_path.join(&format!("{}/", METRICS_REF.table()))?;
    tracing::debug!(target: "caspers::simulation::context", "registering '{}' @ {}", *METRICS_REF, metrics_path);
    let metrics_snapshot = parquet_provider(&metrics_path, wrap_schema(&METRICS_SCHEMA))?;
    schema.register_table(METRICS_REF.table().to_string(), metrics_snapshot)?;

    let events_path = results_path.join(&format!("{}/", EVENTS_REF.table()))?;
    tracing::debug!(target: "caspers::simulation::context", "registering '{}' @ {}", *EVENTS_REF, events_path);
    let events_snapshot = parquet_provider(&events_path, wrap_schema(&EVENTS_SCHEMA))?;
    schema.register_table(EVENTS_REF.table().to_string(), events_snapshot)?;

    Ok(())
}

fn parquet_provider(table_path: &Url, schema: SchemaRef) -> Result<Arc<dyn TableProvider>> {
    let table_path = ListingTableUrl::parse(table_path)?;

    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);

    Ok(Arc::new(ListingTable::try_new(config)?))
}

fn json_provider(table_path: &Url, schema: SchemaRef) -> Result<Arc<dyn TableProvider>> {
    let table_path = ListingTableUrl::parse(table_path)?;

    let file_format = JsonFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(".json");

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);

    Ok(Arc::new(ListingTable::try_new(config)?))
}

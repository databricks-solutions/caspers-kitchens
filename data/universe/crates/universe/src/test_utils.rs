use std::{path::PathBuf, process::Command};

use arrow::util::pretty::print_batches;
use datafusion::prelude::DataFrame;
#[cfg(test)]
use rstest::*;

use crate::{Error, Result, Simulation, Template};
#[cfg(test)]
use crate::{
    SimulationContext, SimulationRunner, SimulationRunnerBuilder, agents::functions::OrderSpec,
};

pub async fn setup_test_simulation(template: impl Into<Option<Template>>) -> Result<Simulation> {
    let caspers_root = find_git_root()?.join(".caspers/system/");
    let system_path = url::Url::from_directory_path(caspers_root)
        .map_err(|_| Error::internal("invalid directory"))?;
    Simulation::try_new_with_template(template.into().unwrap_or_default(), &system_path).await
}

pub fn find_git_root() -> Result<PathBuf> {
    let command = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .output()
        .map_err(Error::from)?;

    if !command.status.success() {
        return Err(Error::invalid_data("no git root found"));
    }

    let output = String::from_utf8(command.stdout).unwrap();
    Ok(std::fs::canonicalize(output.trim())?)
}

pub async fn print_frame(frame: &DataFrame) -> Result<()> {
    let frame = frame.clone().collect().await?;
    print_batches(&frame)?;
    Ok(())
}

#[cfg(test)]
#[fixture]
pub async fn simulation_context() -> Result<SimulationContext> {
    use crate::{
        EntityView, ObjectData, PopulationData, ROUTING_EDGES_REF, ROUTING_NODES_REF,
        context::storage::register_system,
    };
    use chrono::{Timelike as _, Utc};
    use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
    use rand::Rng as _;

    let caspers_root = find_git_root()?.join(".caspers/system/");
    let system_path = url::Url::from_directory_path(caspers_root)
        .map_err(|_| Error::internal("invalid directory"))?;

    let setup = crate::templates::Template::default().load()?;
    let objects = setup.object_data()?;
    let object_data = ObjectData::try_new(objects)?;

    let mut builder = PopulationData::builder();
    for site in object_data.sites()? {
        let n_people = rand::rng().random_range(500..1500);
        let info = site.properties()?;
        builder.add_site(n_people, info.latitude, info.longitude)?;
    }
    let population_data = builder.finish()?;

    let start_time = Utc::now();
    let start_time = start_time.with_hour(12).unwrap();

    let ctx = SimulationContext::builder()
        .with_use_in_memory(true)
        .with_population_data(population_data)
        .with_object_data(object_data)
        .with_simulation_start_time(start_time)
        .build()
        .await?;

    let schema = MemorySchemaProvider::new();
    register_system(&schema, &system_path)?;

    let nodes_table = schema.table(ROUTING_NODES_REF.table()).await?.unwrap();
    let edges_table = schema.table(ROUTING_EDGES_REF.table()).await?.unwrap();

    let df_nodes = ctx.ctx().read_table(nodes_table)?;
    df_nodes
        .write_table(ROUTING_NODES_REF.to_string().as_str(), Default::default())
        .await?;

    let df_edges = ctx.ctx().read_table(edges_table)?;
    df_edges
        .write_table(ROUTING_EDGES_REF.to_string().as_str(), Default::default())
        .await?;

    Ok(ctx)
}

#[cfg(test)]
#[fixture]
pub(crate) async fn builder() -> Result<SimulationRunnerBuilder> {
    use crate::agents::functions::create_order;

    let mut builder: SimulationRunnerBuilder = simulation_context().await?.into();
    builder = builder.with_create_orders(Box::new(create_order));
    Ok(builder)
}

#[cfg(test)]
#[fixture]
pub(crate) async fn runner(
    #[future] builder: Result<SimulationRunnerBuilder>,
) -> Result<SimulationRunner> {
    builder.await?.build().await
}

#[cfg(test)]
#[fixture]
pub(crate) async fn runner_fixed(
    #[default(OrderSpec::Once(vec![1]))] spec: OrderSpec,
    #[future] builder: Result<SimulationRunnerBuilder>,
) -> Result<SimulationRunner> {
    use crate::agents::functions::create_order_fixed;

    let create_order = Box::new(move |menu_items| create_order_fixed(menu_items, spec.clone()));
    let builder = builder.await?.with_create_orders(create_order);

    builder.build().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simulation() {
        let mut simulation = setup_test_simulation(None).await.unwrap();

        let event_stats = simulation.event_stats();
        assert_eq!(event_stats.num_orders_created, 0);

        simulation.run(100).await.unwrap();

        let event_stats = simulation.event_stats();
        assert!(event_stats.num_orders_created > 0);
    }
}

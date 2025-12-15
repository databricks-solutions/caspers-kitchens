use std::collections::HashMap;

use arrow::compute::concat_batches;
use chrono::{DateTime, Duration, Utc};
use datafusion::prelude::{col, lit};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::agents::{PopulationRunner, SiteRunner};
use crate::context::SimulationContext;
use crate::state::{EntityView, RoutingData, State};
use crate::{Error, EventTracker, ObjectData, OrderData, PopulationData, Result};

use super::{EventStatsBuffer, Simulation};

/// Execution mode for the simulation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SimulationMode {
    /// Run the simulation for the specified time horizon.
    Backfill,
    /// Align time passed in simulation with time passed in real time.
    Realtime,
    /// Continue simulation from last snapshot up to current time, then switch to real time.
    Catchup,
}

/// Configuration for the simulation engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationConfig {
    /// all ghost kitchen sites.
    pub(crate) simulation_start: DateTime<Utc>,

    /// time increment for simulation steps
    pub(crate) time_increment: Duration,

    pub(crate) dry_run: bool,

    pub(crate) write_events: bool,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        SimulationConfig {
            simulation_start: Utc::now(),
            time_increment: Duration::seconds(60),
            dry_run: false,
            write_events: false,
        }
    }
}

/// Builder for creating a simulation instance.
pub struct SimulationBuilder {
    ctx: Option<SimulationContext>,

    /// Time resolution for simulation steps
    time_increment: Duration,

    /// Start time for the simulation
    start_time: DateTime<Utc>,

    /// location to store simulation results
    working_directory: Option<Url>,

    /// Whether to run the simulation in dry run mode
    dry_run: bool,

    /// Whether to write events to the event tracker
    write_events: bool,
}

impl Default for SimulationBuilder {
    fn default() -> Self {
        Self {
            ctx: None,
            time_increment: Duration::minutes(1),
            start_time: Utc::now(),
            working_directory: None,
            dry_run: false,
            write_events: false,
        }
    }
}

impl SimulationBuilder {
    /// Create a new simulation builder with default parameters
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the simulation context for the simulation
    pub fn with_context(mut self, ctx: SimulationContext) -> Self {
        self.ctx = Some(ctx);
        self
    }

    /// Set the start time for the simulation
    pub fn with_start_time(mut self, start_time: DateTime<Utc>) -> Self {
        self.start_time = start_time;
        self
    }

    /// Set the time increment for the simulation
    pub fn with_time_increment(mut self, time_increment: Duration) -> Self {
        self.time_increment = time_increment;
        self
    }

    /// Set the result storage location for the simulation
    pub fn with_working_directory(mut self, working_location: impl Into<Url>) -> Self {
        let mut working_location = working_location.into();
        if !working_location.path().ends_with('/') {
            working_location.set_path(&format!("{}/", working_location.path()));
        }
        self.working_directory = Some(working_location);
        self
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    pub fn with_write_events(mut self, write_events: bool) -> Self {
        self.write_events = write_events;
        self
    }

    async fn build_context(&mut self) -> Result<SimulationContext> {
        if let Some(ctx) = self.ctx.take() {
            Ok(ctx)
        } else {
            SimulationContext::builder()
                .with_working_directory(self.working_directory.clone())
                .build()
                .await
        }
    }

    /// Load the prepared street network data into routing data objects.
    async fn build_state(
        &self,
        ctx: &SimulationContext,
        config: &SimulationConfig,
    ) -> Result<State> {
        tracing::debug!(target: "caspers::simulation::builder", "building simulation state");

        let objects = ctx.snapshots().objects().await?.collect().await?;
        let objects = ObjectData::try_new(concat_batches(objects[0].schema_ref(), &objects)?)?;

        tracing::debug!(target: "caspers::simulation::builder", "generating routers");
        let mut routers = HashMap::new();
        for site in objects.sites()? {
            let info = site.properties()?;

            let site_nodes = ctx
                .system()
                .routing_nodes()
                .await?
                .filter(col("location").eq(lit(&info.name)))?
                .collect()
                .await?;
            let site_nodes = concat_batches(site_nodes[0].schema_ref(), &site_nodes)?;

            let site_edges = ctx
                .system()
                .routing_edges()
                .await?
                .filter(col("location").eq(lit(&info.name)))?
                .collect()
                .await?;
            let site_edges = concat_batches(site_edges[0].schema_ref(), &site_edges)?;

            routers.insert(site.id(), RoutingData::try_new(site_nodes, site_edges)?);
        }

        tracing::debug!(target: "caspers::simulation::builder", "building population");
        let population = PopulationData::try_new_from_ctx(ctx).await?;
        let orders = OrderData::try_new(ctx).await?;

        Ok(State::new(config, objects, population, orders, routers))
    }

    /// Build the simulation with the given initial conditions
    pub async fn build(mut self) -> Result<Simulation> {
        let config = SimulationConfig {
            simulation_start: self.start_time,
            time_increment: self.time_increment,
            dry_run: self.dry_run,
            write_events: self.write_events,
        };

        let ctx = if let Some(ctx) = self.ctx.take() {
            ctx
        } else {
            self.build_context().await?
        };

        let state = self.build_state(&ctx, &config).await?;

        let sites = state
            .objects()
            .sites()?
            .map(|site| Ok::<_, Error>((site.id(), SiteRunner::try_new(site.id(), &state)?)))
            .try_collect()?;

        Ok(Simulation {
            population: PopulationRunner::try_new(&ctx).await?,
            ctx,
            config,
            state,
            sites,
            event_tracker: EventTracker::new(),
            stats_buffer: EventStatsBuffer::new(),
        })
    }
}

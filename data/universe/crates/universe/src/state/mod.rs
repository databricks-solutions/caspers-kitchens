//! Internal state management for the simulation.
//!
//! This module provides structures and utilities to manage the internal state of the simulation.
//! Whenever feasible, state is tracked as Arrow RecordBatches for seamless introp with
//! external data storages that might be used to store the state.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow::array::cast::AsArray as _;
use chrono::{DateTime, Utc};
use datafusion::prelude::{Expr, lit};
use datafusion::scalar::ScalarValue;
use geo_traits::PointTrait;
use itertools::Itertools as _;
use uuid::{ContextV7, Timestamp, Uuid};

use crate::{
    Error, EventPayload, OrderLineUpdatedPayload, OrderUpdatedPayload, Result, SimulationConfig,
    SimulationContext,
};
use crate::{OrderDataBuilder, idents::*};

use self::movement::JourneyPlanner;

pub(crate) use self::movement::{Journey, RoutingData, Transport, next::MovementHandler};
pub use self::objects::{ObjectData, ObjectLabel};
pub use self::orders::OrderData;
pub(crate) use self::orders::{OrderLineStatus, OrderStatus};
pub(crate) use self::parse_json::parse_json;
pub use self::population::{
    PersonRole, PersonState, PersonStatus, PersonStatusFlag, PopulationData,
};

mod movement;
mod objects;
mod orders;
mod parse_json;
mod population;

#[derive(Debug, thiserror::Error)]
enum StateError {
    // inconsistent data
    #[error("Inconsistent data")]
    InconsistentData,
}

impl From<StateError> for Error {
    fn from(err: StateError) -> Self {
        Error::InternalError(err.to_string())
    }
}

pub struct State {
    /// Current simulation time
    time: DateTime<Utc>,

    /// Time increment per simulation step
    time_step: Duration,

    /// Population data
    population: PopulationData,

    /// Vendor data
    objects: ObjectData,

    /// Routing data
    routing: HashMap<SiteId, JourneyPlanner>,

    /// Order data
    orders: OrderData,

    ts_context: ContextV7,
}

impl State {
    pub(crate) fn new(
        config: &SimulationConfig,
        objects: ObjectData,
        population: PopulationData,
        orders: OrderData,
        routing: HashMap<SiteId, RoutingData>,
    ) -> Self {
        Self {
            time_step: Duration::from_secs(config.time_increment.num_seconds() as u64),
            time: config.simulation_start,
            population,
            objects,
            orders,
            ts_context: ContextV7::new(),
            routing: routing
                .into_iter()
                .map(|(id, data)| (id, data.into_trip_planner()))
                .collect(),
        }
    }

    pub fn objects(&self) -> &ObjectData {
        &self.objects
    }

    pub fn population(&self) -> &PopulationData {
        &self.population
    }

    pub fn orders(&self) -> &OrderData {
        &self.orders
    }

    pub fn trip_planner(&self, site_id: &SiteId) -> Option<&JourneyPlanner> {
        self.routing.get(site_id)
    }

    pub fn current_time(&self) -> DateTime<Utc> {
        self.time
    }

    pub fn current_time_expr(&self) -> Expr {
        static TZ: LazyLock<Arc<str>> = LazyLock::new(|| "UTC".into());
        lit(ScalarValue::TimestampMillisecond(
            Some(self.current_time().timestamp_millis()),
            Some(TZ.clone()),
        ))
    }

    /// Timestamp used to generate v7 uuids
    pub fn current_timestamp(&self) -> Timestamp {
        Timestamp::from_unix(
            &self.ts_context,
            self.time.timestamp() as u64,
            self.time.timestamp_subsec_nanos(),
        )
    }

    pub fn time_step(&self) -> Duration {
        self.time_step
    }

    pub(crate) fn next_time(&self) -> DateTime<Utc> {
        self.time + self.time_step
    }

    pub(crate) fn process_site_events(&mut self, events: &[EventPayload]) -> Result<()> {
        let order_line_updates = events.iter().filter_map(|event| match event {
            EventPayload::OrderLineUpdated(payload) => Some(payload),
            _ => None,
        });
        self.update_order_lines(order_line_updates)?;
        let order_updates = events.iter().filter_map(|event| match event {
            EventPayload::OrderUpdated(payload) => Some(payload),
            _ => None,
        });
        self.update_orders(order_updates)?;

        Ok(())
    }

    pub(crate) fn process_population_events(
        &mut self,
        events: &[EventPayload],
    ) -> Result<Vec<EventPayload>> {
        let new_orders = events.iter().filter_map(|event| match event {
            EventPayload::OrderCreated(payload) => Some(payload),
            _ => None,
        });

        let mut builder = OrderDataBuilder::new();
        for order in new_orders {
            builder.add_order(
                order.site_id,
                order.person_id,
                order
                    .destination
                    .coord()
                    .ok_or_else(|| Error::invalid_data("no destination coordinates"))?
                    .try_into()?,
                &order.items,
            )?;
        }
        let order_data = builder.finish()?;

        let order_ids = order_data
            .all_orders()
            .map(|o| {
                EventPayload::OrderUpdated(OrderUpdatedPayload {
                    order_id: *o.id(),
                    status: OrderStatus::Submitted,
                    actor_id: None,
                })
            })
            .collect_vec();
        self.orders = self.orders.merge(order_data)?;
        Ok(order_ids)
    }

    fn update_order_lines<'a>(
        &mut self,
        updates: impl IntoIterator<Item = &'a OrderLineUpdatedPayload>,
    ) -> Result<()> {
        self.orders.update_order_lines(
            updates
                .into_iter()
                .map(|payload| (payload.order_line_id, &payload.status)),
        )?;
        Ok(())
    }

    fn update_orders<'a>(
        &mut self,
        updates: impl IntoIterator<Item = &'a OrderUpdatedPayload>,
    ) -> Result<()> {
        self.orders.update_orders(
            updates
                .into_iter()
                .map(|payload| (payload.order_id, &payload.status)),
        )?;
        Ok(())
    }

    /// Advance people's journeys and update their statuses on arrival at their destination.
    pub(super) async fn move_people(
        &mut self,
        ctx: &SimulationContext,
    ) -> Result<Vec<EventPayload>> {
        self.population
            .update_journeys(ctx, &self.time, self.time_step, &self.orders)
            .await
    }

    pub(super) async fn step<'a>(
        &mut self,
        ctx: &SimulationContext,
        events: impl IntoIterator<Item = &'a EventPayload>,
    ) -> Result<()> {
        let updates = events.into_iter().filter_map(|e| {
            if let EventPayload::PersonUpdated(payload) = e {
                Some((&payload.person_id, &payload.status))
            } else {
                None
            }
        });
        self.population.update_person_status(ctx, updates).await?;

        self.step_time();

        Ok(())
    }

    pub(super) fn step_time(&mut self) {
        self.time += self.time_step;
    }
}

pub trait EntityView {
    type Id: TypedId;
    type Properties: serde::de::DeserializeOwned;

    fn data(&self) -> &ObjectData;

    fn valid_index(&self) -> usize;

    fn id(&self) -> Self::Id {
        Uuid::from_slice(
            self.data()
                .objects()
                .column_by_name("id")
                .expect("object data schema should be validated")
                .as_fixed_size_binary()
                .value(self.valid_index()),
        )
        .unwrap()
        .into()
    }

    fn properties(&self) -> Result<Self::Properties> {
        let raw = self
            .data()
            .objects()
            .column_by_name("properties")
            .ok_or(StateError::InconsistentData)?
            .as_string::<i64>()
            .value(self.valid_index());
        Ok(serde_json::from_str(raw)?)
    }
}

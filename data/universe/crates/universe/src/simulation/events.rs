use chrono::{DateTime, Utc};
use datafusion::common::HashMap;
use geo::Point;
use serde::{Deserialize, Serialize};
use tracing::info_span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::State;
use crate::idents::{BrandId, KitchenId, MenuItemId, OrderId, OrderLineId, PersonId, SiteId};
use crate::state::{OrderLineStatus, OrderStatus, PersonStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub timestamp: DateTime<Utc>,
    pub payload: EventPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCreatedPayload {
    pub site_id: SiteId,
    pub person_id: PersonId,
    pub items: Vec<(BrandId, MenuItemId)>,
    pub destination: Point,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonUpdatedPayload {
    pub person_id: PersonId,
    pub status: PersonStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdatedPayload {
    pub order_id: OrderId,
    pub status: OrderStatus,
    pub actor_id: Option<PersonId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderLineUpdatedPayload {
    pub order_line_id: OrderLineId,
    pub status: OrderLineStatus,
    pub kitchen_id: Option<KitchenId>,
    pub actor_id: Option<PersonId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventPayload {
    PersonUpdated(PersonUpdatedPayload),
    OrderUpdated(OrderUpdatedPayload),
    OrderLineUpdated(OrderLineUpdatedPayload),
    OrderCreated(OrderCreatedPayload),
}

impl EventPayload {
    pub fn person_updated(person_id: PersonId, status: PersonStatus) -> Self {
        Self::PersonUpdated(PersonUpdatedPayload { person_id, status })
    }

    pub fn order_updated(
        order_id: OrderId,
        status: OrderStatus,
        actor_id: Option<PersonId>,
    ) -> Self {
        Self::OrderUpdated(OrderUpdatedPayload {
            order_id,
            status,
            actor_id,
        })
    }

    pub fn order_line_updated(
        order_line_id: OrderLineId,
        status: OrderLineStatus,
        kitchen_id: Option<KitchenId>,
        actor_id: Option<PersonId>,
    ) -> Self {
        Self::OrderLineUpdated(OrderLineUpdatedPayload {
            order_line_id,
            status,
            kitchen_id,
            actor_id,
        })
    }

    pub fn order_failed(order_id: OrderId, actor_id: Option<PersonId>) -> Self {
        Self::OrderUpdated(OrderUpdatedPayload {
            order_id,
            status: OrderStatus::Failed,
            actor_id,
        })
    }
}

pub struct EventTracker {
    order_spans: HashMap<OrderId, tracing::Span>,
    order_line_spans: HashMap<OrderLineId, tracing::Span>,
    delivery_spans: HashMap<OrderId, tracing::Span>,
    pub(super) total_stats: EventStats,
}

impl Default for EventTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl EventTracker {
    pub fn new() -> Self {
        Self {
            order_spans: HashMap::new(),
            order_line_spans: HashMap::new(),
            total_stats: EventStats::new(),
            delivery_spans: HashMap::new(),
        }
    }

    pub fn process_events(&mut self, events: &[EventPayload], ctx: &State) -> EventStats {
        let mut stats = EventStats::new();
        for event in events {
            stats.handle_event(event);
            self.handle_event(event, ctx);
        }
        self.total_stats.add(&stats);
        stats
    }

    fn handle_event(&mut self, event: &EventPayload, ctx: &State) {
        match event {
            EventPayload::OrderCreated(_) => {}
            EventPayload::OrderUpdated(payload) => self.handle_order_updated(payload, ctx),
            EventPayload::OrderLineUpdated(payload) => self.handle_order_line_updated(payload, ctx),
            EventPayload::PersonUpdated(payload) => self.handle_person_updated(payload, ctx),
        }
    }

    fn handle_order_updated(&mut self, payload: &OrderUpdatedPayload, _ctx: &State) {
        if payload.status == OrderStatus::Submitted {
            let span = info_span!(
                target: "caspers::universe::orders",
                parent: None,
                "order_processing",
                caspers.order_id = payload.order_id.to_string(),
            );
            self.order_spans.insert(payload.order_id, span);
        }

        if let Some(span) = self.order_spans.get(&payload.order_id) {
            span.in_scope(|| {
                tracing::info!(
                    caspers.new_status = payload.status.to_string(),
                    "order_updated"
                );
            });

            if payload.status == OrderStatus::PickedUp {
                let span = info_span!(
                    target: "caspers::universe::orders",
                    parent: span,
                    "delivering_order",
                    caspers.courier_id = payload.actor_id.map(|id| id.to_string()).unwrap_or_else(|| "missing".to_string()),
                );
                self.delivery_spans.insert(payload.order_id, span);
            }
        }

        if payload.status == OrderStatus::Delivered
            && let Some(span) = self.order_spans.remove(&payload.order_id)
        {
            span.set_status(opentelemetry::trace::Status::Ok);
            if let Some(delivery_span) = self.delivery_spans.get(&payload.order_id) {
                delivery_span.in_scope(|| {
                    tracing::info!("order_delivered");
                });
                delivery_span.set_status(opentelemetry::trace::Status::Ok);
            }
        };

        if payload.status == OrderStatus::Failed
            && let Some(span) = self.order_spans.remove(&payload.order_id)
        {
            span.set_status(opentelemetry::trace::Status::Error {
                description: "order failed".into(),
            });
            if let Some(delivery_span) = self.delivery_spans.get(&payload.order_id) {
                delivery_span.set_status(opentelemetry::trace::Status::Error {
                    description: "order failed".into(),
                });
            }
        };
    }

    fn handle_order_line_updated(&mut self, payload: &OrderLineUpdatedPayload, ctx: &State) {
        if let Some(line) = ctx.orders().order_line(&payload.order_line_id)
            && payload.status == OrderLineStatus::Assigned
        {
            let order_id: OrderId = line.order_id().try_into().unwrap();
            if let Some(order_span) = self.order_spans.get(&order_id) {
                let line_span = info_span!(
                    target: "caspers::universe::orders",
                    parent: order_span,
                    "order_line_processing",
                    caspers.order_line_id = payload.order_line_id.to_string()
                );
                self.order_line_spans
                    .insert(payload.order_line_id, line_span);
            }
        }
        if let Some(span) = self.order_line_spans.get(&payload.order_line_id) {
            span.in_scope(|| {
                tracing::info!(
                    caspers.new_status = payload.status.to_string(),
                    "order_line_updated"
                );
            });
        }
        if payload.status == OrderLineStatus::Ready {
            self.order_line_spans.remove(&payload.order_line_id);
        }
    }

    fn handle_person_updated(&mut self, payload: &PersonUpdatedPayload, _ctx: &State) {
        match &payload.status {
            PersonStatus::Delivering(order_id, journey) => {
                if let Some(span) = self.delivery_spans.get(order_id) {
                    span.set_attribute(
                        "caspers.total_distance",
                        format!("{}m", journey.distance_m()),
                    );
                    span.in_scope(|| {
                        tracing::info!(
                            caspers.delivery_progress = journey.progress_percentage(),
                            caspers.estimated_time_remaining = journey.estimated_time_remaining_s(),
                            "out_for_delivery"
                        );
                    });
                }
            }
            PersonStatus::WaitingForCustomer(order_id, _) => {
                if let Some(span) = self.delivery_spans.get(order_id) {
                    span.in_scope(|| {
                        tracing::info!("waiting_for_customer");
                    });
                }
            }
            _ => (),
        }
    }
}

#[derive(Debug)]
pub struct EventStats {
    pub num_orders_created: u32,
    pub num_orders_updated: u32,
    pub num_order_lines_updated: u32,
    pub num_people_updated: u32,
}

impl Default for EventStats {
    fn default() -> Self {
        Self::new()
    }
}

impl EventStats {
    pub fn new() -> Self {
        Self {
            num_orders_created: 0,
            num_orders_updated: 0,
            num_order_lines_updated: 0,
            num_people_updated: 0,
        }
    }

    pub fn add(&mut self, other: &EventStats) {
        self.num_orders_created += other.num_orders_created;
        self.num_orders_updated += other.num_orders_updated;
        self.num_order_lines_updated += other.num_order_lines_updated;
        self.num_people_updated += other.num_people_updated;
    }

    pub fn handle_event(&mut self, event: &EventPayload) {
        match event {
            EventPayload::OrderCreated(_) => self.num_orders_created += 1,
            EventPayload::OrderUpdated(_) => self.num_orders_updated += 1,
            EventPayload::OrderLineUpdated(_) => self.num_order_lines_updated += 1,
            EventPayload::PersonUpdated(_) => self.num_people_updated += 1,
        }
    }
}

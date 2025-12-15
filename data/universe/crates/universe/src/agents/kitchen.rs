use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use itertools::Itertools as _;
use tracing::{Level, instrument};

use super::OrderLine;
use crate::EventPayload;
use crate::error::Result;
use crate::idents::*;
use crate::models::{KitchenStation, Station};
use crate::state::{OrderLineStatus, State};

pub use next::*;

#[path = "kitchen_next.rs"]
#[allow(unused)]
pub(crate) mod next;

#[derive(Clone)]
enum StationStatus {
    // Station is available for use
    Available,

    // Stores the recipe ID using this station
    Busy(OrderLineId),
}

/// A kitchen station
///
/// Represents a station in the kitchen where certain instructions can be executed.
/// This can be a workstation - i.e. a place where a chef can perform a task and
/// has cutting board, knives, and other necessary tools - or some other station
/// such as a freezer, stove, or oven.
#[derive(Clone)]
struct StationRunner {
    id: StationId,
    station_type: KitchenStation,
    status: StationStatus,
}

impl StationRunner {
    pub fn new(id: StationId, station: Station) -> Self {
        StationRunner {
            id,
            station_type: station.station_type(),
            status: StationStatus::Available,
        }
    }

    #[allow(unused)]
    pub(crate) fn id(&self) -> &StationId {
        &self.id
    }
}

#[derive(Clone)]
enum OrderLineProcessingStatus {
    // Current instruction index and start time
    Processing(usize, DateTime<Utc>),

    // Blocked at instruction index
    Blocked(usize),
}

#[derive(Clone)]
struct OrderProgress {
    // The order line item being processed
    order_line: OrderLine,

    // The processing status of the recipe
    status: OrderLineProcessingStatus,
}

pub struct KitchenRunner {
    id: KitchenId,
    stations: Vec<StationRunner>,
    queue: VecDeque<OrderLine>,
    in_progress: HashMap<OrderLineId, OrderProgress>,
    completed: Vec<(OrderId, OrderLineId)>,
    accepted_brands: HashSet<BrandId>,
}

impl KitchenRunner {
    pub(crate) fn id(&self) -> &KitchenId {
        &self.id
    }

    #[instrument(
        name = "step_kitchen",
        level = Level::TRACE,
        skip(self, ctx),
        fields(
            caspers.kitchen_id = self.id.to_string()
        )
    )]
    pub(crate) fn step(&mut self, ctx: &State) -> Result<Vec<EventPayload>> {
        let mut events = Vec::new();

        // Try to start new recipes if possible
        while self.start_order_line(ctx)? {}

        // Process in-progress recipes
        let mut completed_recipe_ids = Vec::new();
        let mut to_update = Vec::new();

        for (order_line_id, progress) in self.in_progress.iter() {
            let menu_item = ctx.objects().menu_item(&progress.order_line.item.1)?;
            match &progress.status {
                OrderLineProcessingStatus::Processing(instruction_idx, stated_time) => {
                    let expected_duration =
                        menu_item.instructions[*instruction_idx].expected_duration;

                    // Check if the recipe will be completed within the current time step
                    if (ctx.next_time() - stated_time).num_seconds() < expected_duration as i64 {
                        continue;
                    }

                    // We finished to current step, so release the current asset
                    let curr = &menu_item.instructions[*instruction_idx];
                    release_station(&mut self.stations, &curr.required_station, order_line_id);

                    // Move to next instruction
                    let next_idx = instruction_idx + 1;
                    if next_idx >= menu_item.instructions.len() {
                        // Recipe is complete
                        completed_recipe_ids.push(*order_line_id);
                        continue;
                    }

                    // Move the order to the next station, or block if not available
                    let next_step = &menu_item.instructions[next_idx];
                    if let Some(idx) = take_station(&self.stations, &next_step.required_station) {
                        self.stations[idx].status = StationStatus::Busy(*order_line_id);
                        to_update.push((
                            *order_line_id,
                            OrderLineProcessingStatus::Processing(next_idx, ctx.next_time()),
                        ));
                    } else {
                        to_update
                            .push((*order_line_id, OrderLineProcessingStatus::Blocked(next_idx)));
                    }
                }
                OrderLineProcessingStatus::Blocked(instruction_idx) => {
                    // Check if we can now acquire the needed asset
                    let step = &menu_item.instructions[*instruction_idx];
                    if let Some(asset_idx) = take_station(&self.stations, &step.required_station) {
                        // Mark asset as in use
                        self.stations[asset_idx].status = StationStatus::Busy(*order_line_id);
                        to_update.push((
                            *order_line_id,
                            OrderLineProcessingStatus::Processing(
                                *instruction_idx,
                                ctx.next_time(),
                            ),
                        ));
                    }
                }
            }
        }

        // Apply updates
        for (recipe_id, status) in to_update {
            if let Some(progress) = self.in_progress.get_mut(&recipe_id) {
                match status {
                    OrderLineProcessingStatus::Processing(_, _) => {
                        events.push(EventPayload::order_line_updated(
                            recipe_id,
                            OrderLineStatus::Processing,
                            Some(self.id),
                            None,
                        ));
                    }
                    OrderLineProcessingStatus::Blocked(_) => {
                        events.push(EventPayload::order_line_updated(
                            recipe_id,
                            OrderLineStatus::Waiting,
                            Some(self.id),
                            None,
                        ));
                    }
                }

                progress.status = status;
            }
        }

        // Move completed recipes
        for recipe_id in completed_recipe_ids {
            if let Some(progress) = self.in_progress.remove(&recipe_id) {
                self.completed
                    .push((progress.order_line.order_id, progress.order_line.id));
            }
        }

        Ok(events)
    }
}

impl KitchenRunner {
    pub fn try_new(
        id: KitchenId,
        brands: impl IntoIterator<Item = BrandId>,
        state: &State,
    ) -> Result<Self> {
        let stations = state
            .objects()
            .kitchen_stations(&id)?
            .map_ok(|(station_id, station)| StationRunner::new(station_id, station))
            .try_collect()?;
        Ok(KitchenRunner {
            id,
            stations,
            queue: VecDeque::new(),
            in_progress: HashMap::new(),
            completed: Vec::new(),
            accepted_brands: brands.into_iter().collect(),
        })
    }

    pub fn accepted_brands(&self) -> &HashSet<BrandId> {
        &self.accepted_brands
    }

    pub fn queue_order_line(&mut self, item: OrderLine) {
        self.queue.push_back(item);
    }

    fn start_order_line(&mut self, ctx: &State) -> Result<bool> {
        if let Some(order_line) = self.queue.pop_front() {
            let menu_item = ctx.objects().menu_item(&order_line.item.1)?;
            // Check if we can start the first step
            let step = &menu_item.instructions[0];
            if let Some(asset_idx) = take_station(&self.stations, &step.required_station) {
                // Mark asset as in use
                self.stations[asset_idx].status = StationStatus::Busy(order_line.id);

                // Add recipe to in-progress with first instruction
                self.in_progress.insert(
                    order_line.id,
                    OrderProgress {
                        order_line,
                        status: OrderLineProcessingStatus::Processing(0, ctx.current_time()),
                    },
                );

                Ok(true)
            } else {
                // Can't start the recipe yet, put it back in the queue
                self.queue.push_front(order_line);
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Get statistics about the kitchen's current state.
    pub fn stats(&self) -> KitchenStats {
        KitchenStats {
            queued: self.queue.len(),
            in_progress: self.in_progress.len(),
            completed: self.completed.len(),
            idle_stations: self
                .stations
                .iter()
                .filter(|a| matches!(a.status, StationStatus::Available))
                .count(),
            total_stations: self.stations.len(),
        }
    }

    pub fn take_completed(&mut self) -> Vec<(OrderId, OrderLineId)> {
        std::mem::take(&mut self.completed)
    }
}

fn take_station(assets: &[StationRunner], asset_type: &i32) -> Option<usize> {
    assets.iter().position(|asset| {
        matches!(asset.status, StationStatus::Available)
            && &(asset.station_type as i32) == asset_type
    })
}

fn release_station(assets: &mut Vec<StationRunner>, asset_type: &i32, recipe_id: &OrderLineId) {
    for asset in assets {
        if &(asset.station_type as i32) == asset_type
            && let StationStatus::Busy(id) = &asset.status
            && id == recipe_id
        {
            asset.status = StationStatus::Available;
            break;
        }
    }
}

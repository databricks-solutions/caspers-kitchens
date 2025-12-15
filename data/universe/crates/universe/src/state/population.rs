use std::convert::AsRef;
use std::sync::Arc;

use arrow::array::{DictionaryArray, FixedSizeBinaryBuilder, StringBuilder, StringViewBuilder};
use arrow::array::{RecordBatch, cast::AsArray as _};
use arrow::compute::concat_batches;
use arrow::datatypes::{Int8Type, Schema};
use chrono::{DateTime, Utc};
use datafusion::common::JoinType;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::prelude::{DataFrame, coalesce, col, lit};
use geo::Point;
use geoarrow::array::{PointArray, PointBuilder};
use geoarrow_array::IntoArrow;
use geoarrow_schema::{Dimension, PointType};
use h3o::{CellIndex, Resolution};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use uuid::Uuid;

use crate::builders::{POPULATION_SCHEMA, PopulationDataBuilder};
use crate::context::SimulationContext;
use crate::error::{Error, Result};
use crate::functions as f;
use crate::idents::{OrderId, PersonId};
use crate::{EventPayload, OrderData, OrderStatus};

use super::movement::Journey;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default, AsRefStr)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum PersonStatusFlag {
    #[default]
    Idle,
    AwaitingOrder,
    Eating,
    Moving,
    Delivering,
    WaitingForCustomer,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum PersonStatus {
    #[default]
    Idle,
    AwaitingOrder(OrderId),
    Eating(DateTime<Utc>),
    Moving(Journey),
    Delivering(OrderId, Journey),
    WaitingForCustomer(OrderId, Journey),
}

impl PersonStatus {
    pub fn flag(&self) -> PersonStatusFlag {
        match self {
            PersonStatus::Idle => PersonStatusFlag::Idle,
            PersonStatus::AwaitingOrder(_) => PersonStatusFlag::AwaitingOrder,
            PersonStatus::Eating(_) => PersonStatusFlag::Eating,
            PersonStatus::Moving(_) => PersonStatusFlag::Moving,
            PersonStatus::Delivering(_, _) => PersonStatusFlag::Delivering,
            PersonStatus::WaitingForCustomer(_, _) => PersonStatusFlag::WaitingForCustomer,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PersonState {
    status: PersonStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AsRefStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PersonRole {
    Customer,
    Courier,
}

pub struct PopulationData {
    /// Metadata for individuals tracked in the simulation
    population: RecordBatch,

    /// Current geo locations of people
    positions: PointArray,

    /// Lookup index for people.
    ///
    /// An [`IndexMap`] tracks the insertion order of [`PersonId`]s.
    /// as such we can use the "position" of a person in the [`IndexMap`] to
    /// efficiently lookup their [`Person`] data as it corresponds to
    /// the index value within the [`people`] array.
    lookup_index: IndexMap<PersonId, PersonState>,
}

impl PopulationData {
    pub fn builder() -> PopulationDataBuilder {
        PopulationDataBuilder::new()
    }

    pub(crate) async fn try_new(population: DataFrame) -> Result<Self> {
        let batches = population.collect().await?;
        let population = concat_batches(batches[0].schema_ref(), &batches)?;

        let positions = population
            .column_by_name("position")
            .ok_or_else(|| Error::invalid_data("Missing 'position' column"))?
            .as_struct();
        let point_type = PointType::new(Dimension::XY, Default::default());
        let positions: PointArray = (positions, point_type).try_into()?;

        let id_iter = population
            .column_by_name("id")
            .ok_or_else(|| Error::invalid_data("Missing 'id' column"))?
            .as_fixed_size_binary()
            .iter();
        let state_iter = population
            .column_by_name("state")
            .ok_or_else(|| Error::invalid_data("Missing 'state' column"))?
            .as_string_view()
            .iter();

        let lookup_index = id_iter
            .zip(state_iter)
            .map(|(id, state)| {
                let id = Uuid::from_slice(id.unwrap()).unwrap().into();
                let state = serde_json::from_str(state.unwrap()).unwrap();
                (id, state)
            })
            .collect();

        Ok(Self {
            population,
            positions,
            lookup_index,
        })
    }

    pub(crate) async fn try_new_from_ctx(ctx: &SimulationContext) -> Result<Self> {
        let population = ctx.snapshots().population().await?.cache().await?;
        Self::try_new(population).await
    }

    pub(crate) fn snapshot(&self) -> &RecordBatch {
        &self.population
    }

    pub(crate) async fn idle_people_in_cell(
        &self,
        ctx: &SimulationContext,
        cell_index: CellIndex,
        role: &PersonRole,
    ) -> Result<DataFrame> {
        let df = ctx.ctx().read_batch(self.population.clone())?.filter(
            col("status")
                .eq(lit(PersonStatusFlag::Idle.as_ref()))
                .and(col("role").eq(lit(role.as_ref()))),
        )?;
        filter_by_cell(df, cell_index)
    }

    pub(crate) async fn update_person_status(
        &mut self,
        ctx: &SimulationContext,
        updates: impl IntoIterator<Item = (&PersonId, &PersonStatus)>,
    ) -> Result<()> {
        let mut update_data = StatusUpdateBuilder::new();
        for (id, status) in updates {
            self.lookup_index.get_mut(id).ok_or(Error::NotFound)?.status = status.clone();
            update_data.add_update(id.as_ref(), status)?;
        }
        let df_updates = ctx.ctx().read_batch(update_data.finish()?)?.select(vec![
            col("id").alias("id_new"),
            col("status").alias("status_new"),
            col("state").alias("state_new"),
        ])?;
        let df_current = ctx.ctx().read_batch(self.population.clone())?;

        let joined = df_current
            .join(df_updates, JoinType::Left, &["id"], &["id_new"], None)?
            .select(vec![
                col("id"),
                col("role"),
                coalesce(vec![col("status_new"), col("status")]).alias("status"),
                col("properties"),
                col("position"),
                coalesce(vec![col("state_new"), col("state")]).alias("state"),
            ])?
            .collect()
            .await?;

        self.population = concat_batches(joined[0].schema_ref(), &joined)?;

        Ok(())
    }

    pub(super) async fn update_journeys(
        &mut self,
        ctx: &SimulationContext,
        current_time: &DateTime<Utc>,
        time_step: std::time::Duration,
        order_data: &OrderData,
    ) -> Result<Vec<EventPayload>> {
        let mut updates = PositionUpdateBuilder::new();
        let mut events = Vec::new();

        for (person_id, state) in self.lookup_index.iter_mut() {
            let (progress, next_status) = match &mut state.status {
                PersonStatus::Moving(journey) => {
                    let progress = journey.advance(time_step);
                    let next_status = journey.is_done().then_some(PersonStatus::Idle);
                    (Some(progress), next_status)
                }
                PersonStatus::Delivering(order_id, journey) => {
                    let progress = journey.advance(time_step);
                    let next_status = journey.is_done().then_some({
                        // couriers need to reverse their journey when they're done delivering
                        let mut journey = journey.clone();
                        journey.reset_reverse();
                        PersonStatus::WaitingForCustomer(*order_id, journey)
                    });
                    (Some(progress), next_status)
                }
                PersonStatus::WaitingForCustomer(order_id, journey) => {
                    if let Some(order) = order_data.order(order_id) {
                        events.push(EventPayload::order_updated(
                            *order_id,
                            OrderStatus::Delivered,
                            None,
                        ));
                        events.push(EventPayload::person_updated(
                            order.customer_person_id().try_into()?,
                            PersonStatus::Eating(
                                *current_time + chrono::Duration::seconds(30 * 60),
                            ),
                        ));
                    };
                    (None, Some(PersonStatus::Moving(journey.clone())))
                }
                _ => (None, None),
            };

            if let Some(next_status) = next_status {
                events.push(EventPayload::person_updated(*person_id, next_status))
            }
            if let Some(next_pos) = progress.as_ref().and_then(|p| p.last()) {
                updates.add_update(person_id, next_pos)?;
            }
        }

        let df_updates = ctx.ctx().read_batch(updates.finish()?)?.select(vec![
            col("id").alias("id_new"),
            col("position").alias("position_new"),
        ])?;

        let df_current = ctx.ctx().read_batch(self.population.clone())?;
        let joined = df_current
            .join(df_updates, JoinType::Left, &["id"], &["id_new"], None)?
            .select(vec![
                col("id"),
                col("role"),
                col("status"),
                col("properties"),
                coalesce(vec![col("position_new"), col("position")]).alias("position"),
                col("state"),
            ])?
            .collect()
            .await?;

        let population = concat_batches(joined[0].schema_ref(), &joined)?;
        let positions = population
            .column_by_name("position")
            .ok_or_else(|| Error::invalid_data("Missing 'position' column"))?
            .as_struct();
        let point_type = PointType::new(Dimension::XY, Default::default());
        let positions: PointArray = (positions, point_type).try_into()?;

        self.population = population;
        self.positions = positions;

        Ok(events)
    }
}

fn filter_by_cell(df: DataFrame, cell: CellIndex) -> Result<DataFrame> {
    let resolution = match cell.resolution() {
        Resolution::Zero => lit(0_i8),
        Resolution::One => lit(1_i8),
        Resolution::Two => lit(2_i8),
        Resolution::Three => lit(3_i8),
        Resolution::Four => lit(4_i8),
        Resolution::Five => lit(5_i8),
        Resolution::Six => lit(6_i8),
        Resolution::Seven => lit(7_i8),
        Resolution::Eight => lit(8_i8),
        Resolution::Nine => lit(9_i8),
        Resolution::Ten => lit(10_i8),
        Resolution::Eleven => lit(11_i8),
        Resolution::Twelve => lit(12_i8),
        Resolution::Thirteen => lit(13_i8),
        Resolution::Fourteen => lit(14_i8),
        Resolution::Fifteen => lit(15_i8),
    };
    Ok(df.filter(
        f::h3_longlatash3()
            .call(vec![
                col("position").field("x"),
                col("position").field("y"),
                resolution,
            ])
            .eq(lit(u64::from(cell) as i64)),
    )?)
}

struct PositionUpdateBuilder {
    id: FixedSizeBinaryBuilder,
    position: PointBuilder,
}

impl PositionUpdateBuilder {
    fn new() -> Self {
        Self {
            id: FixedSizeBinaryBuilder::new(16),
            position: PointBuilder::new(PointType::new(Dimension::XY, Default::default())),
        }
    }

    fn add_update(&mut self, id: &PersonId, position: &Point) -> Result<()> {
        self.id.append_value(id)?;
        self.position.push_point(Some(position));
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                POPULATION_SCHEMA.field_with_name("id")?.clone(),
                POPULATION_SCHEMA.field_with_name("position")?.clone(),
            ])),
            vec![
                Arc::new(self.id.finish()),
                self.position.finish().into_arrow(),
            ],
        )?)
    }
}

struct StatusUpdateBuilder {
    id: FixedSizeBinaryBuilder,
    status: StringBuilder,
    state: StringViewBuilder,
}

impl StatusUpdateBuilder {
    fn new() -> Self {
        Self {
            id: FixedSizeBinaryBuilder::new(16),
            status: StringBuilder::new(),
            state: StringViewBuilder::new(),
        }
    }

    fn add_update(&mut self, id: &[u8], status: &PersonStatus) -> Result<()> {
        self.id.append_value(id)?;
        self.status.append_value(status.flag().as_ref());
        self.state.append_value(serde_json::to_string(status)?);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        let status: DictionaryArray<Int8Type> = self.status.finish().into_iter().collect();
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                POPULATION_SCHEMA.field_with_name("id")?.clone(),
                POPULATION_SCHEMA.field_with_name("status")?.clone(),
                POPULATION_SCHEMA.field_with_name("state")?.clone(),
            ])),
            vec![
                Arc::new(self.id.finish()),
                Arc::new(status),
                Arc::new(self.state.finish()),
            ],
        )?)
    }
}

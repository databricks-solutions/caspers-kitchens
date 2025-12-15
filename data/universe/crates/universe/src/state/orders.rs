use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::types::Float64Type;
use arrow::array::{RecordBatch, StringArray, cast::AsArray as _};
use arrow::compute::{concat_batches, partition};
use h3o::LatLng;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

use crate::builders::{ORDER_LINE_SCHEMA, ORDER_SCHEMA};
use crate::context::SimulationContext;
use crate::error::{Error, Result};
use crate::idents::{OrderId, OrderLineId, SiteId};

pub static ORDER_SITE_ID_IDX: usize = 1;
pub static ORDER_CUSTOMER_ID_IDX: usize = 2;
pub static ORDER_DESTINATION_IDX: usize = 3;
pub static ORDER_STATUS_IDX: usize = 4;

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, EnumString, Display, AsRefStr, Serialize, Deserialize,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Customer submitted the order
    Submitted,
    /// Order is being processed
    Processing,
    /// Order is ready for pickup
    Ready,
    /// Order is picked up
    PickedUp,
    /// Order is delivered
    Delivered,
    /// Order is cancelled
    Cancelled,
    /// Order failed to be processed
    Failed,

    /// Catch-all for unknown statuses to avoid panics
    #[strum(default)]
    Unknown(String),
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, Display, AsRefStr, Serialize, Deserialize,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum OrderLineStatus {
    /// Order line is submitted
    Submitted,
    /// Order line is assigned to a kitchen
    Assigned,
    /// Order line is currently processing
    Processing,
    /// Order line is ready for pick up
    Ready,
    /// Order line is delivered
    Delivered,
    /// Order line is waiting
    Waiting,
}

pub struct OrderData {
    orders: RecordBatch,
    lines: RecordBatch,
    /// Track the index into orders data and corresponding slice of lines
    ///
    /// The slice is expressed as tuple (offset, length)
    index: IndexMap<OrderId, (usize, (usize, usize))>,
    lines_index: IndexSet<OrderLineId>,
}

impl OrderData {
    pub fn empty() -> Self {
        Self {
            orders: RecordBatch::new_empty(ORDER_SCHEMA.clone()),
            lines: RecordBatch::new_empty(ORDER_LINE_SCHEMA.clone()),
            index: IndexMap::new(),
            lines_index: IndexSet::new(),
        }
    }

    pub(crate) async fn try_new(ctx: &SimulationContext) -> Result<Self> {
        let orders = ctx.snapshots().orders().await?.collect().await?;
        if orders.is_empty() {
            return Ok(Self::empty());
        }
        let orders = concat_batches(orders[0].schema_ref(), &orders)?;
        let lines = ctx.snapshots().order_lines().await?.collect().await?;
        let lines = concat_batches(lines[0].schema_ref(), &lines)?;
        Self::try_new_from_data(orders, lines)
    }

    pub(crate) fn try_new_from_data(orders: RecordBatch, lines: RecordBatch) -> Result<Self> {
        if orders.schema().as_ref() != ORDER_SCHEMA.as_ref() {
            return Err(Error::invalid_data("expected orders to have schema"));
        }
        if lines.schema().as_ref() != ORDER_LINE_SCHEMA.as_ref() {
            return Err(Error::invalid_data("expected lines to have schema"));
        }
        if orders.num_rows() == 0 && lines.num_rows() > 0 {
            return Err(Error::invalid_data("non-empty lines for empty orders"));
        }

        if orders.num_rows() == 0 && lines.num_rows() == 0 {
            return Ok(Self::empty());
        }

        let Some((order_id_idx, _)) = lines.schema().column_with_name("order_id") else {
            return Err(Error::invalid_data("expected column 'order_id'"));
        };

        // partition order lines by their order ids
        let partitions = partition(&lines.columns()[order_id_idx..order_id_idx + 1])?;
        if partitions.len() != orders.num_rows() {
            return Err(Error::invalid_data(
                "expected all orders to have matching lines",
            ));
        }

        let order_id_col = orders.column_by_name("id").unwrap().as_fixed_size_binary();
        let index = partitions
            .ranges()
            .into_iter()
            .enumerate()
            .map(|(i, range)| {
                let order_id = order_id_col.value(i).try_into()?;
                Ok((order_id, (i, (range.start, (range.end - range.start)))))
            })
            .try_collect::<_, _, Error>()?;

        let lines_index: IndexSet<_> = lines
            .column_by_name("id")
            .unwrap()
            .as_fixed_size_binary()
            .iter()
            .filter_map(|raw| raw.map(TryInto::try_into))
            .try_collect()?;

        if lines_index.len() != lines.num_rows() {
            return Err(Error::invalid_data(
                "expected all lines to have matching ids",
            ));
        }

        Ok(Self {
            orders,
            lines,
            index,
            lines_index,
        })
    }

    pub(crate) fn batch_orders(&self) -> &RecordBatch {
        &self.orders
    }

    pub(crate) fn batch_lines(&self) -> &RecordBatch {
        &self.lines
    }

    pub(crate) fn order(&self, order_id: &OrderId) -> Option<OrderView<'_>> {
        self.index
            .get_key_value(order_id)
            .map(|(id, (idx, _))| OrderView::new(id, self, *idx))
    }

    pub(crate) fn order_line(&self, order_line_id: &OrderLineId) -> Option<OrderLineView<'_>> {
        self.lines_index
            .contains(order_line_id)
            .then(|| OrderLineView::new(*order_line_id, self))
    }

    pub(crate) fn all_orders(&self) -> impl Iterator<Item = OrderView<'_>> {
        self.index
            .iter()
            .map(|(id, (idx, _))| OrderView::new(id, self, *idx))
    }

    pub(crate) fn orders(&self, site_id: &SiteId) -> impl Iterator<Item = OrderView<'_>> {
        self.index.iter().filter_map(|(id, (idx, _))| {
            let view = OrderView::new(id, self, *idx);
            (view.site_id() == AsRef::<[u8]>::as_ref(site_id)).then_some(view)
        })
    }

    pub(crate) fn orders_with_status(
        &self,
        site_id: &SiteId,
        status: &OrderStatus,
    ) -> impl Iterator<Item = OrderView<'_>> {
        self.orders(site_id)
            .filter(|order| order.status() == status.as_ref())
    }

    pub(crate) fn merge(&self, other: Self) -> Result<Self> {
        let orders = concat_batches(&ORDER_SCHEMA, &[self.orders.clone(), other.orders])?;
        let lines = concat_batches(&ORDER_LINE_SCHEMA, &[self.lines.clone(), other.lines])?;
        Self::try_new_from_data(orders, lines)
    }

    /// Update the status of order lines.
    ///
    /// This will update the status of the order lines and recompute the order status
    /// based on the aggregate status of the order lines.
    pub(crate) fn update_order_lines<'a>(
        &mut self,
        updates: impl IntoIterator<Item = (OrderLineId, &'a OrderLineStatus)>,
    ) -> Result<()> {
        let mut current = self
            .lines
            .column_by_name("status")
            .unwrap()
            .as_string::<i32>()
            .iter()
            .filter_map(|s| s.map(|s| s.to_string()))
            .collect_vec();
        if current.len() != self.lines.num_rows() {
            return Err(Error::invalid_data("order line status mismatch"));
        }
        for (id, status) in updates {
            let Some(idx) = self.lines_index.get_index_of(&id) else {
                return Err(Error::invalid_data("order line not found"));
            };
            current[idx] = status.to_string();
        }
        // TODO: we assume the status column is always the last column in the schema.
        let new_array = Arc::new(StringArray::from(current));
        let mut arrays = self
            .lines
            .columns()
            .iter()
            .take(self.lines.num_columns() - 1)
            .cloned()
            .collect_vec();
        arrays.push(new_array);
        self.lines = RecordBatch::try_new(ORDER_LINE_SCHEMA.clone(), arrays)?;

        let statuses = self
            .all_orders()
            .map(|order| order.compute_status().to_string());
        let status_arr = Arc::new(StringArray::from(statuses.collect_vec()));
        let mut arrays = self
            .orders
            .columns()
            .iter()
            .take(self.orders.num_columns() - 1)
            .cloned()
            .collect_vec();
        arrays.push(status_arr);
        self.orders = RecordBatch::try_new(ORDER_SCHEMA.clone(), arrays)?;

        Ok(())
    }

    /// Update the status of orders.
    pub(crate) fn update_orders<'a>(
        &mut self,
        updates: impl IntoIterator<Item = (OrderId, &'a OrderStatus)>,
    ) -> Result<()> {
        let update_map: HashMap<OrderId, &OrderStatus> = updates.into_iter().collect();
        let mut statuses = Vec::with_capacity(self.orders.num_rows());
        for order in self.all_orders() {
            if let Some(status) = update_map.get(order.id()) {
                statuses.push(status.to_string());
            } else {
                statuses.push(order.status().to_string());
            }
        }
        let status_arr = Arc::new(StringArray::from(statuses));
        let mut arrays = self
            .orders
            .columns()
            .iter()
            .take(self.orders.num_columns() - 1)
            .cloned()
            .collect_vec();
        arrays.push(status_arr);
        self.orders = RecordBatch::try_new(ORDER_SCHEMA.clone(), arrays)?;
        Ok(())
    }
}

pub struct OrderView<'a> {
    order_id: &'a OrderId,
    data: &'a OrderData,
    valid_index: usize,
}

impl<'a> OrderView<'a> {
    fn new(order_id: &'a OrderId, data: &'a OrderData, valid_index: usize) -> Self {
        Self {
            order_id,
            data,
            valid_index,
        }
    }

    pub fn id(&self) -> &OrderId {
        self.order_id
    }

    pub(crate) fn site_id(&self) -> &[u8] {
        self.data
            .orders
            .column(ORDER_SITE_ID_IDX)
            .as_fixed_size_binary()
            .value(self.valid_index)
    }

    pub(crate) fn customer_person_id(&self) -> &[u8] {
        self.data
            .orders
            .column(ORDER_CUSTOMER_ID_IDX)
            .as_fixed_size_binary()
            .value(self.valid_index)
    }

    pub fn status(&self) -> &str {
        self.data
            .orders
            .column(ORDER_STATUS_IDX)
            .as_string::<i32>()
            .value(self.valid_index)
    }

    fn compute_status(&self) -> OrderStatus {
        let status = self
            .status()
            .parse()
            .unwrap_or(OrderStatus::Unknown(self.status().to_string()));
        match status {
            OrderStatus::Submitted => {
                if self.is_processing() {
                    OrderStatus::Processing
                } else {
                    status
                }
            }
            OrderStatus::Processing => {
                if self.is_ready() {
                    OrderStatus::Ready
                } else {
                    status
                }
            }
            _ => status,
        }
    }

    pub(crate) fn lines(&self) -> impl Iterator<Item = OrderLineView<'_>> {
        let (_order_idx, (offset, len)) = self.data.index.get(self.order_id).unwrap();
        self.data
            .lines_index
            .iter()
            .skip(*offset)
            .take(*len)
            .map(|line_id| OrderLineView::new(*line_id, self.data))
    }

    pub(crate) fn is_processing(&self) -> bool {
        self.lines()
            .any(|line| line.status() == OrderLineStatus::Processing.as_ref())
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.lines()
            .all(|line| line.status() == OrderLineStatus::Ready.as_ref())
    }

    pub(crate) fn destination(&self) -> Result<LatLng> {
        let (order_idx, _) = self.data.index.get(self.order_id).unwrap();
        let pos = self
            .data
            .orders
            .column(ORDER_DESTINATION_IDX)
            .as_fixed_size_list()
            .value(*order_idx);
        let vals = pos.as_primitive::<Float64Type>();
        Ok(LatLng::new(vals.value(0), vals.value(1))?)
    }
}

pub struct OrderLineView<'a> {
    line_id: OrderLineId,
    data: &'a OrderData,
}

impl<'a> OrderLineView<'a> {
    fn new(line_id: OrderLineId, data: &'a OrderData) -> Self {
        Self { line_id, data }
    }

    pub fn id(&self) -> &OrderLineId {
        &self.line_id
    }

    pub fn order_id(&self) -> &[u8] {
        let line_id = self.data.lines_index.get_index_of(&self.line_id).unwrap();
        get_id(&self.data.lines, "order_id", line_id)
    }

    pub fn brand_id(&self) -> &[u8] {
        let line_id = self.data.lines_index.get_index_of(&self.line_id).unwrap();
        get_id(&self.data.lines, "brand_id", line_id)
    }

    pub fn menu_item_id(&self) -> &[u8] {
        let line_id = self.data.lines_index.get_index_of(&self.line_id).unwrap();
        get_id(&self.data.lines, "menu_item_id", line_id)
    }

    pub fn status(&self) -> &str {
        let line_id = self.data.lines_index.get_index_of(&self.line_id).unwrap();
        self.data
            .lines
            .column_by_name("status")
            .unwrap()
            .as_string::<i32>()
            .value(line_id)
    }
}

fn get_id<'a>(batch: &'a RecordBatch, name: &str, idx: usize) -> &'a [u8] {
    batch
        .column_by_name(name)
        .unwrap()
        .as_fixed_size_binary()
        .value(idx)
}

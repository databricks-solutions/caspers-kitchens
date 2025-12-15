use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion::{
    prelude::{DataFrame, Expr, cast, col, concat, lit, named_struct},
    scalar::ScalarValue,
};

use crate::{
    OrderLineStatus, Result, SimulationContext,
    functions::{uuid_to_string, uuidv7},
};

/// Macro to generate FIELD, EXPR, and NULL statics for event fields
macro_rules! event_field {
    (
        $field_name:ident {
            name: $name:expr,
            fields: {
                $($field:ident: $field_type:expr),* $(,)?
            }
        }
    ) => {
        paste::paste! {
            static [<$field_name:upper _FIELD>]: LazyLock<FieldRef> = LazyLock::new(|| {
                FieldRef::new(Field::new(
                    $name,
                    DataType::Struct(
                        vec![
                            $(
                                Field::new(stringify!($field), $field_type, false),
                            )*
                        ]
                        .into(),
                    ),
                    true,
                ))
            });

            static [<$field_name:upper _EXPR>]: LazyLock<Expr> = LazyLock::new(|| {
                named_struct(vec![
                    $(
                        lit(stringify!($field)),
                        col(stringify!($field)),
                    )*
                ])
                .alias($name)
            });

            static [<$field_name:upper _NULL>]: LazyLock<Expr> = LazyLock::new(|| {
                let data_type = match [<$field_name:upper _FIELD>].data_type() {
                    DataType::Struct(fields) => fields.clone(),
                    _ => unreachable!(),
                };
                cast(lit(ScalarValue::Null), DataType::Struct(data_type)).alias($name)
            });
        }
    };
}

// Helper macros for common types
macro_rules! timestamp_field {
    () => {
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
    };
}

macro_rules! uuid_field {
    () => {
        DataType::FixedSizeBinary(16)
    };
}

pub(crate) struct EventsHelper {}

impl EventsHelper {
    pub(crate) fn empty(ctx: &SimulationContext) -> Result<DataFrame> {
        let empty_events = RecordBatch::new_empty(EVENT_SCHEMA.clone());
        Ok(ctx.ctx().read_batch(empty_events)?)
    }

    /// Build data struct with only one field populated, rest null
    fn build_data_struct(active_field: &str, custom_expr: Option<Expr>) -> Expr {
        let mut fields = vec![];

        for info in EVENT_FIELDS.iter() {
            fields.push(lit(info.name));

            let expr = if info.name == active_field {
                custom_expr
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| (info.expr)().clone())
            } else {
                (info.null)().clone()
            };

            fields.push(expr);
        }

        named_struct(fields).alias("data")
    }

    /// Generic event builder - all events use this
    pub(crate) fn build_event(
        df: DataFrame,
        event_type: SimulationEvent,
        source_prefix: &str,
        source_id_col: &str,
        timestamp_col: &str,
        active_field: &str,
        custom_expr: Option<Expr>,
    ) -> Result<DataFrame> {
        Ok(df.select(vec![
            uuidv7().call(vec![col(timestamp_col)]).alias("id"),
            concat(vec![
                lit(source_prefix),
                uuid_to_string().call(vec![col(source_id_col)]),
            ])
            .alias("source"),
            lit("1.0").alias("specversion"),
            event_type.event_type_lit(),
            cast(col(timestamp_col), DataType::LargeUtf8).alias("time"),
            Self::build_data_struct(active_field, custom_expr),
        ])?)
    }

    pub(crate) fn order_created(orders: DataFrame) -> Result<DataFrame> {
        Self::build_event(
            orders,
            SimulationEvent::OrderCreated,
            "/population/",
            "person_id",
            "submitted_at",
            "order_created",
            None,
        )
    }

    pub(crate) fn order_ready(orders: DataFrame) -> Result<DataFrame> {
        Self::build_event(
            orders,
            SimulationEvent::OrderReady,
            "/sites/",
            "site_id",
            "timestamp",
            "order_ready",
            None,
        )
    }

    pub(crate) fn order_picked_up(orders: DataFrame) -> Result<DataFrame> {
        Self::build_event(
            orders,
            SimulationEvent::OrderPickedUp,
            "/sites/",
            "site_id",
            "timestamp",
            "order_picked_up",
            None,
        )
    }

    pub(crate) fn step_started(order_lines: DataFrame) -> Result<DataFrame> {
        Self::build_event(
            order_lines,
            SimulationEvent::OrderLineStepStarted,
            "/stations/",
            "station_id",
            "timestamp",
            "step_started",
            None,
        )
    }

    pub(crate) fn step_finished(order_lines: DataFrame) -> Result<DataFrame> {
        Self::build_event(
            order_lines,
            SimulationEvent::OrderLineStepFinished,
            "/stations/",
            "station_id",
            "timestamp",
            "step_finished",
            None,
        )
    }

    pub(crate) fn order_line_ready(order_lines: DataFrame) -> Result<DataFrame> {
        Self::build_event(
            order_lines,
            SimulationEvent::OrderLineUpdated,
            "/kitchen/",
            "kitchen_id",
            "timestamp",
            "order_line_updated",
            Some(ORDER_LINE_UPDATED_COMPLETED_EXPR.clone()),
        )
    }
}

pub enum SimulationEvent {
    OrderCreated,
    OrderReady,
    OrderPickedUp,
    OrderDelivered,
    OrderLineStepStarted,
    OrderLineStepFinished,
    OrderLineUpdated,
    SiteCheckIn,
    SiteCheckOut,
}

impl SimulationEvent {
    pub fn event_type(&self) -> &'static str {
        use SimulationEvent::*;
        match self {
            OrderCreated => "caspers.universe.order_created",
            OrderReady => "caspers.universe.order_ready",
            OrderPickedUp => "caspers.universe.order_picked_up",
            OrderDelivered => "caspers.universe.order_delivered",
            OrderLineStepStarted => "caspers.universe.order_line_step_started",
            OrderLineStepFinished => "caspers.universe.order_line_step_finished",
            OrderLineUpdated => "caspers.universe.order_line_updated",
            SiteCheckIn => "caspers.universe.site_check_in",
            SiteCheckOut => "caspers.universe.site_check_out",
        }
    }

    fn event_type_lit(&self) -> Expr {
        lit(self.event_type()).alias("type")
    }
}

event_field! {
    OrderCreated {
        name: "order_created",
        fields: {
            order_id: uuid_field!(),
            submitted_at: timestamp_field!(),
            destination: DataType::Struct(
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]
                .into(),
            ),
            items: DataType::List(Arc::new(Field::new(
                "item",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", uuid_field!(), false)),
                    2,
                ),
                false,
            ))),
        }
    }
}

event_field! {
    OrderReady {
        name: "order_ready",
        fields: {
            order_id: uuid_field!(),
            timestamp: timestamp_field!(),
        }
    }
}

event_field! {
    OrderPickedUp {
        name: "order_picked_up",
        fields: {
            order_id: uuid_field!(),
            courier_id: uuid_field!(),
            timestamp: timestamp_field!(),
        }
    }
}

event_field! {
    OrderLineStepStarted {
        name: "step_started",
        fields: {
            timestamp: timestamp_field!(),
            order_line_id: uuid_field!(),
            step_index: DataType::Int32,
        }
    }
}

event_field! {
    OrderLineStepFinished {
        name: "step_finished",
        fields: {
            timestamp: timestamp_field!(),
            order_line_id: uuid_field!(),
            step_index: DataType::Int32,
        }
    }
}

event_field! {
    OrderLineUpdated {
        name: "order_line_updated",
        fields: {
            timestamp: timestamp_field!(),
            order_line_id: uuid_field!(),
            status: DataType::Utf8,
        }
    }
}

static ORDER_LINE_UPDATED_COMPLETED_EXPR: LazyLock<Expr> = LazyLock::new(|| {
    named_struct(vec![
        lit("timestamp"),
        col("timestamp"),
        lit("order_line_id"),
        col("order_line_id"),
        lit("status"),
        lit(OrderLineStatus::Ready.as_ref()),
    ])
});

event_field! {
    SiteCheckIn {
        name: "check_in",
        fields: {
            timestamp: timestamp_field!(),
            site_id: uuid_field!(),
        }
    }
}

event_field! {
    SiteCheckOut {
        name: "check_out",
        fields: {
            timestamp: timestamp_field!(),
            orders: DataType::List(Arc::new(Field::new(
                "item",
                uuid_field!(),
                false,
            ))),
        }
    }
}

// Event registry for iteration
struct EventFieldInfo {
    name: &'static str,
    field: fn() -> &'static FieldRef,
    expr: fn() -> &'static Expr,
    null: fn() -> &'static Expr,
}

static EVENT_FIELDS: LazyLock<Vec<EventFieldInfo>> = LazyLock::new(|| {
    vec![
        EventFieldInfo {
            name: "order_created",
            field: || &ORDERCREATED_FIELD,
            expr: || &ORDERCREATED_EXPR,
            null: || &ORDERCREATED_NULL,
        },
        EventFieldInfo {
            name: "order_ready",
            field: || &ORDERREADY_FIELD,
            expr: || &ORDERREADY_EXPR,
            null: || &ORDERREADY_NULL,
        },
        EventFieldInfo {
            name: "order_picked_up",
            field: || &ORDERPICKEDUP_FIELD,
            expr: || &ORDERPICKEDUP_EXPR,
            null: || &ORDERPICKEDUP_NULL,
        },
        EventFieldInfo {
            name: "order_line_updated",
            field: || &ORDERLINEUPDATED_FIELD,
            expr: || &ORDERLINEUPDATED_EXPR,
            null: || &ORDERLINEUPDATED_NULL,
        },
        EventFieldInfo {
            name: "step_started",
            field: || &ORDERLINESTEPSTARTED_FIELD,
            expr: || &ORDERLINESTEPSTARTED_EXPR,
            null: || &ORDERLINESTEPSTARTED_NULL,
        },
        EventFieldInfo {
            name: "step_finished",
            field: || &ORDERLINESTEPFINISHED_FIELD,
            expr: || &ORDERLINESTEPFINISHED_EXPR,
            null: || &ORDERLINESTEPFINISHED_NULL,
        },
        EventFieldInfo {
            name: "check_in",
            field: || &SITECHECKIN_FIELD,
            expr: || &SITECHECKIN_EXPR,
            null: || &SITECHECKIN_NULL,
        },
        EventFieldInfo {
            name: "check_out",
            field: || &SITECHECKOUT_FIELD,
            expr: || &SITECHECKOUT_EXPR,
            null: || &SITECHECKOUT_NULL,
        },
    ]
});

static EVENT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let data_fields: Vec<FieldRef> = EVENT_FIELDS
        .iter()
        .map(|info| (info.field)().clone())
        .collect();

    SchemaRef::new(Schema::new(vec![
        Field::new("id", uuid_field!(), false),
        Field::new("source", DataType::Utf8, false),
        Field::new("specversion", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("time", timestamp_field!(), false),
        Field::new("data", DataType::Struct(data_fields.into()), false),
    ]))
});

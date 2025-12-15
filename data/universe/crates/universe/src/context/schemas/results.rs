use std::sync::LazyLock;

use datafusion::prelude::DataFrame;
use datafusion::sql::TableReference;

use crate::Result;

use crate::context::SimulationContext;

pub(in crate::context) static RESULTS_SCHEMA_NAME: &str = "results";
pub(in crate::context) static METRICS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", RESULTS_SCHEMA_NAME, "metrics"));
pub(in crate::context) static EVENTS_REF: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::full("caspers", RESULTS_SCHEMA_NAME, "events"));

pub struct ResultsSchema<'a> {
    ctx: &'a SimulationContext,
}

impl<'a> ResultsSchema<'a> {
    pub(in crate::context) fn new(ctx: &'a SimulationContext) -> Self {
        Self { ctx }
    }

    pub async fn metrics(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str; 4] = &["timestamp", "source", "label", "value"];
        Ok(self
            .ctx
            .scan_scoped(&METRICS_REF)
            .await?
            .select_columns(COLUMNS)?)
    }

    pub async fn write_metrics(&self, data: DataFrame) -> Result<()> {
        self.ctx
            .extend_df(data)?
            .write_table(METRICS_REF.to_string().as_str(), Default::default())
            .await?;
        Ok(())
    }

    pub async fn events(&self) -> Result<DataFrame> {
        static COLUMNS: &[&str; 7] = &[
            "id",
            "source",
            "specversion",
            "type",
            "datacontenttype",
            "time",
            "data",
        ];
        Ok(self
            .ctx
            .scan_scoped(&EVENTS_REF)
            .await?
            .select_columns(COLUMNS)?)
    }

    pub async fn write_events(&self, data: DataFrame) -> Result<()> {
        self.ctx
            .extend_df(data)?
            .write_table(EVENTS_REF.to_string().as_str(), Default::default())
            .await?;
        Ok(())
    }
}

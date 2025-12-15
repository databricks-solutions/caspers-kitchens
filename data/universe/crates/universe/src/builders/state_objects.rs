use std::sync::{Arc, LazyLock};

use arrow::array::builder::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
use arrow::array::{LargeStringBuilder, RecordBatch, StringViewBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_schema::extension::{Json as JsonExtension, Uuid as UuidExtension};

use crate::Error;
use crate::error::Result;
use crate::idents::{BrandId, KitchenId, MenuItemId, SiteId, StationId};
use crate::models::{Brand, SiteSetup};
use crate::state::ObjectLabel;

pub(crate) static OBJECTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false).with_extension_type(UuidExtension),
        Field::new("parent_id", DataType::FixedSizeBinary(16), true)
            .with_extension_type(UuidExtension),
        Field::new("label", DataType::Utf8View, false),
        Field::new(
            "name",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            false,
        ),
        Field::new("properties", DataType::LargeUtf8, true)
            .with_extension_type(JsonExtension::default()),
    ]))
});

pub struct ObjectDataBuilder {
    id: FixedSizeBinaryBuilder,
    parent_id: FixedSizeBinaryBuilder,
    name: ListBuilder<StringBuilder>,
    label: StringViewBuilder,
    properties: LargeStringBuilder,
}

impl Default for ObjectDataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectDataBuilder {
    pub fn new() -> Self {
        Self {
            id: FixedSizeBinaryBuilder::new(16),
            parent_id: FixedSizeBinaryBuilder::new(16),
            name: ListBuilder::new(StringBuilder::new()),
            label: StringViewBuilder::new(),
            properties: LargeStringBuilder::new(),
        }
    }

    pub fn append_brand(&mut self, brand_id: &BrandId, brand: &Brand) {
        self.id.append_value(brand_id).unwrap();
        self.parent_id.append_null();
        self.label.append_value(ObjectLabel::Brand);
        self.name.append_value([Some("brands"), Some(&brand.name)]);
        self.properties.append_null();

        for item in &brand.items {
            let item_name = format!("brands/{}/items/{}", brand.name, item.name);
            let item_id = MenuItemId::from_uri_ref(&item_name);
            self.id.append_value(item_id).unwrap();
            self.parent_id.append_value(brand_id).unwrap();
            self.label.append_value(ObjectLabel::MenuItem);
            self.name.append_value([
                Some("brands"),
                Some(&brand.name),
                Some("items"),
                Some(&item.name),
            ]);
            self.properties
                .append_value(serde_json::to_string(&item).unwrap());
        }
    }

    pub fn append_site_info(&mut self, site: &SiteSetup) -> Result<()> {
        let site_info = site
            .info
            .as_ref()
            .ok_or(Error::invalid_data("expected site info object"))?;
        let site_id: SiteId = uuid::Uuid::parse_str(&site_info.id)?.into();

        self.id.append_value(site_id)?;
        self.parent_id.append_null();
        self.label.append_value(ObjectLabel::Site);
        self.name
            .append_value([Some("sites"), Some(&site_info.name)]);
        self.properties
            .append_value(serde_json::to_string(site_info)?);

        for kitchen in &site.kitchens {
            let kitchen_info = kitchen
                .info
                .as_ref()
                .ok_or(Error::invalid_data("expected kitchen info object"))?;

            let kitchen_id: KitchenId = uuid::Uuid::parse_str(&kitchen_info.id)?.into();
            self.id.append_value(kitchen_id)?;
            self.parent_id.append_value(site_id)?;
            self.label.append_value(ObjectLabel::Kitchen);
            self.name.append_value([
                Some("sites"),
                Some(&site_info.name),
                Some("kitchens"),
                Some(&kitchen_info.name),
            ]);
            self.properties.append_null();

            for station in &kitchen.stations {
                let station_id: StationId = uuid::Uuid::parse_str(&station.id)?.into();
                self.id.append_value(station_id).unwrap();
                self.parent_id.append_value(kitchen_id).unwrap();
                self.label.append_value(ObjectLabel::Station);
                self.name.append_value([
                    Some("sites"),
                    Some(&site_info.name),
                    Some("kitchens"),
                    Some(&kitchen_info.name),
                    Some("stations"),
                    Some(&station.name),
                ]);
                self.properties
                    .append_value(serde_json::to_string(station).unwrap());
            }
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let id = Arc::new(self.id.finish());
        let parent_id = Arc::new(self.parent_id.finish());
        let label = Arc::new(self.label.finish());
        let name = Arc::new(self.name.finish());
        let properties = Arc::new(self.properties.finish());

        Ok(RecordBatch::try_new(
            OBJECTS_SCHEMA.clone(),
            vec![id, parent_id, label, name, properties],
        )?)
    }
}

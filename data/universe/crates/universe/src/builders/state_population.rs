use std::sync::{Arc, LazyLock};

use arrow::array::builder::{FixedSizeBinaryBuilder, StringBuilder};
use arrow::array::{ArrayRef, DictionaryArray, RecordBatch, StringViewBuilder, StructArray};
use arrow::datatypes::{DataType, Field, Int8Type, Schema, SchemaRef};
use arrow_schema::extension::Uuid;
use fake::Fake;
use geo::{BoundingRect, Centroid, Contains, Point};
use geoarrow::array::PointBuilder;
use geoarrow_array::IntoArrow;
use geoarrow_schema::{Dimension, PointType};
use h3o::{LatLng, Resolution, geom::SolventBuilder};
use rand::distr::{Distribution, Uniform};
use rand::rngs::ThreadRng;

use crate::idents::PersonId;
use crate::state::PersonState;
use crate::{Error, Result};
use crate::{PersonRole, PersonStatusFlag};

static DEFAULT_STATE: LazyLock<String> =
    LazyLock::new(|| serde_json::to_string(&PersonState::default()).unwrap());

static POPULATION_PROPERTIES_FIELD: LazyLock<Field> = LazyLock::new(|| {
    Field::new(
        "properties",
        DataType::Struct(
            vec![
                Field::new("first_name", DataType::Utf8View, false),
                Field::new("last_name", DataType::Utf8View, false),
                Field::new("email", DataType::Utf8View, false),
                Field::new("cc_number", DataType::Utf8View, true),
            ]
            .into(),
        ),
        false,
    )
});

struct PropertiesBuilder {
    first_names: StringViewBuilder,
    last_names: StringViewBuilder,
    emails: StringViewBuilder,
    cc_numbers: StringViewBuilder,

    rng: ThreadRng,
}

impl Default for PropertiesBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PropertiesBuilder {
    pub fn new() -> Self {
        Self {
            first_names: StringViewBuilder::new(),
            last_names: StringViewBuilder::new(),
            emails: StringViewBuilder::new(),
            cc_numbers: StringViewBuilder::new(),
            rng: rand::rng(),
        }
    }

    fn add_entry(&mut self) {
        let gen_first_name = fake::faker::name::en::FirstName();
        let gen_last_name = fake::faker::name::en::LastName();
        let gen_email = fake::faker::internet::en::SafeEmail();
        let gen_cc = fake::faker::creditcard::en::CreditCardNumber();

        self.first_names
            .append_value(gen_first_name.fake_with_rng::<String, _>(&mut self.rng));
        self.last_names
            .append_value(gen_last_name.fake_with_rng::<String, _>(&mut self.rng));
        self.emails
            .append_value(gen_email.fake_with_rng::<String, _>(&mut self.rng));
        self.cc_numbers
            .append_value(gen_cc.fake_with_rng::<String, _>(&mut self.rng));
    }

    fn finish(&mut self) -> ArrayRef {
        let fields = match POPULATION_PROPERTIES_FIELD.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => panic!("Invalid data type for population properties"),
        };
        Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(self.first_names.finish()),
                Arc::new(self.last_names.finish()),
                Arc::new(self.emails.finish()),
                Arc::new(self.cc_numbers.finish()),
            ],
            None,
        ))
    }
}

pub(crate) static POPULATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(16), false).with_extension_type(Uuid),
        Field::new(
            "role",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
        POPULATION_PROPERTIES_FIELD.clone(),
        Field::new(
            "position",
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        )
        .with_extension_type(PointType::new(Dimension::XY, Default::default())),
        Field::new("state", DataType::Utf8View, false),
    ]))
});

pub struct PopulationDataBuilder {
    id: FixedSizeBinaryBuilder,
    role: StringBuilder,
    status: StringBuilder,
    properties: PropertiesBuilder,
    position: PointBuilder,
    state: StringViewBuilder,
}

impl Default for PopulationDataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PopulationDataBuilder {
    pub fn new() -> Self {
        Self {
            id: FixedSizeBinaryBuilder::new(16),
            role: StringBuilder::new(),
            status: StringBuilder::new(),
            properties: PropertiesBuilder::new(),
            position: PointBuilder::new(PointType::new(Dimension::XY, Default::default())),
            state: StringViewBuilder::new(),
        }
    }

    pub fn add_site(&mut self, n_people: usize, latitude: f64, longitude: f64) -> Result<()> {
        for _ in 0..n_people {
            let id = PersonId::new();
            self.id.append_value(id)?;
            self.properties.add_entry();
            self.role.append_value(PersonRole::Customer.as_ref());
            self.status.append_value(PersonStatusFlag::Idle.as_ref());
            self.state.append_value(DEFAULT_STATE.as_str());
        }

        let latlng = LatLng::new(latitude, longitude)?;
        // TODO: do not use simply hardcoded resolution and grid disk size..
        let cell_index = latlng.to_cell(Resolution::Nine);
        let cells = cell_index.grid_disk::<Vec<_>>(8);
        let solvent = SolventBuilder::new().build();
        let geom = solvent.dissolve(cells)?;

        let bounding_rect = geom
            .bounding_rect()
            .ok_or(Error::internal("failed to get bounding rect"))?;
        let (maxx, maxy) = bounding_rect.max().x_y();
        let (minx, miny) = bounding_rect.min().x_y();

        let x_range = Uniform::new(minx, maxx)?;
        let y_range = Uniform::new(miny, maxy)?;
        x_range
            .sample_iter(rand::rng())
            .zip(y_range.sample_iter(rand::rng()))
            .filter_map(|(x, y)| {
                let p = Point::new(x, y);
                geom.contains(&p).then_some(p)
            })
            .take(n_people)
            .for_each(|p| {
                self.position.push_point(Some(&p));
            });

        let n_couriers = n_people / 10;

        let loc = geom
            .centroid()
            .ok_or(Error::internal("failed to get centroid"))?;
        for _ in 0..n_couriers {
            let id = PersonId::new();
            self.id.append_value(id)?;
            self.properties.add_entry();
            self.role.append_value(PersonRole::Courier.as_ref());
            self.status.append_value(PersonStatusFlag::Idle.as_ref());
            self.position.push_point(Some(&loc));
            self.state.append_value(DEFAULT_STATE.as_str());
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let role: DictionaryArray<Int8Type> = self.role.finish().into_iter().collect();
        let status: DictionaryArray<Int8Type> = self.status.finish().into_iter().collect();

        Ok(RecordBatch::try_new(
            POPULATION_SCHEMA.clone(),
            vec![
                Arc::new(self.id.finish()),
                Arc::new(role),
                Arc::new(status),
                self.properties.finish(),
                self.position.finish().into_arrow(),
                Arc::new(self.state.finish()),
            ],
        )?)
    }
}

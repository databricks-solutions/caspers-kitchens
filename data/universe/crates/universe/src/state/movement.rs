use std::collections::HashMap;
use std::sync::LazyLock;

use arrow::array::cast::AsArray as _;
use arrow::array::{RecordBatch, types::Float64Type};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_schema::extension::Uuid as UuidExtension;
use datafusion::common::SchemaExt;
use fast_paths::{FastGraph, InputGraph, PathCalculator};
use geo::Point;
use geo_traits::PointTrait;
use geo_traits::to_geo::{ToGeoCoord, ToGeoLineString};
use geoarrow::array::{LineStringArray, PointArray};
use geoarrow_array::GeoArrowArrayAccessor as _;
use geoarrow_array::scalar::{LineString as ArrowLineString, Point as ArrowPoint};
use geoarrow_schema::{Dimension, LineStringType, PointType};
use h3o::{CellIndex, LatLng, Resolution};
use indexmap::IndexSet;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use uuid::Uuid;

use crate::Result;

#[path = "movement_next.rs"]
pub(crate) mod next;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default, AsRefStr)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Transport {
    Foot,
    #[default]
    Bicycle,
    Car,
    Bus,
    Train,
    Plane,
    Ship,
}

impl Transport {
    /// Returns the default velocity of the transport in km/h.
    pub fn default_velocity_km_h(&self) -> f64 {
        match self {
            Transport::Foot => 5.0,
            Transport::Bicycle => 15.0,
            Transport::Car => 60.0,
            Transport::Bus => 30.0,
            Transport::Train => 100.0,
            Transport::Plane => 800.0,
            Transport::Ship => 20.0,
        }
    }

    pub fn default_velocity_m_s(&self) -> f64 {
        self.default_velocity_km_h() / 3.6
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JourneyLeg {
    pub destination: Point,
    pub distance_m: usize,
}

impl<T: Into<Point>> From<(T, usize)> for JourneyLeg {
    fn from(value: (T, usize)) -> Self {
        JourneyLeg {
            destination: value.0.into(),
            distance_m: value.1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Journey {
    transport: Transport,
    // The full journey to be completed
    legs: Vec<JourneyLeg>,
    // The current leg being traveled
    current_leg_index: usize,
    // Progress within the current leg (0.0 to 1.0)
    current_leg_progress: f64,
}

impl Journey {
    pub fn distance_m(&self) -> usize {
        self.legs.iter().map(|leg| leg.distance_m).sum()
    }

    pub fn is_done(&self) -> bool {
        self.current_leg_index >= self.legs.len()
    }

    pub fn reset_reverse(&mut self) {
        self.legs.reverse();
        self.current_leg_index = 0;
        self.current_leg_progress = 0.0;
    }

    pub fn advance(&mut self, time_step: std::time::Duration) -> Vec<Point> {
        if self.is_done() {
            return Vec::new();
        }

        // cumpute the total distance travelled during this step.
        let velocity_m_s = self.transport.default_velocity_m_s();
        let distance_m = velocity_m_s * time_step.as_secs_f64();
        let mut distance_remaining = distance_m;

        let mut traversed_points = Vec::new();
        while distance_remaining > 0. && !self.is_done() {
            let current_leg = &self.legs[self.current_leg_index];
            let leg_distance_remaining =
                current_leg.distance_m as f64 * (1.0 - self.current_leg_progress);

            if leg_distance_remaining <= distance_remaining {
                // We completed this leg
                traversed_points.push(current_leg.destination);
                distance_remaining -= leg_distance_remaining;
                self.current_leg_index += 1;
                self.current_leg_progress = 0.0;
            } else {
                // We didn't complete this leg, calculate the intermediate point
                let progress_ratio = distance_remaining / current_leg.distance_m as f64;
                self.current_leg_progress += progress_ratio;

                // If we have a previous point, interpolate between it and the destination
                if let Some(prev_point) = traversed_points.last() {
                    let dx = current_leg.destination.x() - prev_point.x();
                    let dy = current_leg.destination.y() - prev_point.y();

                    let intermediate_point = Point::new(
                        prev_point.x() + dx * progress_ratio,
                        prev_point.y() + dy * progress_ratio,
                    );

                    traversed_points.push(intermediate_point);
                } else {
                    // If there's no previous point, just add the destination
                    traversed_points.push(current_leg.destination);
                }

                break;
            }
        }

        traversed_points
    }

    // New method to get the full journey history
    pub fn full_journey(&self) -> &[JourneyLeg] {
        &self.legs
    }

    // New method to get the current progress
    pub fn progress(&self) -> (usize, f64) {
        (self.current_leg_index, self.current_leg_progress)
    }

    pub fn has_started(&self) -> bool {
        self.current_leg_progress > 1e-10 || self.current_leg_index > 0
    }

    /// Returns the total distance of the journey in meters
    pub(crate) fn total_distance_m(&self) -> usize {
        self.legs.iter().map(|leg| leg.distance_m).sum()
    }

    /// Returns the distance completed so far in meters
    pub(crate) fn distance_completed_m(&self) -> f64 {
        if self.is_done() {
            return self.total_distance_m() as f64;
        }

        let completed_legs_distance: usize = self.legs[..self.current_leg_index]
            .iter()
            .map(|leg| leg.distance_m)
            .sum();

        let current_leg_distance = if self.current_leg_index < self.legs.len() {
            self.legs[self.current_leg_index].distance_m as f64 * self.current_leg_progress
        } else {
            0.0
        };

        completed_legs_distance as f64 + current_leg_distance
    }

    /// Returns the distance remaining in meters
    pub(crate) fn distance_remaining_m(&self) -> f64 {
        self.total_distance_m() as f64 - self.distance_completed_m()
    }

    /// Returns the progress percentage of the entire journey (0.0 to 1.0)
    pub(crate) fn progress_percentage(&self) -> f64 {
        if self.total_distance_m() == 0 {
            return 1.0;
        }
        self.distance_completed_m() / self.total_distance_m() as f64
    }

    /// Returns the estimated time remaining in seconds based on the given transport
    pub(crate) fn estimated_time_remaining_s(&self) -> f64 {
        self.distance_remaining_m() / self.transport.default_velocity_m_s()
    }
}

impl<T: Into<JourneyLeg>> FromIterator<T> for Journey {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Journey {
            transport: Transport::default(),
            legs: iter.into_iter().map(Into::into).collect(),
            current_leg_index: 0,
            current_leg_progress: 0.0,
        }
    }
}

/// Auxiliary structure to handle journey planning and routing.
pub struct JourneyPlanner {
    routing: RoutingData,
    graph: FastGraph,
}

impl JourneyPlanner {
    fn new(routing: RoutingData) -> Self {
        let graph = routing.build_router();
        Self { routing, graph }
    }

    /// Get a path calculator for the routing graph.
    ///
    /// This calculator should be reused for repeated calls to the plan method.
    pub fn get_router(&self) -> PathCalculator {
        fast_paths::create_calculator(&self.graph)
    }

    /// For a given point, find a nearby node in the routing graph.
    ///
    /// This function will not try to find the nearest node, but will instead
    /// return the first node found in the vicinity.
    pub fn nearest_node(&self, point: &LatLng) -> Option<Uuid> {
        // TODO: try different resolutions, starting from lower ones.
        let cell = point.to_cell(Resolution::Ten);
        self.routing
            .nodes()
            .find(|node| node.is_in_cell(cell))
            .map(|node| *node.id())
    }

    pub fn plan(
        &self,
        router: &mut PathCalculator,
        origin: impl AsRef<Uuid>,
        destination: impl AsRef<Uuid>,
    ) -> Option<Journey> {
        let origin_id = self.routing.node_map.get_index_of(origin.as_ref())?;
        let destination_id = self.routing.node_map.get_index_of(destination.as_ref())?;
        let path = router.calc_path(&self.graph, origin_id, destination_id)?;
        Some(
            path.get_nodes()
                .iter()
                .tuple_windows()
                .flat_map(|(a, b)| {
                    let edge = self.routing.edge_map.get(&(*a, *b)).unwrap();
                    let edge = self.routing.edge(*edge);
                    let legs = edge
                        .geometry()
                        .unwrap()
                        .to_line_string()
                        .points()
                        .tuple_windows()
                        .filter_map(|(p0, p1)| {
                            let distance = LatLng::new(p0.y(), p0.x())
                                .ok()?
                                .distance_m(LatLng::new(p1.y(), p1.x()).ok()?);
                            Some(JourneyLeg {
                                destination: p1,
                                distance_m: distance.round().abs() as usize,
                            })
                        })
                        .collect::<Vec<_>>();
                    legs.into_iter()
                })
                .collect(),
        )
    }
}

impl From<RoutingData> for JourneyPlanner {
    fn from(routing: RoutingData) -> Self {
        Self::new(routing)
    }
}

pub struct RoutingData {
    nodes: RecordBatch,
    node_positions: PointArray,
    edges: RecordBatch,
    edge_positions: LineStringArray,
    node_map: IndexSet<Uuid>,
    edge_map: HashMap<(usize, usize), usize>,
}

impl RoutingData {
    pub(crate) fn try_new(nodes: RecordBatch, edges: RecordBatch) -> Result<Self> {
        Self::nodes_schema().logically_equivalent_names_and_types(nodes.schema().as_ref())?;
        Self::edges_schema().logically_equivalent_names_and_types(edges.schema().as_ref())?;
        let mut node_map = IndexSet::new();

        let ids = nodes.column(1).as_fixed_size_binary();
        for id in ids.iter().flatten() {
            let id = Uuid::from_slice(id)?;
            node_map.insert(id);
        }

        let mut edge_map = HashMap::new();
        let sources = edges.column(1).as_fixed_size_binary();
        let targets = edges.column(2).as_fixed_size_binary();
        for (index, (source, target)) in sources.iter().zip(targets.iter()).enumerate() {
            if let (Some(source), Some(target)) = (source, target) {
                let source = Uuid::from_slice(source)?;
                let target = Uuid::from_slice(target)?;
                let source_index = node_map.get_index_of(&source).unwrap();
                let target_index = node_map.get_index_of(&target).unwrap();
                edge_map.insert((source_index, target_index), index);
            }
        }

        let node_positions = (
            nodes.column(3).as_struct(),
            PointType::new(Dimension::XY, Default::default()),
        )
            .try_into()?;
        let edge_positions = (
            edges.column(4).as_list::<i32>(),
            LineStringType::new(Dimension::XY, Default::default()),
        )
            .try_into()?;

        Ok(Self {
            nodes: nodes.project(&[0, 1, 2])?,
            node_positions,
            edges: edges.project(&[0, 1, 2, 3])?,
            edge_positions,
            node_map,
            edge_map,
        })
    }

    pub(crate) fn nodes_schema() -> SchemaRef {
        static NODE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
            SchemaRef::new(Schema::new(vec![
                Field::new("location", DataType::Utf8, false),
                Field::new("id", DataType::FixedSizeBinary(16), false)
                    .with_extension_type(UuidExtension),
                Field::new(
                    "properties",
                    DataType::Struct(
                        vec![
                            Field::new("highway", DataType::Utf8, true),
                            // Field::new("junction", DataType::Utf8, true),
                            Field::new("osmid", DataType::Int64, true),
                            Field::new("railway", DataType::Utf8, true),
                            Field::new("ref", DataType::Utf8, true),
                            Field::new("street_count", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    true,
                ),
                Field::new(
                    "geometry",
                    DataType::Struct(
                        vec![
                            Field::new("x", DataType::Float64, true),
                            Field::new("y", DataType::Float64, true),
                        ]
                        .into(),
                    ),
                    true,
                )
                .with_extension_type(PointType::new(Dimension::XY, Default::default())),
            ]))
        });
        NODE_SCHEMA.clone()
    }

    pub(crate) fn edges_schema() -> SchemaRef {
        static EDGE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
            SchemaRef::new(Schema::new(vec![
                Field::new("location", DataType::Utf8, false),
                Field::new("source", DataType::FixedSizeBinary(16), false)
                    .with_extension_type(UuidExtension),
                Field::new("target", DataType::FixedSizeBinary(16), false)
                    .with_extension_type(UuidExtension),
                Field::new(
                    "properties",
                    DataType::Struct(
                        vec![
                            Field::new("highway", DataType::Utf8, true),
                            Field::new("length", DataType::Float64, true),
                            Field::new("maxspeed_m_s", DataType::Float64, true),
                            Field::new("name", DataType::Utf8, true),
                            Field::new("osmid_source", DataType::Int64, true),
                            Field::new("osmid_target", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                ),
                Field::new_list(
                    "geometry",
                    Field::new_list_field(
                        DataType::Struct(
                            vec![
                                Field::new("x", DataType::Float64, true),
                                Field::new("y", DataType::Float64, true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                    true,
                )
                .with_extension_type(LineStringType::new(Dimension::XY, Default::default())),
            ]))
        });
        EDGE_SCHEMA.clone()
    }

    pub fn nodes(&self) -> impl ExactSizeIterator<Item = StreetNode<'_>> {
        (0..self.nodes.num_rows()).map(|i| StreetNode::new(self, i))
    }

    pub fn edges(&self) -> impl ExactSizeIterator<Item = StreetEdge<'_>> {
        (0..self.edges.num_rows()).map(|i| StreetEdge::new(self, i))
    }

    fn edge(&self, index: usize) -> StreetEdge<'_> {
        StreetEdge::new(self, index)
    }

    fn build_router(&self) -> FastGraph {
        let mut graph = InputGraph::new();

        for edge in self.edges() {
            let source_id = self
                .node_map
                .get_index_of(&Uuid::from_slice(edge.source()).unwrap())
                .unwrap();
            let target_id = self
                .node_map
                .get_index_of(&Uuid::from_slice(edge.target()).unwrap())
                .unwrap();
            graph.add_edge(source_id, target_id, edge.length().round().abs() as usize);
        }

        graph.freeze();
        fast_paths::prepare(&graph)
    }

    pub fn into_trip_planner(self) -> JourneyPlanner {
        JourneyPlanner::new(self)
    }
}

pub struct StreetNode<'a> {
    data: &'a RoutingData,
    valid_index: usize,
}

impl<'a> StreetNode<'a> {
    fn new(data: &'a RoutingData, valid_index: usize) -> Self {
        Self { data, valid_index }
    }

    pub fn id(&self) -> &Uuid {
        self.data.node_map.get_index(self.valid_index).unwrap()
    }

    pub fn is_in_cell(&self, cell: CellIndex) -> bool {
        self.cell(cell.resolution())
            .map(|c| c == cell)
            .unwrap_or(false)
    }

    pub fn cell(&self, resolution: Resolution) -> Option<CellIndex> {
        let coords = self.geometry().ok()?.coord()?;
        let lat_lng: LatLng = coords.to_coord().try_into().ok()?;
        Some(lat_lng.to_cell(resolution))
    }

    pub fn geometry(&self) -> Result<ArrowPoint<'_>> {
        Ok(self.data.node_positions.value(self.valid_index)?)
    }
}

pub struct StreetEdge<'a> {
    data: &'a RoutingData,
    valid_index: usize,
}

impl<'a> StreetEdge<'a> {
    fn new(data: &'a RoutingData, valid_index: usize) -> Self {
        Self { data, valid_index }
    }

    pub fn source(&self) -> &[u8] {
        self.data
            .edges
            .column(1)
            .as_fixed_size_binary()
            .value(self.valid_index)
    }

    pub fn target(&self) -> &[u8] {
        self.data
            .edges
            .column(2)
            .as_fixed_size_binary()
            .value(self.valid_index)
    }

    pub fn length(&self) -> f64 {
        self.data
            .edges
            .column(3)
            .as_struct()
            .column(1)
            .as_primitive::<Float64Type>()
            .value(self.valid_index)
    }

    pub fn geometry(&self) -> Result<ArrowLineString<'_>> {
        Ok(self.data.edge_positions.value(self.valid_index)?)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;

    use super::*;

    #[test_log::test]
    fn test_journey() {
        let journey = Journey {
            transport: Transport::default(),
            legs: vec![
                JourneyLeg {
                    destination: Point::new(-0.1553777, 51.5453468),
                    distance_m: 10,
                },
                JourneyLeg {
                    destination: Point::new(-0.1556396, 51.5455222),
                    distance_m: 20,
                },
                JourneyLeg {
                    destination: Point::new(-0.1556897, 51.5455559),
                    distance_m: 10,
                },
                JourneyLeg {
                    destination: Point::new(-0.1557318, 51.5455873),
                    distance_m: 10,
                },
            ],
            current_leg_index: 0,
            current_leg_progress: 0.0,
        };

        // Test advancing a journey with a single time step that completes all legs
        let mut journey1 = journey.clone();
        journey1.transport = Transport::Car;
        let time_step = std::time::Duration::from_secs(60); // 60 seconds at car speed should complete all legs

        let traversed_points = journey1.advance(time_step);

        // We should have traversed all points
        assert_eq!(traversed_points.len(), journey.legs.len());
        assert!(journey1.is_done(), "All legs should be completed");

        // Test advancing a journey with multiple time steps
        let mut journey2 = journey.clone();
        journey2.transport = Transport::Foot;
        let time_step = std::time::Duration::from_secs(5); // 5 seconds at walking speed

        // First step should traverse part of the first leg
        let traversed_points1 = journey2.advance(time_step);
        assert_eq!(
            traversed_points1.len(),
            1,
            "Should have one intermediate point"
        );
        assert_eq!(
            journey2.current_leg_index, 0,
            "Should still be on the first leg"
        );
        assert!(
            journey2.current_leg_progress > 0.0,
            "Should have made progress on the first leg"
        );

        // Second step should complete the first leg and start on the second
        let traversed_points2 = journey2.advance(time_step);
        assert_eq!(
            traversed_points2.len(),
            2,
            "Should have traversed one point, and started on the second leg"
        );
        assert_eq!(journey2.current_leg_index, 1, "Should be on the second leg");
        assert!(
            journey2.current_leg_progress > 0.0,
            "Should have made progress on the second leg"
        );

        // Test with zero time step
        let mut journey3 = journey.clone();
        let traversed_points = journey3.advance(std::time::Duration::from_secs(0));
        assert!(
            traversed_points.is_empty(),
            "Zero time step should not traverse any points"
        );
        assert_eq!(
            journey3.current_leg_index, 0,
            "Should still be on the first leg"
        );
        assert_eq!(
            journey3.current_leg_progress, 0.0,
            "Should not have made any progress"
        );

        // Test with empty journey
        let mut empty_journey = Journey::default();
        let traversed_points = empty_journey.advance(time_step);
        assert!(
            traversed_points.is_empty(),
            "Empty journey should not traverse any points"
        );
        assert!(empty_journey.is_done(), "Empty journey should be done");
    }

    #[test_log::test]
    fn test_journey_progress_tracking() {
        // Create a journey with 4 legs of different lengths
        let journey = Journey {
            transport: Transport::default(),
            legs: vec![
                JourneyLeg {
                    destination: Point::new(-0.1553777, 51.5453468),
                    distance_m: 100, // 100m
                },
                JourneyLeg {
                    destination: Point::new(-0.1556396, 51.5455222),
                    distance_m: 200, // 200m
                },
                JourneyLeg {
                    destination: Point::new(-0.1556897, 51.5455559),
                    distance_m: 150, // 150m
                },
                JourneyLeg {
                    destination: Point::new(-0.1557318, 51.5455873),
                    distance_m: 50, // 50m
                },
            ],
            current_leg_index: 0,
            current_leg_progress: 0.0,
        };

        // Test initial state
        assert_eq!(journey.total_distance_m(), 500);
        assert_eq!(journey.distance_completed_m(), 0.0);
        assert_eq!(journey.distance_remaining_m(), 500.0);
        assert_eq!(journey.progress_percentage(), 0.0);
        assert!(!journey.is_done());

        // Test after completing first leg
        let mut journey = journey;
        journey.transport = Transport::Foot;
        let time_step = std::time::Duration::from_secs(72); // 72s at 5km/h = 100m
        journey.advance(time_step);

        assert_eq!(journey.current_leg_index, 1);
        assert_eq!(journey.current_leg_progress, 0.0);
        assert_eq!(journey.distance_completed_m(), 100.0);
        assert_eq!(journey.distance_remaining_m(), 400.0);
        assert_eq!(journey.progress_percentage(), 0.2);

        // Test partial progress in second leg
        let time_step = std::time::Duration::from_secs(36); // 36s at 5km/h = 50m
        journey.advance(time_step);

        assert_eq!(journey.current_leg_index, 1);
        assert_eq!(journey.current_leg_progress, 0.25); // 50m/200m
        assert_eq!(journey.distance_completed_m(), 150.0);
        assert_eq!(journey.distance_remaining_m(), 350.0);
        assert_eq!(journey.progress_percentage(), 0.3);

        // Test completing the journey
        let time_step = std::time::Duration::from_secs(252); // 252s at 5km/h = 350m
        journey.advance(time_step);

        assert!(journey.is_done());
        assert_eq!(journey.distance_completed_m(), 500.0);
        assert_eq!(journey.distance_remaining_m(), 0.0);
        assert_eq!(journey.progress_percentage(), 1.0);

        // Test estimated time remaining
        let journey = Journey {
            transport: Transport::Bicycle,
            legs: vec![JourneyLeg {
                destination: Point::new(0.0, 0.0),
                distance_m: 1000,
            }],
            current_leg_index: 0,
            current_leg_progress: 0.0,
        };

        // Test with bicycle (15 km/h)
        assert_abs_diff_eq!(
            journey.estimated_time_remaining_s(),
            240.0,
            epsilon = 0.0001
        ); // 1km at 15km/h = 240s
    }

    #[test_log::test]
    fn test_empty_journey() {
        let journey = Journey::default();

        assert_eq!(journey.total_distance_m(), 0);
        assert_eq!(journey.distance_completed_m(), 0.0);
        assert_eq!(journey.distance_remaining_m(), 0.0);
        assert_eq!(journey.progress_percentage(), 1.0);
        assert!(journey.is_done());
    }
}

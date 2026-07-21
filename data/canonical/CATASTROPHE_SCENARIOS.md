# Catastrophe scenarios for DevConnect

This module defines deterministic incidents for the standalone catastrophe command app.

## Scenarios

- `bridge_outage`: major bridge outage, strongest route disruption.
- `city_center_accident`: severe downtown traffic incident.
- `city_center_protest`: city-center protest with perimeter closures.
- `tomato_supply_shock`: supply-chain outage with menu substitutions and refunds.

## Deterministic seed contract

Use a stable pair of runtime parameters:

- `CATASTROPHE_SCENARIO` (scenario id)
- `CATASTROPHE_SEED` (integer)

Given the same pair and same location ids, `generate_incidents()` produces the same incident stream identifiers and ordering. This is intended for rehearsal repeatability.

## Output tables (created by stage)

- `${CATALOG}.${SIMULATOR_SCHEMA}.catastrophe_scenarios`
- `${CATALOG}.${SIMULATOR_SCHEMA}.catastrophe_incidents`

`catastrophe_incidents` is re-generated per run to keep live-demo resets simple.

---
layout: post
title: "Cooking with Casper’s Kitchens"
description: "A living Databricks demo framework built around a simulated ghost kitchen — designed for reuse, realism, and end-to-end storytelling."
date: 2024-10-10 12:00:00 -0000
categories: blog
tags: [Databricks, Demo Framework, Lakehouse, Lakeflow, Lakebase, Apps, AI]
---

[Casper’s Kitchens](https://github.com/databricks-solutions/caspers-kitchens) started with a practical problem. Our team builds demos all the time, and we wanted a way to make them consistent, composable, and reusable.

We first piloted the Casper’s concept with our ongoing [Databricks DevConnect series](https://luma.com/DevConnectDBX) and again at [DAIS 2025](https://www.databricks.com/dataaisummit). The response was great; people appreciated how comprehensive the demos felt across the platform and that reaction pushed us to formalize the work into something shareable.

As we developed the deployment mechanism, Casper’s evolved beyond a simple demo. It became a living project (part framework, part narrative) for creating end-to-end Databricks experiences that showcase the full platform in a realistic and unified way.

## The idea

Casper’s Kitchens is a simulated ghost-kitchen business that runs entirely on Databricks. You can spin it up in a few minutes, and it immediately starts streaming in JSON order and driver events, curating data through Lakeflow pipelines, and powering downstream dashboards, agents, and apps.

The data itself is designed to feel real. Each order includes realistic timestamps, customer and driver behavior, and GPS coordinates that trace the driver’s actual delivery route. The result is an event stream that looks and behaves like production data, even though it is entirely synthetic. By default the demo is "real-time", meaning you can actually track an order as it happens, but you can also easily configure the speed to simulate data as fast as you want.

The ghost kitchen theme is just one instance of a broader demo framework. It's possible, and could potentially become a real goal, to make something that can easily be adapted for other verticals such as retail, logistics, finance, or manufacturing, all sharing the same underlying structure of data, pipelines, and apps connected by a coherent story.

## Why we’re building it

The main goal is to have a single asset our team can use to show the Databricks platform as a whole. Instead of isolated notebooks, pipelines, or dashboards, we want a cohesive environment that ties them all together. Something that demonstrates how Databricks actually works in practice across ingestion, transformation, storage, governance, AI, and applications.

A few guiding ideas:

- **Platform wide CUJs (Critical User Journeys):** Casper’s attempts to tie together what would normally be separate journeys through the platform. It gives us a consistent launching point for demos and ensures that the way we develop or showcase one feature is compatible with how we use others. In other words, we want to fully, if not over, emulate our users, and experience the same platform constraints they do.

- **Dogfooding Databricks:** everything runs natively on Databricks itself. This keeps us honest and also pushes us to explore the quiet realization that Databricks can now handle almost everything end-to-end. It’s not that you should use it for everything, but for us, it’s a fun and practical constraint that reveals what’s possible.

- **Reusable framework:** each improvement compounds. We can use Databricks Jobs to orchestrate modular stages that stack together; another way to dogfood the platform rather than relying on external tooling. It means every new demo or feature naturally fits into the same shared world.

- **Narrative:** the story keeps everything connected. It makes demos easier to remember, deliver, and extend. It also helps our content stay in the same “universe,” so that multiple talks or events build on one another rather than feeling disconnected.

## Current state

Right now, Casper’s Kitchens runs as a fully simulated environment. The [init.ipynb](https://github.com/databricks-solutions/caspers-kitchens/blob/main/init.ipynb) notebook sets up the main job with which you can control the demo universe. 

You can already extend the demo data generator by adding new locations through simple configuration files. Each location has its own JSON definition that controls parameters like order volume, start time, and growth rate. These configurations live in the [data/generator/configs](https://github.com/databricks-solutions/caspers-kitchens/blob/main/data/generator/configs/README.md) folder and can be easily customized or duplicated to create new scenarios.

![Stages](../../images/stages.png)

The current stages include:

1. **Raw data generation:** dimension tables for brands, menus, and items, along with synthetic order JSON written to a Databricks Volume.

2. **Lakeflow pipeline:** a medallion architecture that produces normalized order (silver) tables and (gold) summary aggregates.

3. **Refund agent:** an AI agent that uses tool calling to suggest whether an order should be refunded.

4. **Scoring stream:** a live stream that applies the refund model to new orders in real time.

5. **Refund app:** a Databricks App backed by Lakebase that allows a human reviewer to confirm or reject the agent’s refund suggestions.

Because we're using tasks (within Databricks Jobs), you can even use the native UI to select to run **only the stages you need**, in case you're working on only a single part of the demo universe. This is a pretty significant feature that emerged as we forced ourselves to dogfood and stick to a Databricks only approach - something we couldn't quite achieve if we had stuck to Terraform, for example.

## Where it’s going

It's already incredibly useful for our demo needs, but, beyond that, there’s a lot we’re exploring:

- **Demo assets:** stock demo scripts, talk tracks, and short videos to help others deliver Casper’s.
- **Interactive data generation:** an App-based interface for building or resetting simulated data.
- **Real ingestion:** connecting to external systems so the pipelines pull in actual data instead of stubs.
- **Meta demos:** building demos about demos. A good example is an MCP server that lets you use the Databricks Playground to control the entire Casper’s demo with natural language: `start caspers`, `new location in Mountain View`, `simulate an order right now` etc.

## Why it matters

Databricks has quietly become a platform where you can build anything, not just data pipelines or AI models but entire applications. With managed Postgres (Lakebase) and managed Apps sitting next to your data and AI, you can now go end-to-end in one place. Casper’s Kitchens is essentially our team's vehicle to explore that.

## Get involved

Casper’s is early, and it’ll keep evolving. We’d love for people to explore it, try spinning it up, and see what breaks. The [GitHub repo has the setup steps, current issues, and roadmap ideas.](https://github.com/databricks-solutions/caspers-kitchens/tree/main)
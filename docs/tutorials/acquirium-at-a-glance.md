---
title: Acquirium at a glance
---

## Architecture

```text
 ┌───────────┐   ┌────────────┐         ┌──────────────────────────────────────────────┐
 │ your code │──▶│ Acquirium  │  HTTP   │               acquirium server               │
 │ notebooks │◀──│   client   │◀───────▶│                                              │
 └───────────┘   └────────────┘         │   graph store        plant model + WaTr      │
                                        │   timeseries store   streams of readings     │
                                        │   experiments        studies, runs, results  │
                                        │   text matcher       words to ontology terms │
                                        │                                              │
                                        │         ▲ write          read │ ▲ write      │
                                        │         │                     ▼ │            │
                                        │   ┌─────┴─────┐            ┌───────┐         │
                                        │   │  drivers  │            │ apps  │         │
                                        │   └─────▲─────┘            └───────┘         │
                                        └─────────┼────────────────────────────────────┘
                                                  │
                     ┌────────────────────────────┴────────────────────────────┐
                     │ SCADA · historians · CSV · MQTT · lab sheets · WaterTAP │
                     └─────────────────────────────────────────────────────────┘
```

- **Server:** the program that stores everything and answers your queries.
- **Client:** the Python module to talk to the server using scripts, notebooks etc.
- **Graph store:** holds the semantic metadata model (e.g. equipment, how it is connected and what is measured where). The model is written with the [WaTr](https://github.com/DataDrivenCPS/water-ontology) ontology.
- **Timeseries store:** holds the readings (i.e. timeseries) of the streams.
- **Text matcher:** turns words like `pump` or `pressure` into terms of the ontology, so you can query without knowing the model's internals.
- **Drivers:** A driver reads a source such as SCADA, a historian, a CSV file or a WaterTAP simulation on a schedule and writes the readings to the server.
- **Apps:** calculate new values from the stored readings, such as a soft sensor or an average, and keep them up to date as data arrives.
- **Experiments:** define and record the inputs and results of experiments.

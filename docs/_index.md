---
title: Acquirium documentation
---

A treatment plant's data is scattered. Flows and pressures sit in the SCADA
historian, lab results in spreadsheets, and setpoints in a model someone built
for a study. Each source names its signals its own way, so the first job of
any analysis is working out which tag is the feed pressure on the second RO
train.

Acquirium keeps a model of the plant beside the data: the equipment, how it is
piped together, and what each sensor measures, described with the
[WaTr](https://github.com/DataDrivenCPS/water-ontology) and ASHRAE 223P
vocabularies. You ask for data in those terms, such as *the pressures on the
RO's inlet*, and Acquirium finds the streams. Apps compute derived streams,
such as averages, alarms and soft sensors, and keep them correct as readings
arrive late or are corrected.

## Architecture

```text
                     ┌──────────────── Acquirium server ────────────────┐
 SCADA, historians,  │                                                  │
 CSV, MQTT,          │  Plant model                Readings             │  query
 lab sheets          │  equipment, sensors  ◀───▶  every value from     │ ◀────── your code,
     │               │  and what each one          every sensor, with   │ ──────▶ notebooks
     ▼               │  measures                   its corrections      │  data
  drivers ─────────▶ │  (WaTr, ASHRAE 223P)          │          ▲       │
  points, readings   │                               ▼          │       │
                     │                            Apps: averages,       │
                     │                            alarms, soft sensors, │
                     │                            kept current          │
                     └──────────────────────────────────────────────────┘
```

Drivers read from plant systems and send readings to the server, along with
the points they belong to in the plant model. Queries name equipment and
measurements in the model's terms and return the matching readings. Apps
compute derived streams from readings and update them when a reading arrives
late or is corrected. [Server internals](explanation/server-internals.md)
shows the parts of the server and where each one keeps its data.

## Documentation structure

[Tutorials](tutorials/_index.md) are the place to start. They build on each
other, from starting a server and querying a simulated plant to writing
drivers and apps of your own. The notebooks under
[`notebooks/watertap/`](https://github.com/DataDrivenCPS/acquirium/tree/main/notebooks/watertap)
cover the same ground interactively.

[How-to guides](how-to/_index.md) answer specific questions once you know
your way around: loading a plant model, recording experiments, running the
server in production.

[Reference](reference/_index.md) pages list every method, configuration key
and endpoint.

[Explanation](explanation/_index.md) pages describe how Acquirium works and
why it was built that way: how a query finds its measurements, how derived
streams stay correct when data changes, and what the server does underneath.

## Citation

The design is described in [Acquirium: Toward Interoperable Data Driven
Applications in Water Treatment Systems](https://dl.acm.org/doi/abs/10.1145/3744256.3812557)
(BuildSys '26). If you use Acquirium in your work, cite:

```bibtex
@inproceedings{saka2026acquirium,
  title     = {Acquirium: Toward Interoperable Data Driven Applications in Water Treatment Systems},
  author    = {Saka, Umut Mete and Chapin, Fletcher T. and Paul, Lazlo and Anwar, Avia and Struck, Scott and Mauter, Meagan S. and Fierro, Gabe},
  booktitle = {Proceedings of the 13th ACM International Conference on Systems for Energy-Efficient Buildings, Cities, and Transportation (BuildSys '26)},
  year      = {2026},
  publisher = {ACM},
  address   = {Banff, AB, Canada},
  doi       = {10.1145/3744256.3812557}
}
```

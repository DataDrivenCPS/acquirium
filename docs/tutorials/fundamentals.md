---
title: 0 - Fundamentals
---




## Data model

- The **graph** describes the plant: its equipment, how the equipment is connected, and the points.
- A **point** is one measured quantity, such as the outlet pressure of a pump. It carries the unit and the quantity kind.
- A **stream** is the timeseries behind a point: rows of `(time, value)`. It is identified by who writes it (`source_id`) and which series it is (`ref_name`).
- A **reference** links a point to its stream. A point can have more than one stream, for example a sensor and a lab sheet measuring the same thing.
- A **source** is whoever writes a set of streams: a driver, an app, or your own script.

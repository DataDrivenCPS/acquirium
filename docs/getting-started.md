---
title: Getting started
---

## Install

Acquirium requires Python 3.11 or newer.
Pick the package manager you use:

::::{dropdown} uv (recommended)
```bash
uv init my-plant && cd my-plant
uv add acquirium
```

:::{note}
With uv, the `acquirium` and `python` commands are run as `uv run acquirium ...` and `uv run python ...`.
:::

New to uv? See [installing uv](https://docs.astral.sh/uv/getting-started/installation/).
::::

::::{dropdown} conda
```bash
conda create -n my-plant python=3.11
conda activate my-plant
pip install acquirium
```

New to conda? See [getting started with conda](https://docs.conda.io/projects/conda/en/stable/user-guide/getting-started.html).
::::

::::{dropdown} pip
```bash
mkdir my-plant && cd my-plant
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install acquirium
```

New to pip? See [installing packages with pip](https://packaging.python.org/en/latest/tutorials/installing-packages/).
::::

## Running Acquirium

Here's a short script to check that the installation works.
We'll explain what each line does in the following pages of tutorial.

Save it as `check.py`:

```python
from datetime import datetime

import acquirium as aq

acq = aq.init(exact_only=True)    # start Acquirium

# write one temperature reading
acq.register_streams([
    {"source_id": "check", "ref_name": "temperature", "value_kind": "numeric"}
])
acq.insert_timeseries("check", "temperature", [(datetime(2026, 1, 1), 21.5)])

# read it back
print(acq.query().measurement().dataframe())

aq.shutdown()                     # stop Acquirium
```

Then run it:

```bash
python check.py
```
```text
Acquirium: starting a local server for /home/you/my-plant/.acquirium
Acquirium: ready at http://127.0.0.1:50532
shape: (1, 2)
┌─────────────────────────┬───────────────────┐
│ time                    ┆ data__temperature │
│ ---                     ┆ ---               │
│ datetime[μs, UTC]       ┆ f64               │
╞═════════════════════════╪═══════════════════╡
│ 2026-01-01 00:00:00 UTC ┆ 21.5              │
└─────────────────────────┴───────────────────┘
```

The first start takes about a minute.
If you see the `ready at` line and the table with the reading, Acquirium is running.
The folder and the number after `127.0.0.1:` will be different on your machine.

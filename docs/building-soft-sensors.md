# Building soft sensors without rebuilding the plumbing

A soft sensor reads one or more stored streams, calculates a value, and writes
that value back as another stream. The calculation is often the easy part. A
running average is one line of Polars. The surrounding work—finding inputs,
naming outputs, recording lineage, scheduling runs, and making the result
debuggable—is where small scripts tend to become small systems.

Acquirium's app framework owns that surrounding work. Your app should describe
what it needs and implement the calculation.

This guide builds a running-average app and follows it from a local check to a
long-running deployment.

## The useful mental model

There are three distinct things that are easy to conflate:

- The **source spec**, such as `./running_average.py:RunningAverage`, tells the
  CLI where it can import Python code.
- The app's **name**, such as `running_average`, is its identity on the server.
- A mapped app's **stream mappings** connect each selected input point to one
  deterministic output point.

The source spec is a loading instruction. The name is the deployment identity.
Changing the file does not modify an app that is already running: registration
ships a snapshot of the source to the server.

If you deploy changed code with the same name, use `--replace`:

```bash
uv run acquirium app run ./running_average.py:RunningAverage --replace
```

If you change `App.name`, Acquirium sees a second app. The original app remains
registered until you remove it by name:

```bash
uv run acquirium app deregister old_running_average
```

For mapped apps, the app name participates in output identity. Renaming one is
therefore intentionally equivalent to creating a new family of output streams.

## A running average for every matching sensor

Here is the complete shape of a mapped soft sensor:

```python
import polars as pl

from acquirium import AppContext, MappedApp, MappedStream, OutputTemplate


class RunningAverage(MappedApp):
    name = "running_average"
    input_alias = "sensor"
    fetch_limit = 60

    output = OutputTemplate(
        name="average",
        value_kind="numeric",
        unit="same_as_input",
    )

    def build_query(self, aq):
        return aq.query().measurement(
            alias=self.input_alias,
            quantity_kind="Temperature",
        )

    def transform(self, stream: MappedStream, ctx: AppContext):
        window = int(ctx.params.get("window", 5))
        return (
            stream.values
            .with_columns(
                pl.col("value")
                .rolling_mean(window_size=window, min_samples=1)
                .alias("value")
            )
            .tail(1)
        )
```

`build_query` is a semantic selector. It may match zero streams today and fifty
tomorrow. `MappedApp` calls `transform` independently for every match.

Each `MappedStream` contains:

- `values`, a Polars frame with `time` and `value` columns;
- `input_point_uri`, `input_ref_uri`, and `input_unit`;
- `output_point_uri` and `output_ref_name`.

The framework derives a stable output identity from the app name, output
template name, and input point URI. It registers the output, copies the unit
when requested, and records a direct `isCalculatedFrom` lineage edge. It also
filters out the app's own outputs, preventing a broad selector from recursively
feeding the result back into itself.

The repository includes a runnable version at
[`scripts/examples/running_average.py`](../scripts/examples/running_average.py).

## Inspect before executing

Start with the cheapest check:

```bash
uv run acquirium app check ./running_average.py:RunningAverage
```

This imports the class and checks its static contract. It does not need a
server and does not resolve the semantic selector.

To see what the selector means against live graph data, print its mappings:

```bash
uv run acquirium app mappings ./running_average.py:RunningAverage
```

This accepts either a local source spec or the name of a registered app:

```bash
uv run acquirium app mappings running_average
uv run acquirium app mappings running_average --json
```

The local form resolves the selector now. The name form asks the server for the
mappings held by the registered actor, including inputs discovered after graph
changes. The JSON form is useful in scripts.

## Run once without leaving a trace

`--dry-run` executes the query, build phase, and run phase, then replaces the
output sink with a recorder:

```bash
uv run acquirium app run ./running_average.py:RunningAverage \
  --dry-run \
  --params '{"window": 5}'
```

The result describes matched streams, returned outputs, intended inserts or
webhooks, timings, and validation warnings. It does not register the app, write
observations, change the graph, or call declared output webhooks.

The read-only wrapper blocks Acquirium mutations made through the app API. It
cannot prevent arbitrary side effects such as directly opening a file or
calling `requests.post`. Represent external effects as `Output` objects if you
want dry-run to suppress them.

## Put `transform` under a microscope

When a data frame does not look the way you expected, a JSON preview is often
too indirect. The debug command prepares real inputs and drops into a read-only
Python REPL:

```bash
uv run acquirium app debug ./running_average.py:RunningAverage \
  --params '{"window": 5}'
```

Useful expressions inside the shell include:

```python
len(streams)
stream.input_point_uri
stream.values
transform()
transform(streams[1])
run()
```

`stream` is the first mapped input and `streams` contains all of them.
`transform()` returns the direct result of your method. `run()` evaluates the
whole app contract. Neither helper persists outputs. The shell also exposes
`app`, `aq`, `ctx`, `query`, `queries`, and `state`.

The production runner and debugger use the same code to construct
`MappedStream` objects. Debugging does not give you a convenient approximation
of the input—it gives you the input shape `transform` will actually receive.

## One shot, polling, and restarts

Without flags, `app run` registers the source snapshot and dispatches one run:

```bash
uv run acquirium app run ./running_average.py:RunningAverage \
  --replace \
  --params '{"window": 5}'
```

Add `--keep-alive` to poll on an interval:

```bash
uv run acquirium app run ./running_average.py:RunningAverage \
  --replace \
  --keep-alive \
  --interval 60 \
  --params '{"window": 5}'
```

Keep-alive is interval-driven, not event-driven. New data does not itself
trigger a run. Every tick resolves graph changes, fetches the selected inputs,
and applies the transform to each stream. `fetch_limit = 60` is a stateless
lookback, not a cursor or exactly-once checkpoint.

An active keep-alive app persists its interval, parameters, and fixed
`start`/`end` window. If the server restarts, Acquirium rebuilds the app and
resumes the loop. An explicit stop, replacement, or deregistration clears that
resume marker, so an intentionally stopped app stays stopped.

## Operate apps by name

Once code reaches the server, the app name is the useful handle:

```bash
uv run acquirium app list
uv run acquirium app list --name running_average
uv run acquirium app mappings running_average
uv run acquirium app deregister running_average
```

`app list` includes the original absolute `path.py:Class` spec, making it easy
to copy into `app run`, `app mappings`, or `app debug`. Deregistration stops the
loop, removes the registration graph and persisted source snapshot, but leaves
previously emitted observations intact.

## Make deployment declarative

The same app can start with the server from `acquirium.toml`:

```toml
[[apps]]
spec = "./running_average.py:RunningAverage"
replace = true
autostart = true
keep_alive = true
interval = 60
build_params = {}
params = { window = 5 }
```

The path is relative to the TOML file. `replace = true` makes configuration
desired state: restarting the server uploads the current source. Acquirium
waits for its API, restores persisted apps, starts configured drivers, and then
processes configured apps so selectors can see metadata inserted during driver
setup.

Set `autostart = false` to register and build without running. Set
`keep_alive = false` for a one-shot startup run.

## What the framework promises

The important boundary is now small enough to remember:

- Your app selects inputs and calculates outputs.
- `MappedApp` fans one calculation across any number of matching streams.
- Acquirium owns output identity, lineage, registration, validation, emission,
  scheduling, and keep-alive recovery.
- `check`, `mappings`, `--dry-run`, and `debug` expose progressively more of
  the execution path before you commit writes.
- The app name is server identity; the source spec is local provenance.

For the complete `App`, `Output`, and configuration reference, continue with
[Apps](apps.md).

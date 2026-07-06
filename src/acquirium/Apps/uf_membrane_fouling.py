from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import polars as pl

from acquirium.Apps.base import App, Output
from acquirium.Apps.output_emission import emit_outputs
from acquirium.Graframe import Graframe, Profile
from acquirium.internals.models import AppContext

# Vogel correlation for water's dynamic viscosity (Pa*s), T in deg C -- lets the
# resistance model correct for feed temperature without a lookup table.
_MU_A, _MU_B, _MU_C = 2.414e-5, 247.8, 140.0


def _viscosity_pas(temp_c: pl.Expr) -> pl.Expr:
    temp_k = temp_c + 273.15
    return _MU_A * 10 ** (_MU_B / (temp_k - _MU_C))


@dataclass(frozen=True)
class PlantSignals:
    """Plant-wide UF feed signals (shared by every module)."""

    feed_pressure: str
    feed_pressure_unit: str
    feed_temp: str
    backwash_flow: str


@dataclass(frozen=True)
class ModuleSignals:
    """One UF module's identity, membrane area and permeate-side signals."""

    module: str
    area_m2: float
    permeate_flow: str
    permeate_flow_unit: str
    permeate_pressure: str
    permeate_pressure_unit: str


def _one(nodes: list[str], what: str) -> str:
    """Return the sole element of ``nodes`` or raise a clear, named error.

    Discovery in this app walks the graph by *meaning* (quantity kind + role);
    when a required signal is absent the raw ``.nodes()[0]`` ``IndexError`` is
    useless to an operator. This surfaces what was missing instead.
    """
    if not nodes:
        raise LookupError(f"could not find a UF {what} point in the graph")
    if len(nodes) > 1:
        raise LookupError(
            f"expected exactly one UF {what} point, found {len(nodes)}: {nodes}"
        )
    return nodes[0]


def discover(client: Any) -> tuple[PlantSignals, list[ModuleSignals]]:
    """Find every UF module and its fouling signals purely by *meaning* -- quantity
    kind plus connection-point role -- never a hardcoded SCADA tag or module URI.

    Every step is an explicit CURIE (never fuzzy natural-language resolution), so
    discovery is deterministic and needs no embedding matcher at runtime.
    """
    g = Graframe(client, profile=Profile.base(), fuzzy=False)
    props = g.instances("s223:QuantifiableObservableProperty")

    def feed_point(quantity: str) -> str:
        return _one(
            props.having("qudt:hasQuantityKind", value=quantity)
            .having("^s223:hasProperty/s223:hasRole", value="nawi:Role-Feed")
            .nodes(),
            f"feed {quantity.split('/')[-1].lower()}",
        )

    def unit_of(point_uri: str) -> str:
        return _one(
            g.nodes(point_uri).follow("qudt:hasUnit").nodes(),
            f"unit for {point_uri}",
        )

    feed_pressure = feed_point("qk:Pressure")
    backwash_flow = _one(
        props.having("qudt:hasQuantityKind", value="qk:VolumeFlowRate")
        .having("^s223:hasProperty/nawi:hasProcess", value="nawi:Process-Backwashing")
        .nodes(),
        "backwash flow",
    )
    plant = PlantSignals(feed_pressure, unit_of(feed_pressure), feed_point("qk:Temperature"), backwash_flow)

    # Membrane area is a static design value (s223:hasValue on the contained
    # membrane element), so one facet chain gets every module's area at once.
    areas = (
        g.instances("nawi:UltrafiltrationUnit").mark("module")
        .follow("s223:contains").follow("s223:hasProperty")
        .follow("s223:hasValue").mark("area")
        .select("module", "area", compact=False)
    )
    area_by_module = dict(zip(areas["module"], areas["area"]))

    modules = []
    for module_uri, area_m2 in area_by_module.items():
        mod = g.nodes(module_uri)

        def permeate(quantity: str) -> str:
            return _one(
                mod.follow("s223:hasConnectionPoint", is_a="s223:OutletConnectionPoint")
                .having("s223:hasRole", value="nawi:Role-Permeate")
                .follow("s223:hasProperty")
                .having("qudt:hasQuantityKind", value=quantity)
                .nodes(),
                f"permeate {quantity.split('/')[-1].lower()} for {module_uri}",
            )

        flow, pressure = permeate("qk:VolumeFlowRate"), permeate("qk:Pressure")
        modules.append(
            ModuleSignals(module_uri, float(area_m2), flow, unit_of(flow), pressure, unit_of(pressure))
        )
    return plant, sorted(modules, key=lambda m: m.module)


def _convert(client: Any, col: pl.Expr, frm: str, to: str) -> pl.Expr:
    """polars expression: `col` (in QUDT unit `frm`) expressed in unit `to`."""
    f = client.get_conversion_factors(frm, to)
    return (col + f["from_offset"]) * f["from_multiplier"] / f["to_multiplier"] - f["to_offset"]


def _latest_timestamp(client: Any, point_uri: str) -> datetime:
    return Graframe(client).nodes(point_uri).latest_data()["time"][0]


def assemble(client: Any, plant: PlantSignals, mod: ModuleSignals, start: Any, end: Any) -> pl.DataFrame:
    """One time-aligned frame per module: plant-wide feed + backwash, this module's permeate.

    A single ``.dataframe()`` call fetches every series and joins them on time.
    """
    points = {
        plant.feed_pressure: "feed_p",
        plant.feed_temp: "temp_c",
        plant.backwash_flow: "bw_gpm",
        mod.permeate_flow: "perm_flow",
        mod.permeate_pressure: "perm_p",
    }
    wide = Graframe(client).nodes(*points).dataframe(shape="wide", start=start, end=end)
    return wide.rename({client.compact_uri(uri): name for uri, name in points.items()})


def normalize(client: Any, plant: PlantSignals, mod: ModuleSignals, df: pl.DataFrame) -> pl.DataFrame:
    """TMP, specific flux and the resistance-in-series fouling model (Gu et al. 2018,
    Desalination 431:86-99): ``R_t = TMP / (mu(T) * J)``. Masked to genuine filtration.
    """
    df = df.with_columns(
        [
            _convert(client, pl.col("feed_p"), plant.feed_pressure_unit, "PA").alias("feed_p_pa"),
            _convert(client, pl.col("perm_p"), mod.permeate_pressure_unit, "PA").alias("perm_p_pa"),
            (_convert(client, pl.col("perm_flow"), mod.permeate_flow_unit, "M3-PER-SEC") / mod.area_m2).alias(
                "jv_m_s"
            ),
        ]
    )
    df = df.with_columns((pl.col("feed_p_pa") - pl.col("perm_p_pa")).alias("tmp_pa"))
    df = df.with_columns(_viscosity_pas(pl.col("temp_c")).alias("mu_pas"))
    df = df.with_columns(
        [
            (pl.col("tmp_pa") / 1e5).alias("tmp_bar"),
            (pl.col("jv_m_s") * 3.6e6).alias("flux_lmh"),
            (pl.col("tmp_pa") / (pl.col("mu_pas") * pl.col("jv_m_s"))).alias("resistance_per_m"),
            ((pl.col("jv_m_s") > 1e-6) & (pl.col("tmp_pa") > 1e4)).alias("filtering"),
        ]
    )
    return df.filter(pl.col("filtering") & pl.col("resistance_per_m").is_finite()).sort("time")


def assign_cycles(full_df: pl.DataFrame, op: pl.DataFrame, bw_on_gpm: float) -> pl.DataFrame:
    """Number filtration cycles by backwash onset (rising edge on `bw_gpm`); tag each
    operating sample with its cycle."""
    d = full_df.sort("time").with_columns((pl.col("bw_gpm") > bw_on_gpm).fill_null(False).alias("bw_on"))
    d = d.with_columns((pl.col("bw_on") & ~pl.col("bw_on").shift(1, fill_value=False)).alias("onset"))
    d = d.with_columns(pl.col("onset").cast(pl.Int32).cum_sum().alias("cycle"))
    return op.join(d.select(["time", "cycle"]), on="time", how="left").drop_nulls(["cycle"])


def fouling_rates(op: pl.DataFrame, min_cycle_min: float, trim_s: float) -> pl.DataFrame:
    """Linear fit of resistance vs elapsed time within each real cycle -> fouling rate
    (m^-1/hr, positive = declining permeability). Drops short backwash-transition
    fragments and trims the settling period at the start of each cycle."""
    rows = []
    for cyc, part in op.group_by("cycle", maintain_order=True):
        part = part.sort("time")
        t0 = part["time"][0]
        secs = (part["time"] - t0).dt.total_seconds().to_numpy()
        if secs[-1] < min_cycle_min * 60:
            continue
        keep = secs >= trim_s
        if keep.sum() < 60:
            continue
        hrs = secs[keep] / 3600.0
        y = part["resistance_per_m"].to_numpy()[keep]
        slope, _ = np.polyfit(hrs, y, 1)
        cycle_id = cyc[0] if isinstance(cyc, tuple) else cyc
        rows.append(
            {"cycle": int(cycle_id), "end": part["time"][-1], "fouling_rate": float(slope), "resistance": float(y[-1])}
        )
    return pl.DataFrame(rows).sort("end")


def flag_fouling(cycles: pl.DataFrame, z_alarm: float, cip_resistance_per_m: float) -> pl.DataFrame:
    """Flag each cycle whose fouling rate is anomalous against *prior* cycles only
    (an expanding, backward-looking z-score -- no lookahead), or whose resistance has
    already crossed the CIP-equivalent threshold (Gu et al.: ``R ~ 9.63e12 m^-1`` at
    TMP ~ 100 kPa)."""
    rate_mag = cycles["fouling_rate"].abs().to_numpy()
    resistance = cycles["resistance"].to_numpy()
    z = np.zeros(len(rate_mag))
    for i in range(3, len(rate_mag)):
        history = rate_mag[:i]
        sd = history.std()
        z[i] = (rate_mag[i] - history.mean()) / sd if sd else 0.0
    detected = (z > z_alarm) | (resistance > cip_resistance_per_m)
    return cycles.with_columns(
        [pl.Series("z", z), pl.Series("fouling_detected", detected.astype(float))]
    )


def derived_uri(module_uri: str, suffix: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in module_uri).strip("_")
    return f"urn:acquirium:point#{safe}_{suffix}"


def _rows(df: pl.DataFrame, col: str) -> list[tuple[datetime, float]]:
    return list(zip(df["time"].to_list(), df[col].to_list()))


_QUDT_QK = "http://qudt.org/vocab/quantitykind/"
_QUDT_UNIT = "http://qudt.org/vocab/unit/"


@dataclass(frozen=True)
class DerivedStream:
    """One derived-stream column, its output suffix, and its QUDT metadata.

    ``quantity_kind``/``unit`` are explicit full QUDT URIs (never resolved by
    fuzzy text matching) -- left ``None`` where this branch's own bundled QUDT
    vocabulary has no dimensionally-correct concept, rather than tag a stream
    with a plausible-looking but wrong unit/quantity kind.
    """

    suffix: str
    column: str
    quantity_kind: str | None
    unit: str | None
    label: str


DERIVED_STREAMS: tuple[DerivedStream, ...] = (
    DerivedStream("tmp_bar", "tmp_bar", _QUDT_QK + "Pressure", _QUDT_UNIT + "BAR", "Transmembrane pressure (TMP)"),
    DerivedStream(
        "flux_lmh", "flux_lmh", _QUDT_QK + "VolumetricFlux", None,
        "Specific flux, LMH = L*m^-2*h^-1 (no exact QUDT unit in this vocabulary)",
    ),
    DerivedStream(
        "resistance_per_m", "resistance_per_m", _QUDT_QK + "InverseLength", _QUDT_UNIT + "PER-M",
        "Hydraulic resistance (resistance-in-series model, Gu et al. 2018)",
    ),
    DerivedStream(
        "fouling_rate_per_m_per_hr", "fouling_rate", None, None,
        "Per-cycle fouling rate: resistance rise per hour (no QUDT quantity kind for a resistance rate)",
    ),
    DerivedStream(
        "fouling_detected", "fouling_detected", None, _QUDT_UNIT + "UNITLESS",
        "Fouling-detected flag: 1 if the latest cycle's fouling rate is anomalous "
        "against its history, or resistance has crossed the CIP threshold",
    ),
)


_S223_QUANTIFIABLE_OBSERVABLE_PROPERTY = "http://data.ashrae.org/standard223#QuantifiableObservableProperty"


def _annotate_derived_points(client: Any, module_uris: list[str]) -> None:
    """Attach rdfs:label/qudt:hasQuantityKind/qudt:hasUnit to every derived point.

    Streams that carry a real quantity kind are also typed
    ``s223:QuantifiableObservableProperty`` -- the same class raw signals use --
    so they're discoverable through the identical facet-API pattern
    (``instances("s223:QuantifiableObservableProperty").having("qudt:hasQuantityKind", ...)``),
    not just readable by URI. Idempotent (re-inserting the same triples is a
    no-op), so it is safe to call on every run rather than only at registration.
    """
    from rdflib import RDF, Graph, Literal, URIRef
    from rdflib.namespace import RDFS

    QUDT = "http://qudt.org/schema/qudt/"
    g = Graph()
    for module_uri in module_uris:
        for stream in DERIVED_STREAMS:
            subj = URIRef(derived_uri(module_uri, stream.suffix))
            g.add((subj, RDFS.label, Literal(stream.label)))
            if stream.quantity_kind:
                g.add((subj, RDF.type, URIRef(_S223_QUANTIFIABLE_OBSERVABLE_PROPERTY)))
                g.add((subj, URIRef(QUDT + "hasQuantityKind"), URIRef(stream.quantity_kind)))
            if stream.unit:
                g.add((subj, URIRef(QUDT + "hasUnit"), URIRef(stream.unit)))
    client.insert_graph(g.serialize(format="turtle"), format="turtle", replace=False)


class UFMembraneFoulingApp(App):
    """UF membrane-fouling soft sensor, built entirely on the Graframe facet API.

    Discovers every UF module and its feed/permeate signals by meaning (quantity
    kind + connection-point role -- no hardcoded SCADA tags or module URIs), then
    runs the resistance-in-series fouling model from Gu et al. 2018 (Desalination
    431:86-99). Emits the intermediate variables (TMP, specific flux, hydraulic
    resistance) as derived streams, plus a per-cycle fouling rate and a
    ``fouling_detected`` flag for each membrane.
    """

    name = "uf_membrane_fouling"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"

    def __init__(
        self,
        *,
        lookback_hours: float = 24.0,
        bw_on_gpm: float = 1.0,
        min_cycle_min: float = 10.0,
        trim_s: float = 60.0,
        z_alarm: float = 3.0,
        cip_resistance_per_m: float = 9.63e12,
    ) -> None:
        self.lookback_hours = float(lookback_hours)
        self.bw_on_gpm = float(bw_on_gpm)
        self.min_cycle_min = float(min_cycle_min)
        self.trim_s = float(trim_s)
        self.z_alarm = float(z_alarm)
        self.cip_resistance_per_m = float(cip_resistance_per_m)

    def build_query(self, aq: Any):
        # Keep app registration compatible with the existing Query serializer.
        # Graframe discovery is performed in run() from this query's client.
        return aq.query()

    def run(self, ctx: AppContext) -> list[Output]:
        if ctx.query is None:
            return []
        client = ctx.query.client
        params = ctx.params or {}
        bw_on_gpm = float(params.get("bw_on_gpm", self.bw_on_gpm))
        min_cycle_min = float(params.get("min_cycle_min", self.min_cycle_min))
        trim_s = float(params.get("trim_s", self.trim_s))
        z_alarm = float(params.get("z_alarm", self.z_alarm))
        cip_resistance_per_m = float(params.get("cip_resistance_per_m", self.cip_resistance_per_m))
        lookback_hours = float(params.get("lookback_hours", self.lookback_hours))

        plant, modules = discover(client)
        if not modules:
            return []

        # Anchor "now" on the data itself: works unchanged for a live deployment
        # (latest sample ~= wall clock) and for a historical replay (latest sample
        # is wherever the archive ends).
        end = ctx.end or _latest_timestamp(client, plant.feed_pressure)
        start = ctx.start or (end - timedelta(hours=lookback_hours))

        outputs: list[Output] = []
        annotated_modules: list[str] = []
        for mod in modules:
            wide = assemble(client, plant, mod, start, end)
            if wide.is_empty():
                continue
            op = normalize(client, plant, mod, wide)
            if op.is_empty():
                continue
            tagged = assign_cycles(wide, op, bw_on_gpm)
            if tagged.is_empty():
                continue
            cycles = fouling_rates(tagged, min_cycle_min, trim_s)
            if cycles.is_empty():
                continue
            flagged = flag_fouling(cycles, z_alarm, cip_resistance_per_m).rename({"end": "time"})

            frames = {"tmp_bar": tagged, "flux_lmh": tagged, "resistance_per_m": tagged,
                      "fouling_rate": flagged, "fouling_detected": flagged}
            for stream in DERIVED_STREAMS:
                outputs.append(
                    Output.timeseries(
                        point_uri=derived_uri(mod.module, stream.suffix),
                        rows=_rows(frames[stream.column], stream.column),
                    )
                )
            annotated_modules.append(mod.module)

        if annotated_modules:
            _annotate_derived_points(client, annotated_modules)
        return outputs


def _run_and_emit(app: UFMembraneFoulingApp, ctx: AppContext, acq: Any, *, dry_run: bool, logger: Any) -> int:
    """One run() + persist cycle; returns the number of output streams produced."""
    outputs = app.run(ctx)
    logger.info("computed %d derived-stream outputs", len(outputs))
    if dry_run:
        for out in outputs:
            print(out.payload["point_uri"], len(out.payload["rows"]), "rows")
    else:
        emit_outputs(ctx.app_id, outputs, insert_timeseries=acq.client.insert_timeseries, logger=logger)
    return len(outputs)


def main() -> None:
    """Run this soft sensor directly against a running Acquirium server.

    ``python -m acquirium.Apps.uf_membrane_fouling`` -- no server-side App
    registration or Docker required; this connects like any other client and
    persists derived streams the same way the App runtime would. Pass
    ``--keep-alive`` to keep re-running on an interval instead of exiting after
    one pass.
    """
    import argparse
    import logging
    import time

    from acquirium import Acquirium

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--server-url", default="localhost")
    parser.add_argument("--server-port", type=int, default=8000)
    parser.add_argument("--use-ssl", action="store_true")
    parser.add_argument("--start", default=None, help="ISO 8601 (default: --end minus --lookback-hours)")
    parser.add_argument("--end", default=None, help="ISO 8601 (default: the latest sample on the graph)")
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument("--bw-on-gpm", type=float, default=1.0)
    parser.add_argument("--min-cycle-min", type=float, default=10.0)
    parser.add_argument("--trim-s", type=float, default=60.0)
    parser.add_argument("--z-alarm", type=float, default=3.0)
    parser.add_argument("--cip-resistance-per-m", type=float, default=9.63e12)
    parser.add_argument(
        "--app-id",
        default=None,
        help=(
            "source_id under which derived streams are persisted (default: the App's "
            "name). A point URI is bound to whichever app_id first inserts it, so reuse "
            "the same --app-id across runs against the same server."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="compute and print outputs but don't persist them")
    parser.add_argument(
        "--keep-alive", action="store_true", help="loop forever, re-running every --interval seconds (Ctrl-C to stop)"
    )
    parser.add_argument("--interval", type=float, default=30.0, help="seconds between runs in --keep-alive mode")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("acquirium.uf_membrane_fouling")

    if args.keep_alive and (args.start or args.end):
        logger.warning(
            "--keep-alive with a fixed --start/--end recomputes the same window every tick; "
            "omit --start/--end to anchor each tick on the latest available data instead"
        )

    acq = Acquirium(server_url=args.server_url, server_port=args.server_port, use_ssl=args.use_ssl)
    app = UFMembraneFoulingApp(
        lookback_hours=args.lookback_hours,
        bw_on_gpm=args.bw_on_gpm,
        min_cycle_min=args.min_cycle_min,
        trim_s=args.trim_s,
        z_alarm=args.z_alarm,
        cip_resistance_per_m=args.cip_resistance_per_m,
    )
    ctx = AppContext(
        app_id=args.app_id or app.name,
        started_at=datetime.now(timezone.utc),
        start=datetime.fromisoformat(args.start) if args.start else None,
        end=datetime.fromisoformat(args.end) if args.end else None,
        query=app.build_query(acq),
        params={},
    )

    if not args.keep_alive:
        _run_and_emit(app, ctx, acq, dry_run=args.dry_run, logger=logger)
        return

    logger.info("keep-alive mode: running every %.1fs (Ctrl-C to stop)", args.interval)
    run_count = 0
    try:
        while True:
            run_count += 1
            started = time.monotonic()
            try:
                _run_and_emit(app, ctx, acq, dry_run=args.dry_run, logger=logger)
            except Exception:
                logger.exception("run #%d failed; will retry after interval", run_count)
            elapsed = time.monotonic() - started
            time.sleep(max(args.interval - elapsed, 0.0))
    except KeyboardInterrupt:
        logger.info("stopped after %d run(s)", run_count)


if __name__ == "__main__":
    main()

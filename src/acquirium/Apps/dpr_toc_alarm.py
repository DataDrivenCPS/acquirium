from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import polars as pl

from acquirium.Apps.base import App, Output
from acquirium.Graframe import Graframe, Profile
from acquirium.internals.models import AppContext


@dataclass(frozen=True)
class TocPoint:
    point_uri: str
    sensor_uri: str | None = None
    location_uri: str | None = None
    label: str | None = None


@dataclass(frozen=True)
class AlarmRule:
    code: str
    threshold_mg_l: float
    duration: timedelta
    intervention: str
    severity: str = "WARNING"


@dataclass(frozen=True)
class AlarmEvaluation:
    rule: AlarmRule
    exceeded_since: datetime
    latest_time: datetime
    latest_value: float


RO_RULES = (
    AlarmRule(
        code="dpr_ro_toc_tthmfp",
        threshold_mg_l=0.1,
        duration=timedelta(hours=24),
        intervention=(
            "Collect a grab sample and perform a 5-day TTHMFP study "
            "(CA DPR §64669.50(j)(2))."
        ),
    ),
    AlarmRule(
        code="dpr_ro_toc_integrity",
        threshold_mg_l=0.15,
        duration=timedelta(hours=120),
        intervention=(
            "Investigate RO integrity, run a conductivity profile to identify "
            "the underperforming vessel/element, and take corrective action "
            "(CA DPR §64669.50(j)(1))."
        ),
        severity="CRITICAL",
    ),
)


def post_aop_rules(wwc: float) -> tuple[AlarmRule, ...]:
    critical = 0.5 / wwc
    return (
        AlarmRule(
            code="dpr_post_aop_toc_half_limit",
            threshold_mg_l=critical / 2.0,
            duration=timedelta(minutes=60),
            intervention=(
                "Evaluate the treatment system, initiate source-control "
                "investigation, collect lab samples, and report in the monthly "
                "compliance report (CA DPR §64669.50(n)(4))."
            ),
        ),
        AlarmRule(
            code="dpr_post_aop_toc_critical_limit",
            threshold_mg_l=critical,
            duration=timedelta(0),
            intervention=(
                "Immediately discontinue delivery and notify the State Board "
                "and receiving public water systems within 24 hours "
                "(CA DPR §64669.50(n)(3))."
            ),
            severity="CRITICAL",
        ),
    )


class DprTocAlarmApp(App):
    """CA DPR TOC monitor that emits recommended interventions as text streams."""

    name = "dpr_toc_alarm"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"

    def __init__(
        self,
        *,
        wwc: float = 1.0,
        lookback_hours: float = 7 * 24,
        monitor_unclassified: bool = False,
    ) -> None:
        self.wwc = float(wwc)
        self.lookback_hours = float(lookback_hours)
        self.monitor_unclassified = bool(monitor_unclassified)

    def build_query(self, aq: Any):
        # Keep app registration compatible with the existing Query serializer.
        # Graframe discovery is performed in run() from this query's client.
        return aq.query()

    def run(self, ctx: AppContext) -> list[Output]:
        if ctx.query is None:
            return []
        if self.wwc <= 0:
            raise ValueError("wwc must be greater than zero")

        params = ctx.params or {}
        wwc = float(params.get("wwc", self.wwc))
        lookback_hours = float(params.get("lookback_hours", self.lookback_hours))
        monitor_unclassified = bool(
            params.get("monitor_unclassified", self.monitor_unclassified)
        )
        end = ctx.end or datetime.now(timezone.utc)
        start = ctx.start or (end - timedelta(hours=lookback_hours))

        points = discover_toc_points(ctx.query.client)
        if not points:
            return []

        point_by_uri = {p.point_uri: p for p in points}
        selection = _toc_point_selection(ctx.query.client, [p.point_uri for p in points])
        df = selection.data(start=start, end=end, order="asc").dataframe(shape="narrow")
        if df.is_empty():
            return []

        outputs: list[Output] = []
        for point_uri, point_df in _iter_point_frames(df):
            toc_point = point_by_uri.get(point_uri) or TocPoint(point_uri=point_uri)
            rules = rules_for_point(toc_point, wwc, monitor_unclassified)
            if not rules:
                continue
            for evaluation in evaluate_rules(point_df, rules):
                stream_uri = intervention_stream_uri(point_uri, evaluation.rule.code)
                outputs.append(
                    Output.text_timeseries(
                        point_uri=stream_uri,
                        rows=[
                            (
                                evaluation.latest_time,
                                _intervention_value(toc_point, evaluation),
                            )
                        ],
                    )
                )
        return outputs


def discover_toc_points(client: Any) -> list[TocPoint]:
    """Discover TOC observable properties with the new Graframe facet API."""
    g = Graframe(client, profile=Profile.base())

    sensor_points = (
        g.instances("s223:Sensor")
        .mark("sensor")
        .follow("s223:observes")
        .mark("point")
        .having("s223:ofSubstance", value="watr:Constituent-Organics")
    )

    # Touch facets intentionally: this app uses the branch's facet surface to
    # discover the graph shape before traversing the named virtual edge.
    sensor_points.to("sensor").facets(direction="out", only=["s223:observes"])

    rows = _selection_rows(
        sensor_points,
        "point",
        "sensor",
        label_query=True,
        location_query=True,
    )

    if not rows:
        class_points = (
            g.instances("watr:TotalOrganicCompoundConcentrationSensor")
            .mark("sensor")
            .follow("s223:observes")
            .mark("point")
        )
        class_points.to("sensor").facets(direction="out", only=["s223:observes"])
        rows = _selection_rows(
            class_points,
            "point",
            "sensor",
            label_query=True,
            location_query=True,
        )

    if not rows:
        property_points = (
            g.instances("s223:QuantifiableObservableProperty")
            .having("s223:ofSubstance", value="watr:Constituent-Organics")
            .having("qudt:hasQuantityKind", value="qk:Concentration")
            .mark("point")
        )
        property_points.facets(direction="in", only=["s223:observes"])
        rows = _selection_rows(
            property_points,
            "point",
            label_query=True,
            location_query=False,
        )

    seen: set[str] = set()
    points: list[TocPoint] = []
    for row in rows:
        point = row.get("point")
        if not point or point in seen:
            continue
        seen.add(point)
        points.append(
            TocPoint(
                point_uri=point,
                sensor_uri=row.get("sensor"),
                location_uri=row.get("location"),
                label=row.get("label"),
            )
        )
    return points


def _selection_rows(
    selection: Any,
    *columns: str,
    label_query: bool,
    location_query: bool,
) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    selected = selection.select(*columns, compact=False)
    for values in selected.iter_rows(named=True):
        row = {k: (str(v) if v is not None else None) for k, v in values.items()}
        rows.append(row)

    if not rows:
        return rows

    point_uris = [r["point"] for r in rows if r.get("point")]
    labels = _labels(selection.client, point_uris) if label_query else {}
    locations = _sensor_locations(selection.client, rows) if location_query else {}
    for row in rows:
        point = row.get("point")
        sensor = row.get("sensor")
        if point:
            row["label"] = labels.get(point)
        if sensor:
            row["location"] = locations.get(sensor)
    return rows


def _labels(client: Any, uris: Iterable[str]) -> dict[str, str]:
    values = _values("p", sorted(set(uris)))
    if not values:
        return {}
    query = (
        f"SELECT ?p ?label\nWHERE {{\n  {values}\n"
        "  OPTIONAL { ?p <http://www.w3.org/2000/01/rdf-schema#label> ?label . }\n}"
    )
    res = client.sparql_query(query, use_union=True)
    out: dict[str, str] = {}
    for row in res.get("rows", []):
        if len(row) >= 2 and row[0] and row[1]:
            out[str(row[0])] = str(row[1])
    return out


def _sensor_locations(client: Any, rows: list[dict[str, str | None]]) -> dict[str, str]:
    sensors = sorted({r["sensor"] for r in rows if r.get("sensor")})
    values = _values("sensor", sensors)
    if not values:
        return {}
    query = (
        f"SELECT ?sensor ?location\nWHERE {{\n  {values}\n"
        "  OPTIONAL { ?sensor <http://data.ashrae.org/standard223#hasObservationLocation> ?location . }\n}"
    )
    res = client.sparql_query(query, use_union=True)
    out: dict[str, str] = {}
    for row in res.get("rows", []):
        if len(row) >= 2 and row[0] and row[1]:
            out[str(row[0])] = str(row[1])
    return out


def _values(var: str, uris: Iterable[str]) -> str:
    items = [f"<{u}>" for u in uris]
    if not items:
        return ""
    return f"VALUES ?{var} {{ {' '.join(items)} }}"


def _toc_point_selection(client: Any, point_uris: list[str]):
    return Graframe(client).nodes(*point_uris)


def _iter_point_frames(df: pl.DataFrame) -> Iterable[tuple[str, pl.DataFrame]]:
    point_col = "point_uri" if "point_uri" in df.columns else "point_id"
    for point_uri in df[point_col].drop_nulls().unique().sort().to_list():
        yield str(point_uri), df.filter(pl.col(point_col) == point_uri)


def rules_for_point(
    point: TocPoint, wwc: float, monitor_unclassified: bool = False
) -> tuple[AlarmRule, ...]:
    text = " ".join(
        x.lower()
        for x in (point.point_uri, point.sensor_uri, point.location_uri, point.label)
        if x
    )
    is_ro = "reverseosmosis" in text or "reverse-osmosis" in text or "-ro-" in text
    is_ro = is_ro or ("ro" in _tokens(text) and "toc" in text)
    is_post_aop = any(
        token in text
        for token in (
            "post-aop",
            "postaop",
            "after-aop",
            "distribution",
            "potable-effluent",
            "finished-water",
            "finished_water",
        )
    )
    # TODO: replace URI/label heuristics with explicit control-point roles in
    # the graph, e.g. RO permeate TOC vs post-AOP distribution control point.
    if is_ro:
        return RO_RULES
    if is_post_aop or monitor_unclassified:
        return post_aop_rules(wwc)
    return ()


def _tokens(text: str) -> set[str]:
    normalized = "".join(ch.lower() if ch.isalnum() else " " for ch in text)
    return set(normalized.split())


def evaluate_rules(df: pl.DataFrame, rules: Iterable[AlarmRule]) -> list[AlarmEvaluation]:
    if df.is_empty() or "value_numeric" not in df.columns:
        return []
    rows = (
        df.select("time", "value_numeric")
        .drop_nulls()
        .sort("time")
        .iter_rows(named=True)
    )
    samples = [(r["time"], float(r["value_numeric"])) for r in rows]
    if not samples:
        return []
    return [
        evaluation
        for rule in rules
        if (evaluation := evaluate_rule(samples, rule)) is not None
    ]


def evaluate_rule(
    samples: list[tuple[datetime, float]], rule: AlarmRule
) -> AlarmEvaluation | None:
    streak_start: datetime | None = None
    latest_time: datetime | None = None
    latest_value: float | None = None
    for ts, value in sorted(samples, key=lambda item: item[0]):
        if value > rule.threshold_mg_l:
            if streak_start is None:
                streak_start = ts
            latest_time = ts
            latest_value = value
        else:
            streak_start = None
            latest_time = None
            latest_value = None

    if streak_start is None or latest_time is None or latest_value is None:
        return None
    elapsed = latest_time - streak_start
    if rule.duration == timedelta(0):
        pass
    elif elapsed <= rule.duration:
        return None
    return AlarmEvaluation(
        rule=rule,
        exceeded_since=streak_start,
        latest_time=latest_time,
        latest_value=latest_value,
    )


def intervention_stream_uri(point_uri: str, rule_code: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in point_uri).strip("_")
    return f"urn:acquirium:point#{safe}_{rule_code}_intervention"


def _intervention_value(point: TocPoint, evaluation: AlarmEvaluation) -> str:
    return (
        f"{evaluation.rule.code}: TOC {evaluation.latest_value:g} mg/L exceeded "
        f"{evaluation.rule.threshold_mg_l:g} mg/L since "
        f"{evaluation.exceeded_since.isoformat()}. "
        f"{evaluation.rule.intervention}"
    )

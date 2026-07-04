"""Builds membrane_fouling.ipynb. Run: uv run python notebooks/port_hueneme/_build_notebook.py"""
import json, pathlib

cells = []
def md(*lines): cells.append({"cell_type":"markdown","id":f"cell-{len(cells)}","metadata":{},"source":list(_join(lines))})
def code(*lines): cells.append({"cell_type":"code","id":f"cell-{len(cells)}","metadata":{},"execution_count":None,"outputs":[],"source":list(_join(lines))})
def _join(lines):
    text = "\n".join(lines)
    # nbformat wants a list of strings each ending in \n (except maybe last)
    parts = text.split("\n")
    return [p + ("\n" if i < len(parts)-1 else "") for i,p in enumerate(parts)]

md(
"# UF membrane-fouling detection — via the Graframe facet interface",
"",
"This notebook pulls the signals a UF fouling algorithm needs **entirely through the",
"faceted query API** (`aq.graph()`) against the Port Hueneme UF/RO model, then runs the",
"four-step algorithm from Gu et al. 2018 (*Desalination* 431:86-99), the paper behind this",
"dataset.",
"",
"| step | what | how here |",
"|---|---|---|",
"| 1 | normalized permeability | `specific_flux = permeate_flow / area / TMP`, ×`exp(-0.0239·(T-20))` viscosity correction |",
"| 2 | detect cleaning events | backwash-flow onsets (with a first-difference fallback) |",
"| 3 | per-cycle fouling rate | linear fit of normalized permeability vs time between backwashes |",
"| 4 | flag + trend | z-score of the latest cycle's slope; track shrinking cycle length |",
"",
"> ⚠️ **The SCADA tag→role mapping in the model is a best guess** (the paper has no legend).",
"> Treat the numbers as illustrative until the mapping is locked (see the closing note).",
)

md("## Connect + curate a discovery profile")
code(
"import numpy as np",
"import polars as pl",
"from acquirium import Acquirium",
"from acquirium.Graframe import Profile",
"",
"pl.Config.set_fmt_str_lengths(60)",
"pl.Config.set_tbl_rows(20)",
"",
"acq = Acquirium(server_url=\"localhost\", server_port=8000, use_ssl=False)",
"",
"# A profile keeps facets readable: show the water/223 predicates, hide the",
"# low-level connection plumbing, and name the paths we traverse a lot.",
"water = Profile.base().with_(",
"    allow=[\"s223:\", \"nawi:\", \"qudt:hasQuantityKind\", \"qudt:hasUnit\", \"s223:hasRole\"],",
"    deny=[\"s223:cnx\", \"s223:connected\", \"s223:connectedThrough\", \"s223:connectsAt\",",
"          \"s223:connectsThrough\", \"s223:connectsTo\", \"s223:connectsFrom\", \"s223:connectedFrom\"],",
"    edges={",
"        \"measures\": \"s223:hasProperty\",",
"        \"quantity\": \"s223:hasProperty/qudt:hasQuantityKind\",",
"    },",
")",
"g = acq.graph(profile=water)",
"print(\"graph version:\", acq.graph_version())",
)

md(
"## (Optional) load the model",
"",
"Skip this if you started the server with `deployments/PORT_HUENEME/acquirium.toml`",
"(the driver already inserted the model). Otherwise load it client-side:",
)
code(
"# acq.insert_graph(open(\"../../deployments/PORT_HUENEME/models/port-hueneme-uf.ttl\").read(),",
"#                  format=\"turtle\", replace=False)",
"# print(\"graph version:\", acq.graph_version())",
)

md(
"## Explore the UF system",
"",
"Start from the class and let `facets()` show what's reachable — no URI knowledge needed.",
)
code(
"uf = g.instances(\"nawi:UltrafiltrationUnit\")",
"print(\"UF modules:\", uf.count())",
"uf.frame()",
)
code(
"# What hangs off a UF module? (curated view + named edges)",
"uf.facets().show()",
)

md(
"## Discover the signals by *meaning* — no node names",
"",
"We must not type `UF1` or `feed-manifold`: those are model-authoring details a fouling",
"analyst wouldn't know. A data point is identified by **what it measures** (quantity kind)",
"and **where it sits** (the role of the connection point it hangs off). The inverse path",
"`^s223:hasProperty/s223:hasRole` walks a property back to its connection point and reads",
"its role — so `Role-Feed` vs `Role-Permeate` comes from the graph, not from us.",
)
code(
"props = g.instances(\"s223:QuantifiableObservableProperty\")",
"",
"# Identify a point by MEANING, written inline — no helper, no node names. A property's",
"# quantity kind says WHAT it measures; the role of the connection point it hangs off says",
"# WHERE it is. `^s223:hasProperty` is the inverse hop (property -> its CP) and `/s223:hasRole`",
"# reads that CP's role, so the chain below reads as \"a Pressure on a Feed-role point\":",
"feed_pressure = (props",
"    .having(\"qudt:hasQuantityKind\", value=\"qk:Pressure\")",
"    .having(\"^s223:hasProperty/s223:hasRole\", value=\"nawi:Role-Feed\"))",
"print(\"feed pressure    :\", feed_pressure.nodes())",
"",
"# same two lines, role flipped -> the permeate-side pressures (one per module):",
"print(\"permeate pressure:\",",
"      props.having(\"qudt:hasQuantityKind\", value=\"qk:Pressure\")",
"           .having(\"^s223:hasProperty/s223:hasRole\", value=\"nawi:Role-Permeate\").nodes())",
)
code(
"# It's just role + quantity kind — inspect the compiled SPARQL, no black box:",
"print(feed_pressure.to_sparql())",
)

md(
"## Pull the fouling signals",
"",
"Feed conditions are single points (found by role). For the permeate side we analyze **one",
"module** — and even *that* URI comes from `instances(...)`, not from us naming it. TMP pairs",
"the module's permeate pressure with the common feed pressure. Backwash flow isn't on a",
"role connection point, so we find it by the process its line performs (`Process-Backwashing`).",
)
code(
"# The modules come from the graph; pick one to analyze (swap the index or loop):",
"modules = sorted(g.instances(\"nawi:UltrafiltrationUnit\").nodes())",
"mod = g.nodes(modules[0])                       # its URI came from instances(), not typed",
"print(\"analyzing\", modules[0], \"of\", [m.rsplit('#', 1)[-1] for m in modules])",
"",
"# Every signal is the same inline facet chain; each comment says how it's pinned down.",
"selections = {",
"    # feed conditions (common to the plant): a quantity kind on a Feed-role connection point",
"    \"feed_p_psi\": props.having(\"qudt:hasQuantityKind\", value=\"qk:Pressure\")",
"                       .having(\"^s223:hasProperty/s223:hasRole\", value=\"nawi:Role-Feed\"),",
"    \"temp_c\":     props.having(\"qudt:hasQuantityKind\", value=\"qk:Temperature\")",
"                       .having(\"^s223:hasProperty/s223:hasRole\", value=\"nawi:Role-Feed\"),",
"    \"turb_ntu\":   props.having(\"qudt:hasQuantityKind\", value=\"qk:Turbidity\")",
"                       .having(\"^s223:hasProperty/s223:hasRole\", value=\"nawi:Role-Feed\"),",
"    # permeate side: walk out from the chosen module to its permeate connection point",
"    \"perm_gpm\":   mod.follow(\"s223:hasConnectionPoint\").having(\"s223:hasRole\", value=\"nawi:Role-Permeate\")",
"                     .follow(\"s223:hasProperty\").having(\"qudt:hasQuantityKind\", value=\"qk:VolumeFlowRate\"),",
"    \"filt_p_psi\": mod.follow(\"s223:hasConnectionPoint\").having(\"s223:hasRole\", value=\"nawi:Role-Permeate\")",
"                     .follow(\"s223:hasProperty\").having(\"qudt:hasQuantityKind\", value=\"qk:Pressure\"),",
"    # pump speed: only one AngularVelocity point, so the quantity kind alone pins it",
"    \"pump_rpm\":   props.having(\"qudt:hasQuantityKind\", value=\"qk:AngularVelocity\"),",
"    # backwash flow: not on a role CP -> find it by the process its line performs",
"    \"bw_gpm\":     props.having(\"qudt:hasQuantityKind\", value=\"qk:VolumeFlowRate\")",
"                       .having(\"^s223:hasProperty/nawi:hasProcess\", value=\"nawi:Process-Backwashing\"),",
"}",
"for name, sel in selections.items():",
"    print(f\"{name:12s} -> {sel.count()} point(s): {sel.nodes()}\")",
)
code(
"# Every selection compiles to SPARQL — inspect it, no black box:",
"print(selections[\"perm_gpm\"].to_sparql())",
)

md(
"### Fetch + align the timeseries",
"",
"Each selection is one data point, so `.dataframe(shape=\"wide\")` gives `time` + one value",
"column; we rename and outer-join on `time`. **Narrow the window** to one operating run",
"first — the raw record is 1 Hz over years.",
)
code(
"# A known continuously-operating day (2013-06-05); widen for more cycles, or set",
"# both to None to pull everything (large — 1 Hz over years).",
"START = \"2013-06-05T00:00:00Z\"",
"END   = \"2013-06-06T00:00:00Z\"",
"",
"def fetch(name, sel):",
"    df = sel.dataframe(shape=\"wide\", start=START, end=END)",
"    valcols = [c for c in df.columns if c != \"time\"]",
"    if not valcols:",
"        raise RuntimeError(f\"no data for {name} — is the driver ingesting, and is the window right?\")",
"    return df.select([\"time\", pl.col(valcols[0]).cast(pl.Float64).alias(name)])",
"",
"frames = [fetch(n, s) for n, s in selections.items()]",
"df = frames[0]",
"for f in frames[1:]:",
"    df = df.join(f, on=\"time\", how=\"full\", coalesce=True)",
"df = df.sort(\"time\")",
"print(df.shape)",
"df.head()",
)
code(
"# Membrane area is a static design value (s223:hasValue), not a stream. It lives on the",
"# membrane *element* the unit contains (s223:contains), not on the unit itself — so we",
"# hop through `contains/hasProperty` to reach it. modules[0] is the URI from instances().",
"area_q = acq.client.sparql_query(f'''",
"  SELECT ?v WHERE {{",
"    <{modules[0]}> <http://data.ashrae.org/standard223#contains>/<http://data.ashrae.org/standard223#hasProperty> ?p .",
"    ?p <http://qudt.org/schema/qudt/hasQuantityKind> <http://qudt.org/vocab/quantitykind/Area> ;",
"       <http://data.ashrae.org/standard223#hasValue> ?v . }}''')",
"AREA_M2 = float(area_q[\"rows\"][0][0]) if area_q[\"rows\"] else 50.0",
"print(\"membrane area (m^2):\", AREA_M2)",
)

md(
"## Step 1 — normalized permeability",
"",
"`TMP = feed pressure − filtrate pressure`; specific flux `= permeate_flow / area / TMP`;",
"normalize out temperature with the viscosity rule `× exp(-0.0239·(T-20))`. We keep only",
"genuine **filtration** samples (pump on, positive TMP and flux) — the raw record is full",
"of standby.",
)
code(
"PSI_TO_BAR = 0.0689476",
"GPM_TO_LPH = 3.785411784 * 60.0   # US gal/min -> L/h",
"",
"df = df.with_columns([",
"    ((pl.col(\"feed_p_psi\") - pl.col(\"filt_p_psi\")) * PSI_TO_BAR).alias(\"tmp_bar\"),",
"    (pl.col(\"perm_gpm\") * GPM_TO_LPH / AREA_M2).alias(\"flux_lmh\"),",
"])",
"df = df.with_columns([",
"    (pl.col(\"flux_lmh\") / pl.col(\"tmp_bar\")",
"     * (-0.0239 * (pl.col(\"temp_c\") - 20.0)).exp()).alias(\"norm_perm_lmh_bar\"),",
"    # filtration mask: pump running, sensible TMP + established forward flux",
"    ((pl.col(\"pump_rpm\") > 0.1) & (pl.col(\"tmp_bar\") > 0.1) & (pl.col(\"flux_lmh\") > 5.0)).alias(\"filtering\"),",
"])",
"op = df.filter(pl.col(\"filtering\") & pl.col(\"norm_perm_lmh_bar\").is_finite()).sort(\"time\")",
"print(f\"operating samples: {len(op)} / {len(df)} total\")",
"op.select([\"time\",\"temp_c\",\"tmp_bar\",\"flux_lmh\",\"norm_perm_lmh_bar\"]).head()",
)

md(
"## Step 2 — detect cleaning (backwash) events",
"",
"`bw_gpm` (discovered above by process, already joined into `df`) makes cleaning explicit:",
"a **backwash onset** ends a filtration cycle. We number cycles by cumulative onset count.",
)
code(
"BW_ON_GPM = 1.0   # backwash flow above this = backwash active",
"",
"d = (df.sort(\"time\")",
"       .with_columns((pl.col(\"bw_gpm\") > BW_ON_GPM).fill_null(False).alias(\"bw_on\")))",
"d = d.with_columns((pl.col(\"bw_on\") & ~pl.col(\"bw_on\").shift(1, fill_value=False)).alias(\"bw_onset\"))",
"d = d.with_columns(pl.col(\"bw_onset\").cast(pl.Int32).cum_sum().alias(\"cycle\"))",
"print(\"backwash onsets detected:\", int(d[\"bw_onset\"].sum()))",
"",
"op = op.join(d.select([\"time\", \"cycle\"]), on=\"time\", how=\"left\", coalesce=True).drop_nulls([\"cycle\"])",
"print(\"raw filtration cycles:\", op[\"cycle\"].n_unique())",
)

md(
"## Step 3 — per-cycle fouling rate",
"",
"Within each cycle, fit normalized permeability vs elapsed time; the slope is the fouling",
"rate (permeability *decline* → negative slope; magnitude per hour).",
"",
"Two guards matter here. The three modules are **backwashed sequentially**, so the shared",
"backwash-flow tag fires several onsets around each real cycle — we keep only cycles",
"`≥ MIN_CYCLE_MIN` (dropping the short transition fragments), and we **trim the first",
"`TRIM_S` seconds** of each cycle while TMP settles. On the reference day this yields ~25 min",
"cycles, matching Gu et al. (20-50 min).",
)
code(
"MIN_CYCLE_MIN = 10    # a real filtration cycle is at least this long",
"TRIM_S        = 60    # skip the first minute of each cycle (pressure settling)",
"",
"rows = []",
"for cyc, part in op.group_by(\"cycle\", maintain_order=True):",
"    part = part.sort(\"time\")",
"    t0 = part[\"time\"][0]",
"    secs = part.select(((pl.col(\"time\") - t0).dt.total_seconds()).alias(\"s\"))[\"s\"].to_numpy()",
"    if secs[-1] < MIN_CYCLE_MIN * 60:      # a backwash-transition fragment, not a cycle",
"        continue",
"    keep = secs >= TRIM_S",
"    if keep.sum() < 60:",
"        continue",
"    hrs = secs[keep] / 3600.0",
"    y = part[\"norm_perm_lmh_bar\"].to_numpy()[keep]",
"    slope, _ = np.polyfit(hrs, y, 1)        # LMH/bar per hour",
"    rows.append({",
"        \"cycle\": int(cyc[0]) if isinstance(cyc, tuple) else int(cyc),",
"        \"start\": t0,",
"        \"duration_min\": float(secs[-1] / 60.0),",
"        \"n\": int(keep.sum()),",
"        \"fouling_rate\": float(slope),        # signed (negative = declining permeability)",
"        \"perm_median\": float(np.median(y)),",
"    })",
"cycles = pl.DataFrame(rows).sort(\"start\")",
"print(f\"{len(cycles)} real cycles | median duration {cycles['duration_min'].median():.1f} min\")",
"cycles",
)

md(
"## Step 4 — flag anomalies + trend",
"",
"z-score the latest cycle's fouling rate against the history of prior cycles; also watch",
"whether cycles are getting **shorter** (fouling acceleration).",
)
code(
"cy = cycles.with_columns([",
"    pl.col(\"fouling_rate\").abs().alias(\"rate_mag\"),",
"])",
"mu, sd = cy[\"rate_mag\"][:-1].mean(), cy[\"rate_mag\"][:-1].std()",
"cy = cy.with_columns(((pl.col(\"rate_mag\") - mu) / (sd if sd else 1.0)).alias(\"z\"))",
"latest = cy.tail(1)",
"print(f\"history mean|rate|={mu:.3g}  std={sd:.3g}\")",
"print(f\"latest cycle z-score = {latest['z'][0]:.2f}\"",
"      f\"  -> {'ALARM: accelerated fouling' if abs(latest['z'][0])>3 else 'nominal'}\")",
"# cycle-length trend (shrinking = accelerating fouling)",
"if len(cy) >= 3:",
"    d = cy.select(((pl.col('start') - cy['start'][0]).dt.total_seconds()/3600).alias('h'))['h'].to_numpy()",
"    trend = np.polyfit(d, cy['duration_min'].to_numpy(), 1)[0]",
"    print(f\"cycle-length trend: {trend:+.3f} min per hour of operation\",",
"          '(shrinking!)' if trend < 0 else '')",
"cy",
)

md("## Visualize")
code(
"import matplotlib.pyplot as plt",
"fig, ax = plt.subplots(3, 1, figsize=(11, 9), constrained_layout=True)",
"",
"# (1) normalized permeability over time, colored by cycle",
"t = op[\"time\"].to_numpy(); y = op[\"norm_perm_lmh_bar\"].to_numpy(); c = op[\"cycle\"].to_numpy()",
"ax[0].scatter(t, y, c=c, cmap=\"tab20\", s=3)",
"ax[0].set_title(\"Step 1-2: temperature-normalized permeability, per filtration cycle\")",
"ax[0].set_ylabel(\"LMH/bar\")",
"",
"# (2) per-cycle fouling rate with mean +/- std band",
"ax[1].plot(cy[\"start\"].to_numpy(), cy[\"rate_mag\"].to_numpy(), \"o-\")",
"ax[1].axhline(mu, color=\"grey\", ls=\"--\", label=\"hist mean\")",
"ax[1].axhspan(max(mu-3*sd,0), mu+3*sd, color=\"grey\", alpha=0.15, label=\"±3σ\")",
"ax[1].set_title(\"Step 3-4: per-cycle fouling rate |slope| (alarm if outside band)\")",
"ax[1].set_ylabel(\"|LMH/bar/h|\"); ax[1].legend()",
"",
"# (3) cycle length over time",
"ax[2].plot(cy[\"start\"].to_numpy(), cy[\"duration_min\"].to_numpy(), \"s-\", color=\"C3\")",
"ax[2].set_title(\"Cycle length (shrinking = fouling acceleration)\")",
"ax[2].set_ylabel(\"minutes\"); ax[2].set_xlabel(\"time\")",
"plt.show()",
)

md(
"## Notes & caveats",
"",
"- **Nothing above names a node.** Each signal is a short inline `having`/`follow` chain that",
"  picks a point by *what it means* — its quantity kind plus the role of the connection point",
"  it hangs off (the inverse path `^s223:hasProperty/s223:hasRole`), or the process its line",
"  performs. The module comes from `instances(...)`. No `UF1`/`feed-manifold` URIs, no SCADA",
"  tag names — change `modules[0]` to loop over the others.",
"- **The tag→role mapping is unverified.** Before trusting absolute fouling rates, lock it:",
"  for each candidate (feed-PT, filtrate-PT, permeate-FT) combination, check that",
"  `R_t = ΔP/(μ·J)` reproduces the paper's sawtooth (Gu et al. Fig. 5; `R_t ≈ 2.0-2.6×10¹² m⁻¹`,",
"  TMP rising to ≤100 kPa then resetting at backwash).",
"- The paper's resistance-in-series form is equivalent: `FR_n = ΔR_T/Δt` (Step 3) and",
"  post-backwash resistance `R_PB` rising toward the CIP threshold (~100 kPa) is the Step-4 trend.",
)

nb = {"cells": cells,
      "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                   "language_info": {"name": "python"}},
      "nbformat": 4, "nbformat_minor": 5}
out = pathlib.Path(__file__).parent / "membrane_fouling.ipynb"
out.write_text(json.dumps(nb, indent=1))
print("wrote", out, "with", len(cells), "cells")

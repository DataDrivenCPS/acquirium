# Port Hueneme UF/RO pilot — WaTr model + SCADA ingest

A WaTr (NAWI water ontology) model of the NAVFAC-EXWC integrated **ultrafiltration /
reverse-osmosis** seawater desalination pilot at Port Hueneme, CA, plus a driver
that ingests its raw SCADA record. Target use: **UF membrane-fouling detection**
(normalized permeability → cleaning-event detection → per-cycle fouling-rate fit
→ trend/alarm).

## Provenance

- **Data:** Zenodo record [`4630769`](https://zenodo.org/records/4630769) =
  Dryad [`10.5068/D1310B`](https://datadryad.org/dataset/doi:10.5068/D1310B),
  **CC0**. ~3 years (422 operating days, 2012–2015) of 1 Hz SCADA data, a 340 MB
  `.rar` (~17 GB uncompressed) of daily `.csv`/`.xlsx` files under
  `data in UF-RO system/{training,test}/`.
  **The `.rar` needs `unar`** (`brew install unar`); p7zip and bsdtar fail on the codec.
- **System description & P&ID:** Gu et al. 2018, *Desalination* 431:86–99 (Fig. 1).
  The paper's resistance-in-series model is exactly the fouling algorithm:
  `J_F = ΔP/(μ·R_t)`, per-cycle fouling rate `FR_n = ΔR_T/Δt`, post-backwash
  resistance `R_PB` → time-to-CIP. CIP at TMP ≈ 100 kPa (`R ≈ 9.63×10¹² m⁻¹`).

## System (`models/port-hueneme-uf.ttl`)

```
feed seawater → LP pump (VFD) → prefilter → inline coagulant
   → [ UF1 ‖ UF2 ‖ UF3 ]  (PES multibore hollow-fiber, 0.02 µm, 50 m² each)
   → filtrate header → RO holding tank → HP pump → carbon filters
   → RO (3× SW30HRLE-400 in series) → product + brine
Backwash: RO concentrate/permeate, pulsed via accumulators; modules backwashed individually.
```

Modeled with `nawi:UltrafiltrationUnit` / `nawi:ReverseOsmosisMembrane` (each with
`nawi:hasProcess`), `s223:` connection points carrying `s223:hasRole`
(`nawi:Role-Feed` / `-Permeate` / `-Backwash`), and `s223:QuantifiableObservableProperty`
for each measurement. Each UF unit **`s223:contains` a membrane element**
(`nawi:UltrafiltrationMembrane`) that carries the membrane area as a static
`s223:hasValue` (50 m²) — area is a property of the membrane, not the unit.
Validated with rdflib.

**`models/water-membrane-ext.ttl`** is a small extension ontology that adds
`nawi:Membrane` / `nawi:UltrafiltrationMembrane` (physical membrane elements,
`rdfs:subClassOf s223:Equipment`) to the water ontology *without editing the
upstream file* — the existing watr membrane classes are `subClassOf watr:Filter`,
a `UnitProcess` that requires a treatment process, which is the wrong shape for a
contained physical part. The deployment loads it via `[ontologies].sources` in the
toml, and the model `owl:imports` it.

## ⚠️ Tag mapping is a BEST GUESS

The paper publishes **no SCADA tag legend**. The mapping below reads the raw
instrument tags against the P&ID; it is *not* verified. **Lock it empirically**
before trusting any derived TMP: for each candidate (feed-PT, filtrate-PT, FT)
combination, compute `R_t = ΔP/(μ(T)·J_F)` over one operating run and keep the
combination that reproduces the paper's sawtooth (Fig. 5; `R_t ≈ 2.0–2.6×10¹² m⁻¹`,
TMP rising to ≤100 kPa then resetting at each backwash).

| SCADA tag | unit | best-guess role | model Property |
|---|---|---|---|
| `FE/FT-100` | GPM | UF feed flow (common header) | `ph:UF-feed-flow` |
| `FE/FT-101/102/103` | GPM | UF1/2/3 filtrate (permeate) flow | `ph:UFn-filtrate-flow` |
| `FE/FT-104` | GPM | backwash flow | `ph:UF-backwash-flow` |
| `PT-100` | PSIG | UF feed-side pressure (**high side of TMP**) | `ph:UF-feed-pressure` |
| `PT-101/102/103` | PSIG | UF1/2/3 filtrate-side pressure (**low side of TMP**) | `ph:UFn-filtrate-pressure` |
| `PT-104` | PSI | filtrate header pressure | `ph:UF-filtrate-header-pressure` |
| `PT-100x` | PSIG | backwash line pressure | `ph:UF-backwash-pressure` |
| `TE/TT-100` | °C | feed temperature (viscosity correction) | `ph:UF-feed-temperature` |
| `Tu/TuT-100` | NTU | feed turbidity (fouling driver) | `ph:UF-feed-turbidity` |
| `pH/pHT-100` | pH | feed pH | `ph:UF-feed-ph` |
| `ORP/ORPT-100` | mV | feed ORP | `ph:UF-feed-orp` |
| `VFD-100-FB` | RPM | feed pump speed (filtration ON/OFF flag) | `ph:LP-pump-speed` |

`TMP = feed pressure − filtrate pressure` (derived; no native `TransmembranePressure`
quantity kind). Cleaning/backwash events are detected from the data, not modeled.

## Ingest driver (`scripts/port_hueneme_ingest.py`)

`PortHuenemeUFIngestDriver` subclasses `XLSXIngestDriver` to handle three quirks
the stock drivers can't:

1. **Dual header** — row 1 = tags, row 2 = engineering units. The units row is dropped.
2. **Three on-disk layouts:**
   - **`.csv`** — comma-delimited, with an absolute **Excel-serial `Time`** column
     (decoded via the 1899-12-30 epoch) plus `Time Elapsed`.
   - **`.xlsx` elapsed dumps** — either a single fixed-width text column
     (whitespace-delimited) or normal wide columns with **only elapsed-seconds
     `TIME`, no absolute timestamp**. The driver reconstructs absolute time as
     **filename-date (YYYYMMDD) midnight UTC + elapsed seconds**.
   - **experiment-summary exports** — descriptive headers, a row of SCADA tags,
     then units/notes, with no timestamp column. The driver retains recognizable
     SCADA tags and reconstructs absolute time as **filename-date midnight UTC +
     row index seconds**.

   ⚠️ Filename anchoring is *approximate* — the CSVs proved filenames need not
   equal the internal date — but it preserves each run's filtration/backwash
   **cycle structure**, which is what the fouling math needs. If cross-day
   absolute alignment matters, prefer files with an internal Excel-serial `Time`.
3. All daily files are consolidated under one datasource, `port-hueneme-uf`, with
   `ref_name` = the exact SCADA tag. Known tags are linked to their model Property
   (`point_uri`) at registration, so `models/port-hueneme-uf.ttl` resolves to live
   data. Offset paging is inherited, so growing/re-dropped files re-ingest safely.

Both layouts were verified end-to-end against extracted sample files.

## Running

`acquirium.toml` in this directory is self-contained: it starts the server,
**loads the model**, and **ingests the data** with one command.

```bash
# 1. get + unpack the data into ./data (once)
brew install unar
mkdir -p deployments/PORT_HUENEME/data && cd deployments/PORT_HUENEME/data
curl -sL -o ph.rar "https://zenodo.org/api/records/4630769/files/data_in_UF-RO_system.rar/content"
unar ph.rar        # -> "data in UF-RO system/{training,test}/*.csv|xlsx"
cd -

# 2. start everything (server + model load + ingest)
acquirium server --config deployments/PORT_HUENEME/acquirium.toml
```

The driver loads `models/port-hueneme-uf.ttl` on setup (via its `model_graph`
key → `insert_graph`; the model `owl:imports` the bundled NAWI water ontology),
then watches `./data` recursively and ingests every `.csv`/`.xlsx` under one
datasource, `port-hueneme-uf`. Paths in the toml resolve against this directory,
so it's portable. Set `recreate = true` once to wipe and reload from scratch.

Long clean runs to target first: **Test #1 (12 d / 300 h), Test #2 (~5 d),
Test #3 (~5 d, storm event)**. Filter standby periods (pump `VFD-100-FB ≈ 0`,
railed `ORP ≈ −1750`, `Tu ≈ −2.5`) before the fouling math.

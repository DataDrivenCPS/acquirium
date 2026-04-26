from __future__ import annotations

import io
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import rdflib
import requests
import streamlit as st
from pyomo.environ import value as pyomo_value
from pyomo.core.base.componentuid import ComponentUID
from pydantic import BaseModel
# Your WaterTAP build + solve entrypoint
from example_watertap import build_and_solve as build_fn

class InsertTimeseriesRequest(BaseModel):
    values: list[tuple[datetime, float | int | str | None]]

# ----------------------------
# Core utilities (mostly your code, lightly cleaned up)
# ----------------------------

def get_value_from_model(model: Any, component_name: str) -> Optional[float]:
    """
    Best-effort extraction of a numeric value given a full Pyomo component name.
    Returns None if not found or not numeric.
    """
    comp = None
    try:
        comp = model.find_component(component_name)
    except Exception:
        comp = None

    if comp is None and ComponentUID is not None:
        try:
            comp = ComponentUID(component_name).find_component_on(model)
        except Exception:
            comp = None

    if comp is None:
        return None

    try:
        v = pyomo_value(comp)
    except Exception:
        try:
            v = comp.value
        except Exception:
            return None

    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_mappings_from_ttl(ttl_path: str) -> Dict[str, str]:
    """
    Reads TTL and extracts { pyomo_var_name -> point_uri } using:
      ?s ref:hasExternalReference ?point_uri .
      ?s acquirium:hasPyomoVar  ?pyomo_var .
    """
    g = rdflib.Graph().parse(ttl_path, format="turtle")
    acq = rdflib.Namespace("urn:acquirium#")
    ref = rdflib.Namespace("https://brickschema.org/schema/Brick/ref#")

    mappings: Dict[str, str] = {}
    for s, _, point_uri in g.triples((None, ref.hasExternalReference, None)):
        for _, _, pyomo_var in g.triples((s, acq.hasPyomoVar, None)):
            mappings[str(pyomo_var)] = str(point_uri)

    if not mappings:
        raise ValueError(
            "No mappings found in TTL. Expected triples using "
            "ref:hasExternalReference and urn:acquirium#hasPyomoVar."
        )

    return mappings


@dataclass
class PushStats:
    sent: int
    missing: int
    failures: int


def push_points(
    model: Any,
    mappings: Dict[str, str],
    api_url: str,
    *,
    timeout_s: float = 10.0,
    dry_run: bool = True,
) -> PushStats:
    session = requests.Session()
    timestamp = _iso_now()

    sent = 0
    missing = 0
    failures = 0

    for var_name, point_uri in mappings.items():
        v = get_value_from_model(model, var_name)
        if v is None:
            missing += 1
            continue

        req = InsertTimeseriesRequest(values=[(timestamp, v)])
        params = {"ref_uri": point_uri}

        try:
            if dry_run:
                print(point_uri, req.model_dump(mode='json'))
            else:
                r = session.post(api_url,params = params ,json=req.model_dump(mode='json'), timeout=timeout_s)
                r.raise_for_status()
            sent += 1
        except Exception as e:
            failures += 1
            print(f"[warn] POST failed for {point_uri}: {e}", file=sys.stderr)

    return PushStats(sent=sent, missing=missing, failures=failures)


def run_build_and_push(
    *,
    mappings: Dict[str, str],
    api_url: str,
    dry_run: bool,
    flow_vol: float,
    salt_mass_conc: float,
    operating_pressure: float,
    flow_mass_liq: float,
    flow_mass_salt: float,
) -> Tuple[PushStats, str]:
    """
    Runs build_fn(...) then push_points(...).
    Returns (stats, combined_stdout_stderr).
    """
    buf_out = io.StringIO()
    buf_err = io.StringIO()

    old_out, old_err = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = buf_out, buf_err
    try:
        out = build_fn(
            flow_vol=flow_vol,
            salt_mass_conc=salt_mass_conc,
            operating_pressure=operating_pressure,
            flow_mass_liq=flow_mass_liq,
            flow_mass_salt=flow_mass_salt,
        )

        model = out[0] if isinstance(out, tuple) and len(out) >= 1 else out

        stats = push_points(
            model=model,
            mappings=mappings,
            api_url=api_url,
            timeout_s=10.0,
            dry_run=dry_run,
        )
    finally:
        sys.stdout, sys.stderr = old_out, old_err

    logs = buf_out.getvalue()
    err_logs = buf_err.getvalue()
    if err_logs.strip():
        logs = logs + ("\n" if logs and not logs.endswith("\n") else "") + err_logs

    return stats, logs


# ----------------------------
# Streamlit UI
# ----------------------------

st.set_page_config(page_title="WaterTAP Runner", page_icon="💧", layout="centered")

st.markdown(
    """
<style>
.block-container { max-width: 900px; padding-top: 2rem; }
div[data-testid="stMetricValue"] { font-size: 1.6rem; }
.small-muted { color: rgba(120,120,120,1); font-size: 0.9rem; }
.card {
  border: 1px solid rgba(200,200,200,0.6);
  border-radius: 14px;
  padding: 14px 16px;
  background: rgba(250,250,250,0.85);
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("WaterTAP Model Runner")
st.write("Adjust inputs, run the model, then push solved values to your API.")

with st.sidebar:
    st.header("Settings")

    ttl_path = st.text_input(
        "TTL model path",
        value="/workspace/test-model.ttl",
        help="Used to derive {pyomo_var -> point_uri} mappings.",
    )

    api_url = st.text_input(
        "API URL",
        value="http://acquirium:8000/insert_timeseries",
        help="Endpoint that accepts {ref_uri, value, timestamp}.",
    )

    dry_run = st.toggle(
        "Dry run (print payloads, do not POST)",
        value=True,
    )

    if st.button("Reload mappings", use_container_width=True):
        st.session_state.pop("mappings", None)
        st.session_state.pop("mappings_error", None)

# Load mappings once (cached in session)
if "mappings" not in st.session_state and "mappings_error" not in st.session_state:
    try:
        st.session_state["mappings"] = load_mappings_from_ttl(ttl_path)
    except Exception as e:
        st.session_state["mappings_error"] = str(e)

if "mappings_error" in st.session_state:
    st.error(st.session_state["mappings_error"])
    st.stop()

mappings: Dict[str, str] = st.session_state["mappings"]

st.markdown(f"<div class='small-muted'>Loaded {len(mappings)} point mappings</div>", unsafe_allow_html=True)
st.write("")

st.markdown("<div class='card'>", unsafe_allow_html=True)
st.subheader("Inputs")

flow_vol = st.slider(
    "Flow Volume",
    min_value=0.0005,
    max_value=0.0020,
    value=0.0010,
    step=0.0001,
    format="%.4f",
)
salt_mass_conc = st.slider(
    "Salt Mass Concentration",
    min_value=0.005,
    max_value=0.05,
    value=0.035,
    step=0.001,
    format="%.3f",
)
operating_pressure = st.slider(
    "Operating Pressure",
    min_value=4.5e6,
    max_value=5.5e6,
    value=5e6,
    step=1e5,
    format="%.0f",
)
flow_mass_liq = st.slider(
    "Flow Mass Liquid",
    min_value=0.900,
    max_value=0.999,
    value=0.985,
    step=0.001,
    format="%.3f",
)
flow_mass_salt = st.slider(
    "Flow Mass Salt",
    min_value=0.001,
    max_value=0.100,
    value=0.015,
    step=0.001,
    format="%.3f",
)

st.markdown("</div>", unsafe_allow_html=True)

st.write("")
run = st.button("Run build_fn and push points", type="primary", use_container_width=True)

if run:
    with st.spinner("Running model and pushing points..."):
        try:
            stats, logs = run_build_and_push(
                mappings=mappings,
                api_url=api_url,
                dry_run=dry_run,
                flow_vol=float(flow_vol),
                salt_mass_conc=float(salt_mass_conc),
                operating_pressure=float(operating_pressure),
                flow_mass_liq=float(flow_mass_liq),
                flow_mass_salt=float(flow_mass_salt),
            )
        except Exception as e:
            st.error(f"Run failed: {e}")
            st.stop()

    c1, c2, c3 = st.columns(3)
    c1.metric("Sent", stats.sent)
    c2.metric("Missing", stats.missing)
    c3.metric("Failures", stats.failures)

    st.subheader("Logs")
    st.code(logs if logs.strip() else "(no logs)", language="text")

st.write("")
# st.caption("Tip: turn off Dry run when your API endpoint is ready.")

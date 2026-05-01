"""
Push WaterTAP model readings to an HTTP API using a mappings file produced by
`collect_varname_to_point_uri`.

Example:

    python push_watertap_points.py \
        --mappings point_mappings.json \
        --build-module my_watertap_script \
        --build-fn build_and_solve \
        --api-url http://localhost:8000/ingest

By default, this sends one POST request per point:

    POST {api_url}
    {
      "uri": "<point uri>",
      "value": 3.2
    }

Use `--batch` to send a single request with:
    { "observations": [ { "uri": ..., "value": ... }, ... ] }
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pyomo.environ import value as pyomo_value
from pyomo.core.base.componentuid import ComponentUID
from example_watertap import build_and_solve as build_fn  
import rdflib
import requests

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


def push_points(
    model: Any,
    mappings: Dict[str, str],
    # api_url: str,
    *,
    timeout_s: float = 10.0,
) -> None:
    session = requests.Session()
    timestamp = _iso_now()

    missing = 0
    failures = 0
    for var_name, point_uri in mappings.items():
        v = get_value_from_model(model, var_name)
        if v is None:
            missing += 1
            continue

        payload = {"ref_uri": point_uri, "value": v}
        if timestamp is not None:
            payload["timestamp"] = timestamp

        try:
            print(payload)
            # pass
            # r = session.post(api_url, json=payload, timeout=timeout_s)
            # r.raise_for_status()
        except Exception as e:
            failures += 1
            print(f"[warn] POST failed for {point_uri}: {e}", file=sys.stderr)

    if missing:
        print(f"[warn] {missing} values were missing or non-numeric; skipped.", file=sys.stderr)
    if failures:
        print(f"[warn] {failures} POSTs failed.", file=sys.stderr)


def main() -> None:
    G = rdflib.Graph().parse("deployments/WATERTAP2/models/test-model.ttl", format="turtle")
    mappings = {}
    ACQUIRIUM_NS = rdflib.Namespace("urn:acquirium#")
    REF_NS = rdflib.Namespace("https://brickschema.org/schema/Brick/ref#")
    for s,p,o in G.triples((None, REF_NS.hasExternalReference, None)):
        for s2,p2,o2 in G.triples((s, ACQUIRIUM_NS.hasPyomoVar, None)):
            mappings[str(o2)] = str(o)
    
    if not isinstance(mappings, dict):
        raise TypeError("Expected mappings JSON to be a JSON object of { var_name: point_uri }")


    out = build_fn(
        flow_vol=0.001,
        salt_mass_conc=0.035,
        operating_pressure=5e6,
        flow_mass_liq=0.985,
        flow_mass_salt=0.015,
    )
    if isinstance(out, tuple) and len(out) >= 1:
        model = out[0]
    else:
        model = out

    api_url = "acquirium/insert_timeseries"

    push_points(
        model,
        mappings,
        api_url=api_url,
        timeout_s=10.0,
    )


if __name__ == "__main__":
    main()

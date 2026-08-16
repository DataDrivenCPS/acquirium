"""Membrane-fouling soft sensor, packaged as an Acquirium monitoring app.

This is the notebook `notebooks/watertap/soft-sensor.ipynb` turned into a
long-running app. The build phase trains a linear soft sensor on the plant's
first two weeks of operation (assumed clean); each run scores the most recent
window, estimates relative permeability (actual / expected permeate flow), and
fires a webhook when the membrane fouls.

Point the trigger at `scripts/watertap/fouling_gui.py` to watch it live:

    python scripts/watertap/fouling_gui.py            # dashboard on :10001
    python scripts/watertap/ml-workload.py            # register + run (see __main__)
"""

from datetime import datetime, timezone

import numpy as np
import polars as pl

from acquirium import Acquirium, App, AppContext, Output


class InteractionOLS:
    """OLS on standardized features (the notebook's soft-sensor model)."""

    def __init__(self, features):
        self.features = features

    def _design(self, X):
        Xs = (X - self.mean_) / self.std_
        cols = [np.ones(len(Xs))] + [Xs[:, i] for i in range(Xs.shape[1])]
        return np.column_stack(cols)

    def fit(self, X, y):
        self.mean_ = X.mean(axis=0)
        std = X.std(axis=0)
        self.std_ = np.where(std == 0, 1.0, std)  # constant feature -> zero column, not NaN
        D = self._design(X)
        self.coef_, *_ = np.linalg.lstsq(D, y, rcond=None)
        resid = y - D @ self.coef_
        self.train_resid_std_ = resid.std()
        ss_res, ss_tot = (resid ** 2).sum(), ((y - y.mean()) ** 2).sum()
        self.r2_ = 1 - ss_res / ss_tot
        return self

    def predict(self, X):
        return self._design(X) @ self.coef_


class MembraneFoulingSoftSensor(App):
    name = "membrane_fouling_soft_sensor"
    version = "0.1"
    app_type = "soft_sensor"
    outputs = [
        {
            "kind": "trigger",
            "point_uri": "urn:derived:relative_permeability",
        }
    ]

    def build_query(self, aq: Acquirium):
        from acquirium.Client.explore.attributes import Not

        ro_q = aq.query().entity("reverse osmosis membrane", alias="ro")
        ro_cp_q = ro_q.related("ConnectionPoint", alias="ro_cp", max_depth=1)
        ro_feed_q = (ro_cp_q.related("Water-Seawater", alias="feed_medium", frm="ro_cp", max_depth=1)
                     .measurement(alias="feed", frm="ro_cp"))
        # Two branches from the shared root: permeate hangs off ro_q, not ro_cp_q.
        # The original chained filter_by_medium("fluid water") and then
        # filter_by_medium("brine", exclude=True); the second silently
        # replaced the first (filters are keyed per attribute), so only the
        # brine exclusion ever applied. Ported to that effective behaviour.
        ro_perm_q = (ro_q.related("outlet Connection Point", alias="RO_out", max_depth=1)
                     .measurement(alias="permeate", frm="RO_out",
                                  quantity_kind="mass flow rate",
                                  medium=Not("brine"),
                                  substance=Not("constituent salt"))
                     )
        return {
            "feed": ro_feed_q,
            "permeate": ro_perm_q
        }

    # --- build phase: train once on the clean baseline ----------------------
    # Config comes from the params registered with the app (ctx.params): the
    # clean-baseline window and the threshold width. Trained once, at register.
    def build_app(self, ctx: AppContext):
        baseline_start = ctx.params.get("baseline_start")
        baseline_end = ctx.params.get("baseline_end")
        k_sigma = ctx.params.get("k_sigma", 4.0)

        feed = ctx.queries["feed"].dataframe(start=baseline_start, end=baseline_end, shape="wide", cast_value="float")
        permeate = ctx.queries["permeate"].dataframe(start=baseline_start, end=baseline_end, shape="wide", cast_value="float")
        df = feed.join(permeate, on="time", how="inner").drop_nulls().sort("time")

        if df.is_empty():
            raise RuntimeError("no overlapping feed/permeate data to train on")

        features = [i for i in feed.columns if i != "time"]
        target = [i for i in permeate.columns if i != "time"][0]

        X = df.select(features).to_numpy()
        y = df[target].to_numpy()

        model = InteractionOLS(features).fit(X, y)

        # Baseline noise on relative permeability sets the fouling threshold.
        rel = y / model.predict(X)
        baseline_mean = float(rel.mean())
        baseline_std = float(rel.std())
        threshold = baseline_mean - k_sigma * baseline_std

        return {
            "model": model,
            "features": features,
            "target": target,
            "baseline_mean": baseline_mean,
            "baseline_std": baseline_std,
            "threshold": threshold,
        }

    # --- run phase: score the latest window ---------------------------------
    # Config comes from the params passed to run_app (ctx.params): how many
    # samples to pull, the rolling-mean width, and the sustain count.
    def run(self, ctx: AppContext) -> list[Output]:
        state = ctx.state
        if not state:
            raise RuntimeError("model not built; setup() did not complete")

        model = state["model"]
        features = state["features"]
        target = state["target"]
        baseline_mean = state["baseline_mean"]
        threshold = state["threshold"]

        run_window = ctx.params.get("run_window", 48)
        smooth = ctx.params.get("smooth", 6)
        sustain = ctx.params.get("sustain", 4)

        # Most recent run_window samples, oldest-first for the rolling mean.
        feed = ctx.queries["feed"].dataframe(
            limit=run_window, order="desc", shape="wide", cast_value="float"
        )
        permeate = ctx.queries["permeate"].dataframe(
            limit=run_window, order="desc", shape="wide", cast_value="float"
        )
        df = feed.join(permeate, on="time", how="inner").drop_nulls().sort("time")
        if df.is_empty():
            return []

        X = df.select(features).to_numpy()
        rel_raw = df[target].to_numpy() / model.predict(X)
        rel = (
            pl.Series(rel_raw)
              .rolling_mean(window_size=smooth, min_samples=1)
              .to_numpy()
        )

        latest_rel = float(rel[-1])
        latest_time = df["time"][-1]
        below = rel < threshold
        sustained = bool(len(below) >= sustain and below[-sustain:].all())
        drop_pct = (1.0 - latest_rel / baseline_mean) * 100.0

        if sustained:
            status = "fouling"
            text = (f"⚠ Fouling detected — relative permeability {latest_rel:.3f}, "
                    f"{drop_pct:.1f}% below baseline for {sustain}+ samples")
        elif latest_rel < threshold:
            status = "watch"
            text = (f"Permeability dip — {latest_rel:.3f} "
                    f"({drop_pct:.1f}% below baseline, threshold {threshold:.3f})")
        else:
            status = "nominal"
            text = (f"Membrane nominal — relative permeability {latest_rel:.3f} "
                    f"(baseline {baseline_mean:.3f})")

        message = {
            "text": text,
            "status": status,
            "relative_permeability": round(latest_rel, 4),
            "baseline": round(baseline_mean, 4),
            "threshold": round(threshold, 4),
            "drop_pct": round(drop_pct, 2),
            "as_of": latest_time.isoformat(),
        }

        return [Output.trigger(
            url="localhost:10001/alerts",
            message=message,
            point_uri=self.outputs[0]["point_uri"],
        )]


if __name__ == "__main__":
    acq = Acquirium(server_url="localhost", server_port=8000)

    # Build-time config (the clean-baseline window, threshold width) is
    # registered with the app and reaches build_app via ctx.params.
    # acq.register_app(
    #     MembraneFoulingSoftSensor(),
    #     replace=True,
    #     params={
    #         "baseline_start": datetime(2025, 1, 1, tzinfo=timezone.utc),
    #         "baseline_end": datetime(2025, 1, 15, tzinfo=timezone.utc),
    #         "k_sigma": 4.0,
    #     },
    # )

    # Run-time config (window/smoothing/sustain) is passed per run and reaches
    # run() via ctx.params.
    # acq.run_app(
    #     "membrane_fouling_soft_sensor",
    #     keep_alive=True,
    #     interval=60,
    #     params={"run_window": 48, "smooth": 6, "sustain": 4},
    # )
    acq.stop_app(app_id="membrane_fouling_soft_sensor")
    # print(acq.list_app_runs())

"""Experiment harness for semantic incremental views.

Each ``rq*.py`` module is a runnable script that starts a private local
server under ``experiments/runs/<name>/<timestamp>/``, loads the Benicia
model and generated data, deploys the view library in ``views.py``, drives
changes through the public client, and writes result tables and figures
into the run directory. ``common.py`` holds the server lifecycle and the
quiescence wait, ``benicia.py`` the model and data generation, ``replay.py``
the ingestion driver with lateness and corrections, ``oracle.py`` the
from-scratch recomputation, and ``metrics.py`` the event-log analysis.
"""

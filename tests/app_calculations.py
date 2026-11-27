"""Calculation source that tests can pin when their focus is the app plan."""


def mean(frame, context):
    port, = context["operator"]["outputs"]
    return {port: frame["value"].mean()}


def total(frame, context):
    port, = context["operator"]["outputs"]
    values = frame["value"].drop_nulls()
    return {port: None if values.is_empty() else values.sum()}

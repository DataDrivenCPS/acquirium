"""Calculate the complete threshold result for one input timestamp."""

def threshold(frame, context):
    value = frame["value"].drop_nulls().first()
    return {"high": value if value is not None and value > 0.5 else None}

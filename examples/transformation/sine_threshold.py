"""Calculate the complete threshold result for one input timestamp."""

from escapement import emit, empty


def threshold(frame, context):
    if frame.is_empty():
        return {"high": empty()}

    value = frame["value"][0]
    if value > 0.5:
        result = emit(context, value)
    else:
        result = empty()

    return {"high": result}

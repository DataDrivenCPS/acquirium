"""Build the app declaration for Python deployment or startup configuration."""

from pathlib import Path

import acquirium as aq


def high_wave(client, parameters):
    query = client.query().measurement(alias="wave", uri="urn:example:sine-wave")
    return aq.App(
        name="high-wave",
        inputs=[aq.Selection.from_query(query, alias="wave")],
        outputs={"high": aq.Output()},
        function=aq.PythonFunction.from_file(
            Path(__file__).with_name("sine_threshold.py"), "threshold"
        ),
        window=aq.Pointwise(),
    )

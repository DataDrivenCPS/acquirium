"""Smoke test run against a freshly built distribution.

Imports the public surface exported from ``acquirium.__init__`` to confirm the
wheel/sdist installs cleanly and that no top-level symbol is missing. Heavier
runtime checks (server, drivers) are out of scope here.
"""

from acquirium import (
    Acquirium,
    App,
    DataObject,
    Driver,
    EventIngestDriver,
    IngestDriver,
    OutputSpec,
    PollingIngestDriver,
    Query,
    align,
    output,
)


def main() -> None:
    exports = (
        Acquirium,
        App,
        DataObject,
        Driver,
        EventIngestDriver,
        IngestDriver,
        OutputSpec,
        PollingIngestDriver,
        Query,
            align,
        output,
    )
    for obj in exports:
        assert obj is not None, f"{obj!r} import failed"
    print(f"acquirium smoke test OK ({len(exports)} exports)")


if __name__ == "__main__":
    main()

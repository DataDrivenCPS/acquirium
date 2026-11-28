"""Public entry points for collecting, querying, and processing readings.

`Acquirium` connects to the HTTP API. `init` manages a local server or
attaches to an existing one. On the server, Acquirium resolves streams and
their metadata, and Escapement publishes readings and app outputs in
epochs. Drivers feed the same write API that the Python client exposes.
"""

# TODO: import lazily (module __getattr__) so calculation workers importing
# acquirium.functions skip the client stack (~0.25 s per cold worker).
from acquirium.Client.acquirium import Acquirium
from acquirium.runtime import init, shutdown
from acquirium.apps import App, Selection, Output, ReplacementOutput
from acquirium.app_spec import FixedBucket, Pointwise, PythonFunction, Rolling
from acquirium.debug import console
from acquirium.Client.streams import Stream, StreamInterval
from acquirium.actors import Actor, GraphChange, InputChange, TimerFired, Transition
from acquirium.Experiments import timestamp
from acquirium.ExperimentValues import register_value_handler
from acquirium.ModelAdapters import AdapterDefinition, Component, ModelAdapter
from acquirium.ObjectCodecs import BytesCodec, CloudpickleCodec, Codec, JsonCodec, PickleCodec
from acquirium.Experiments import DeclaredVariables
from acquirium import adapters
from acquirium.Client.explore.core import Query
from acquirium.Client.data_object import DataObject
from acquirium.Drivers.Driver import (
    Driver,
    DriverBufferFull,
    EventIngestDriver,
    FileBatch,
    FileIngestDriver,
    IngestDriver,
    PollingIngestDriver,
    UndeclaredStreamError,
)
from acquirium.Drivers.BuiltInDrivers.csv_ingest import CSVIngestDriver
from acquirium.Drivers.tabular import to_observations, to_timestamp

# Native semantic apps

`temperature_normalization.py:temperature_normalization` is a factory accepting
`(client, parameters)` and returning an `acquirium.App` declaration. It selects
temperature streams, freezes unit conversion factors in their input context,
and runs a versioned native Python function over pointwise groups. It explicitly
excludes its own derived references from its broad semantic selection.

Deploy with `acquirium app deploy scripts/apps/temperature_normalization.py:temperature_normalization`
or call the factory and `client.deploy_app()` from Python.

Full-history gap filling has been retired until Escapement supports that workload.

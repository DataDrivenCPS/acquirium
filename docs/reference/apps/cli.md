---
title: App commands
---

| Command | Purpose |
| --- | --- |
| `acquirium app list` | List deployed declarations and their status |
| `acquirium app inspect NAME` | Inspect selected inputs, operators and failures |
| `acquirium app deploy SPEC --dry-run` | Preview a declaration's plan and estimated work |
| `acquirium app deploy SPEC` | Deploy a declaration or factory from a module or file |

A spec has the form `path/to/module.py:factory`. When called by the CLI or
server, a factory receives the client and a parameter mapping. The driver and
app commands use the configured server address.

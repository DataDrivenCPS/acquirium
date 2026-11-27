---
title: Apps and engine status
---

| Method | Result |
| --- | --- |
| `deploy_app(declaration, dry_run=False)` | Deploy an `App`, or preview the work with `dry_run=True` |
| `list_apps()` | List declarations and published operators |
| `inspect_app(name)` | Report selected streams, warnings and failures for one app |
| `remove_app(name)` | Retract an app's current outputs after dependants are removed |
| `engine_status()` | Report publication progress, capacity and failures |
| `engine_snapshot()` | Capture a published epoch and active plan |
| `retry_engine()` | Request another attempt for failed operators |
| `wait(receipt, timeout=30)` | Wait for an accepted write to publish |
| `changes(epoch=0, ordinal=0, limit=1000, timeout=30)` | Read a page of signed changes |

Inspection can report `pending` during plan activation. Use
[deploying apps](../apps/deploying.md) for details and [the change feed guide](../../how-to/consume-streaming-changes.md) for cursor handling.

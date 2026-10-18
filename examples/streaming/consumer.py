"""Resumable example sink: atomically checkpoint complete epochs and their view."""

from copy import deepcopy

from .state import key, load, save


class ChangeConsumer:
    def __init__(self, client, path, *, page_size=1000):
        self.client, self.path, self.page_size = client, path, page_size
        self.state = load(
            path,
            {
                "cursor": {"epoch": 0, "ordinal": 0},
                "complete_through": 0,
                "rows": [],
                "changes": 0,
            },
        )
        self.cursor = dict(self.state["cursor"])
        self.pending = []

    def poll(self):
        page = self.client.changes(**self.cursor, limit=self.page_size, timeout=5)
        self.pending.extend(page["changes"])
        self.cursor = page["cursor"]
        complete = page["complete_through"]
        ready = [change for change in self.pending if change["epoch"] <= complete]
        if ready or complete > self.state["complete_through"]:
            state = deepcopy(self.state)
            rows = {key(row): row for row in state["rows"]}
            for change in ready:
                row = change["row"]
                slot = key(row)
                if change["weight"] == -1:
                    if rows.get(slot) != row:
                        raise RuntimeError(f"sink retraction mismatch at {slot}")
                    del rows[slot]
                elif change["weight"] == 1:
                    if slot in rows:
                        raise RuntimeError(f"sink duplicate insert at {slot}")
                    rows[slot] = row
                else:
                    raise RuntimeError("unsupported change weight")
            state["rows"] = [rows[slot] for slot in sorted(rows)]
            if ready:
                state["cursor"] = {
                    name: ready[-1][name] for name in ("epoch", "ordinal")
                }
            state["changes"] += len(ready)
            state["complete_through"] = complete
            # Cursor and visible rows share one durable replacement. Partial
            # epochs stay in memory; a crash rereads them from the saved cursor.
            save(self.path, state)
            self.state = state
            self.pending = [
                change for change in self.pending if change["epoch"] > complete
            ]
        return page

    def values(self):
        return {key(row): row["data"]["value"] for row in self.state["rows"]}

---
title: Query API
---

<!-- TODO: intro -->

<!-- TODO: signatures of every verb (entity, related, measurement, where, include, drop,
     with_columns, alias, refocus) and terminal (metadata, data, dataframe, options,
     facets, execute, to_sparql, resolved_nodes) -->

## Coming from the old interface

Optional. Skip this unless you have queries written against the previous
`find_*` methods.

Those methods still exist, reachable through `acq.find_entity()` and
`acq.find_all_data()`, but they are deprecated and will not gain features.
The rewrite is usually mechanical: one verb per old method, one `where()` in
place of the `filter_by_*` family.

| old | new |
|---|---|
| `find_entity(_class=, uri=, alias=)` | `entity(cls, uri=, alias=)` |
| `find_entity(process=...)` | `entity(cls, process=...)` |
| `find_related(_class=, _from=, hops=, predicates=)` | `related(cls, frm=, max_depth=, via=)` |
| `find_related(direction=)` | `related(direction=)` |
| `find_data(_from=, alias=, filters_dict=)` | `measurement(frm=, alias=, **attrs)` |
| `find_all_data()` | `measurement(frm="*")`, or `measurement()` on an empty query |
| `find_related_data(...)` | `related(...).measurement(...)` |
| `filter_by_unit(v)` | `where(unit=v)` |
| `filter_by_medium(v)` | `where(medium=v)` |
| `filter_by_substance(v)` | `where(substance=v)` |
| `filter_by_quantity_kind(v)` | `where(quantity_kind=v)` |
| `filter_by_enumeration_kind(v)` | `where(enumeration_kind=v)` |
| `filter_data_nodes(predicate=, value=, _from=)` | `where(attr=value, target=)` |
| `filter_by_*(..., exclude=True)` | `where(attr=Not(value))` |
| `show_query_graph()` | `to_sparql()` |

Keyword names changed with them: `_class` is `cls`, `_from` is `frm`, `hops`
is `max_depth`, and `predicates` is `via`.
`metadata()`, `data()`, `dataframe()`, `execute()`, `to_sparql()`,
`insert_log()` and `read_logs()` kept their names and behave the same.

Two behavior differences matter when porting.

Defaults changed.
`related()` returns nearest matches where `find_related` returned everything
reachable, so add `nearest=False` if you relied on the old behavior.

Filtering is one vocabulary now.
Anywhere you passed a predicate URI to `filter_data_nodes`, you now pass a
named attribute to `where()`, and the same names work as inline keyword
arguments on `entity()`, `related()` and `measurement()`.

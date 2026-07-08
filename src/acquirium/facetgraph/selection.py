"""Fluent faceted query interface over an RDF graph.

Graframe ("graph frame") lets you explore graph-shaped metadata (223, Brick, the
NAWI water ontology, ...) without writing SPARQL. You hold a **Selection** — a
set of focus nodes carried as a bindings table with a cursor — and move through
the graph with two symmetric operators:

* :meth:`Selection.having` — *stay* on the current nodes, keeping only those
  that satisfy an edge condition (an existential semijoin; never multiplies
  rows).
* :meth:`Selection.follow` — *move* the cursor to the neighbours reached along an
  edge / property path (an image; the only operator that adds a column).

You inspect what is reachable with :meth:`Selection.facets` (whose rows can be
fed straight back into :meth:`~Selection.follow` / :meth:`~Selection.having`),
hold waypoints with :meth:`Selection.mark` / :meth:`Selection.to`, express
correlated constraints with :meth:`Selection.where` / :meth:`Selection.any_of`,
and pull results with :meth:`Selection.nodes` / :meth:`Selection.select`.

The whole surface denotes a SPARQL query (see :meth:`Selection.to_sparql`); that
denotation is the correctness anchor.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping, Sequence, TYPE_CHECKING

from rdflib import URIRef

from .algebra import (
    RDF_TYPE,
    RDFS_SUBCLASS_OF,
    Cmp,
    DatatypeCmp,
    Exists,
    Iri,
    Lit,
    OrExists,
    Path,
    Pattern,
    Pred,
    RawPath,
    Term,
    Triple,
    Values,
    Var,
    _is_uri,
    alt_of,
    parse_path,
    patterns_vars,
    to_path,
)
from .facets import FacetRow
from .profile import Profile
from .resolve import Fuzzy, resolve_iri, suggest as _suggest

if TYPE_CHECKING:
    import polars as pl
    from acquirium.Client.data_object import DataObject
    from .facets import Facets

StepArg = "str | Path | Sequence[str]"


@dataclass(frozen=True)
class Reasoning:
    """Which entailments to fold into queries.

    * ``subclass`` — treat ``rdf:type`` as ``rdf:type/rdfs:subClassOf*`` so
      ``instances(Sensor)`` also matches instances of subclasses of Sensor.
      This is on by default; it is usually what you want for 223/Brick/water.
    * ``subproperty`` / ``inverse`` — reserved; not yet implemented. Setting
      either raises so behaviour is never silently wrong.
    """

    subclass: bool = True
    subproperty: bool = False
    inverse: bool = False

    def __post_init__(self) -> None:
        if self.subproperty or self.inverse:
            raise NotImplementedError(
                "Reasoning.subproperty / Reasoning.inverse are not implemented yet"
            )

    def type_path(self) -> Path:
        if self.subclass:
            return RawPath(f"<{RDF_TYPE}>/<{RDFS_SUBCLASS_OF}>*")
        return RawPath(f"<{RDF_TYPE}>")


@dataclass(frozen=True)
class _State:
    """Immutable query state: a conjunctive pattern + cursor + waypoints."""

    patterns: tuple[Pattern, ...] = ()
    focus: str = "n0"
    marks: Mapping[str, str] = field(default_factory=dict)
    counter: int = 1


class Graframe:
    """Root/session object: builds seed selections bound to a client."""

    def __init__(
        self,
        client: Any,
        reasoning: Reasoning | None = None,
        profile: Profile | None = None,
        *,
        fuzzy: bool = True,
        min_score: float = 0.5,
    ):
        self.client = client
        self.reasoning = reasoning or Reasoning()
        self.profile = profile
        self.fuzzy = fuzzy
        self.min_score = min_score

    def _resolve(self, x: Any, kind: str | None) -> str:
        return resolve_iri(
            self.client, x, kind=kind, fuzzy=self.fuzzy, min_score=self.min_score
        )

    # -- seeds ----------------------------------------------------------
    def instances(self, cls: Any) -> "Selection":
        """Selection of all instances of ``cls``.

        ``cls`` may be a CURIE, URI, URIRef, a natural-language name (resolved
        via the embedding matcher when fuzzy matching is on), or ``like(...)``.
        With the default reasoning profile subclasses are included.
        """
        iri = self._resolve(cls, "class")
        state = _State(
            patterns=(Triple(Var("n0"), self.reasoning.type_path(), Iri(iri)),),
            focus="n0",
            counter=1,
        )
        return self._seed(state)

    def nodes(self, *uris: Any) -> "Selection":
        """Selection seeded from one or more explicit nodes (URI/CURIE/name)."""
        if not uris:
            raise ValueError("nodes: provide at least one URI/CURIE")
        terms = tuple(Iri(self._resolve(u, None)) for u in uris)
        state = _State(
            patterns=(Values(Var("n0"), terms),), focus="n0", counter=1
        )
        return self._seed(state)

    def everything(self) -> "Selection":
        """Selection of every node that appears as a subject in the graph."""
        state = _State(
            patterns=(Triple(Var("n0"), RawPath("?__gp0"), Var("n1")),),
            focus="n0",
            counter=2,
        )
        return self._seed(state)

    def _seed(self, state: _State) -> "Selection":
        return Selection(
            self.client, self.reasoning, state,
            profile=self.profile, fuzzy=self.fuzzy, min_score=self.min_score,
        )

    def suggest(self, text: str, kind: str | None = None, *, top_k: int = 5) -> list[dict]:
        """Preview embedding matches for ``text`` — ``[{curie, score, kind}, ...]``.

        Useful for disambiguation (e.g. ``"pump"`` matches both s223 and nawi).
        """
        return _suggest(self.client, text, kind, top_k=top_k)


class Selection:
    """An immutable cursor over a set of graph nodes. Operators return copies."""

    def __init__(
        self,
        client: Any,
        reasoning: Reasoning,
        state: _State,
        profile: Profile | None = None,
        *,
        fuzzy: bool = True,
        min_score: float = 0.5,
    ):
        self.client = client
        self.reasoning = reasoning
        self._state = state
        self.profile = profile
        self.fuzzy = fuzzy
        self.min_score = min_score

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------
    def _with(self, state: _State) -> "Selection":
        return Selection(
            self.client, self.reasoning, state,
            profile=self.profile, fuzzy=self.fuzzy, min_score=self.min_score,
        )

    def _fresh(self, counter: int) -> tuple[Var, int]:
        return Var(f"n{counter}"), counter + 1

    def _resolve(self, x: Any, kind: str | None) -> str:
        """Resolve a term (URI/CURIE/name/Fuzzy) to a URI string for ``kind``."""
        return resolve_iri(
            self.client, x, kind=kind, fuzzy=self.fuzzy, min_score=self.min_score
        )

    def _expand(self, x: Any) -> str:
        """Resolve a CURIE/URI/URIRef to a full URI string (no fuzzy)."""
        return _iri(self.client, x)

    def _step_to_path(self, step: Any, direction: str) -> Path:
        if direction not in ("out", "in"):
            raise ValueError("direction must be 'out' or 'in'")
        if isinstance(step, str) and self.profile and step in self.profile.edges:
            path = to_path(self.profile.edges[step], self._expand)
        elif isinstance(step, Path):
            path = step
        elif isinstance(step, (list, tuple)):
            path = alt_of([self._atomic(s) for s in step])
        elif isinstance(step, str) and _is_path_expr(step):
            # inline SPARQL property-path syntax, e.g. "a/b", "connectedTo+",
            # "^a|b". Each segment resolves like a predicate slot — URI, CURIE,
            # or (when fuzzy is on) a natural-language name, so
            # "has property/has quantity kind" works.
            path = parse_path(step, lambda t: self._resolve(t, "predicate"))
        elif isinstance(step, (str, Fuzzy)):
            path = self._atomic(step)
        else:
            raise TypeError(f"step must be a str, list, Path, or like(...), got {type(step)}")
        if direction == "in":
            path = path.inverse()
        return path

    def _atomic(self, s: Any) -> Path:
        if isinstance(s, Fuzzy):
            return Pred(self._resolve(s, "predicate"))
        if isinstance(s, str) and (s.startswith("^") or s.startswith("~")):
            return Pred(self._resolve(s[1:], "predicate")).inverse()
        return Pred(self._resolve(s, "predicate"))

    def _term(self, x: Any) -> Term:
        """Coerce an object-filter value to a term.

        Strings resolve exactly like concept slots (URI / bound CURIE / unknown
        prefix -> fuzzy / natural language -> fuzzy), so you filter by name, not
        by URI. To force a plain literal, pass a number or an already-built
        ``Lit`` / ``rdflib.Literal``.
        """
        if isinstance(x, Term):
            return x
        if isinstance(x, Fuzzy):
            return Iri(self._resolve(x, None))
        if isinstance(x, URIRef):
            return Iri(str(x))
        if isinstance(x, str):
            return Iri(self._resolve(x, None))
        return Lit(x)

    def _facet_row_expand(
        self, step: Any, direction: str, filters: dict
    ) -> tuple[Any, str, dict]:
        """Expand a :class:`FacetRow` passed as ``step`` into (step, dir, φ).

        A facet row carries everything needed to take the move it describes:
        its predicate / virtual-edge name, its direction, and — for ``pred-obj``
        / ``pred-obj-type`` facets — the object value or type as a filter. This
        is what makes facets *actionable*: ``sel.follow(f.row(...))``. Explicit
        keyword filters passed alongside win over the row's own key.
        """
        if not isinstance(step, FacetRow):
            return step, direction, filters
        row = step
        if row.direction == "in":
            direction = "in"
        filters = dict(filters)
        if row.key is not None:
            if row.key_kind == "type" and filters.get("is_a") is None:
                filters["is_a"] = row.key
            elif row.key_kind == "datatype" and filters.get("datatype") is None:
                filters["datatype"] = row.key
            elif row.key_kind == "value" and filters.get("value") is None:
                # keep literal objects literal; let URIs resolve as IRIs
                filters["value"] = row.key if _is_uri(row.key) else Lit(row.key)
        return row.predicate, direction, filters

    def _constraints(
        self,
        target: Var,
        counter: int,
        *,
        value: Any = None,
        is_a: Any = None,
        datatype: Any = None,
        min: Any = None,
        max: Any = None,
        in_: Sequence[Any] | None = None,
        matching: "Selection | None" = None,
    ) -> tuple[list[Pattern], int]:
        """Build the object-filter (φ) patterns constraining ``target``."""
        out: list[Pattern] = []

        vals: list[Any] = []
        if value is not None:
            vals.extend(value if isinstance(value, (list, tuple)) else [value])
        if in_ is not None:
            vals.extend(in_)
        if vals:
            out.append(Values(target, tuple(self._term(v) for v in vals)))

        if datatype is not None:
            out.append(DatatypeCmp(target, self._expand(datatype)))

        if is_a is not None:
            classes = is_a if isinstance(is_a, (list, tuple)) else [is_a]
            tpath = self.reasoning.type_path()
            if len(classes) == 1:
                out.append(Exists((Triple(target, tpath, Iri(self._resolve(classes[0], "class"))),)))
            else:
                tvar, counter = self._fresh(counter)
                out.append(
                    Exists(
                        (
                            Triple(target, tpath, tvar),
                            Values(tvar, tuple(Iri(self._resolve(c, "class")) for c in classes)),
                        )
                    )
                )

        if min is not None:
            out.append(Cmp(target, ">=", Lit(min)))
        if max is not None:
            out.append(Cmp(target, "<=", Lit(max)))

        if matching is not None:
            block, counter = _inline(matching._state, target.name, counter)
            out.append(Exists(block))

        return out, counter

    def _branch(self, fn: Callable[["Selection"], "Selection"]) -> tuple[tuple[Pattern, ...], int]:
        """Run a sub-pipeline anchored at the current focus; return its patterns."""
        sub_state = _State(
            patterns=(),
            focus=self._state.focus,
            marks=self._state.marks,
            counter=self._state.counter,
        )
        res = fn(self._with(sub_state))
        return res._state.patterns, res._state.counter

    # ------------------------------------------------------------------
    # navigation
    # ------------------------------------------------------------------
    def having(
        self,
        step: Any,
        *,
        direction: str = "out",
        value: Any = None,
        is_a: Any = None,
        datatype: Any = None,
        min: Any = None,
        max: Any = None,
        in_: Sequence[Any] | None = None,
        matching: "Selection | None" = None,
    ) -> "Selection":
        """Keep only current nodes that *have* an edge ``step`` satisfying φ.

        The narrowing operator: an existential semijoin, so the cursor does not
        move and rows never multiply. ``step`` may be a predicate/path/named
        edge, or a :class:`FacetRow` (from ``facets().row(...)``), in which case
        its direction and object key are taken from the row.
        """
        filters = dict(value=value, is_a=is_a, datatype=datatype, min=min, max=max, in_=in_, matching=matching)
        step, direction, filters = self._facet_row_expand(step, direction, filters)
        path = self._step_to_path(step, direction)
        obj, counter = self._fresh(self._state.counter)
        body: list[Pattern] = [Triple(Var(self._state.focus), path, obj)]
        extra, counter = self._constraints(obj, counter, **filters)
        body.extend(extra)
        new = replace(
            self._state,
            patterns=self._state.patterns + (Exists(tuple(body)),),
            counter=counter,
        )
        return self._with(new)

    def without(self, step: Any, *, direction: str = "out", **filters: Any) -> "Selection":
        """Keep only current nodes that have *no* edge ``step`` satisfying φ.

        Accepts a :class:`FacetRow` as ``step`` like :meth:`having`.
        """
        step, direction, filters = self._facet_row_expand(step, direction, filters)
        path = self._step_to_path(step, direction)
        obj, counter = self._fresh(self._state.counter)
        body: list[Pattern] = [Triple(Var(self._state.focus), path, obj)]
        extra, counter = self._constraints(obj, counter, **filters)
        body.extend(extra)
        new = replace(
            self._state,
            patterns=self._state.patterns + (Exists(tuple(body), negated=True),),
            counter=counter,
        )
        return self._with(new)

    def follow(
        self,
        step: Any,
        *,
        direction: str = "out",
        value: Any = None,
        is_a: Any = None,
        datatype: Any = None,
        min: Any = None,
        max: Any = None,
        in_: Sequence[Any] | None = None,
        matching: "Selection | None" = None,
    ) -> "Selection":
        """Move the cursor to the neighbours reached along ``step`` (adds a column).

        The traversal operator: an image. ``step`` may be a predicate / path /
        named edge, or a :class:`FacetRow` (from ``facets().row(...)``), in which
        case its direction and object key come from the row.
        """
        filters = dict(value=value, is_a=is_a, datatype=datatype, min=min, max=max, in_=in_, matching=matching)
        step, direction, filters = self._facet_row_expand(step, direction, filters)
        path = self._step_to_path(step, direction)
        obj, counter = self._fresh(self._state.counter)
        new_patterns: list[Pattern] = [Triple(Var(self._state.focus), path, obj)]
        extra, counter = self._constraints(obj, counter, **filters)
        new_patterns.extend(extra)
        new = replace(
            self._state,
            patterns=self._state.patterns + tuple(new_patterns),
            focus=obj.name,
            counter=counter,
        )
        return self._with(new)

    def where(self, fn: Callable[["Selection"], "Selection"]) -> "Selection":
        """Correlated existential constraint: focus must satisfy the sub-pipeline."""
        body, counter = self._branch(fn)
        new = replace(
            self._state,
            patterns=self._state.patterns + (Exists(body),),
            counter=counter,
        )
        return self._with(new)

    def any_of(self, *fns: Callable[["Selection"], "Selection"]) -> "Selection":
        """Disjunctive constraint: focus must satisfy at least one sub-pipeline."""
        if not fns:
            raise ValueError("any_of: provide at least one branch")
        branches: list[tuple[Pattern, ...]] = []
        counter = self._state.counter
        for fn in fns:
            sub_state = _State(
                patterns=(), focus=self._state.focus, marks=self._state.marks, counter=counter
            )
            res = fn(self._with(sub_state))
            branches.append(res._state.patterns)
            counter = res._state.counter
        new = replace(
            self._state,
            patterns=self._state.patterns + (OrExists(tuple(branches)),),
            counter=counter,
        )
        return self._with(new)

    # ------------------------------------------------------------------
    # focus filters (φ applied to the current node)
    # ------------------------------------------------------------------
    def is_a(self, cls: Any) -> "Selection":
        """Keep only current nodes whose type is ``cls`` (or a subclass)."""
        extra, counter = self._constraints(Var(self._state.focus), self._state.counter, is_a=cls)
        new = replace(self._state, patterns=self._state.patterns + tuple(extra), counter=counter)
        return self._with(new)

    def is_one_of(self, *uris: Any) -> "Selection":
        """Restrict the current focus to specific nodes (URI/CURIE/name)."""
        terms = tuple(Iri(self._resolve(u, None)) for u in uris)
        new = replace(self._state, patterns=self._state.patterns + (Values(Var(self._state.focus), terms),))
        return self._with(new)

    def in_range(self, *, min: Any = None, max: Any = None) -> "Selection":
        """Constrain the current (literal) focus to a numeric range."""
        pats: list[Pattern] = []
        if min is not None:
            pats.append(Cmp(Var(self._state.focus), ">=", Lit(min)))
        if max is not None:
            pats.append(Cmp(Var(self._state.focus), "<=", Lit(max)))
        new = replace(self._state, patterns=self._state.patterns + tuple(pats))
        return self._with(new)

    # ------------------------------------------------------------------
    # waypoints
    # ------------------------------------------------------------------
    def mark(self, name: str) -> "Selection":
        """Name the current focus column so it can be returned or revisited."""
        marks = dict(self._state.marks)
        marks[name] = self._state.focus
        return self._with(replace(self._state, marks=marks))

    def to(self, name: str) -> "Selection":
        """Move the cursor back to a previously marked column."""
        if name not in self._state.marks:
            raise KeyError(f"to: no mark named {name!r} (have: {sorted(self._state.marks)})")
        return self._with(replace(self._state, focus=self._state.marks[name]))

    # ------------------------------------------------------------------
    # facets
    # ------------------------------------------------------------------
    def facets(
        self,
        by: str = "predicate",
        *,
        direction: str = "both",
        limit: int = 50,
        only: Sequence[str] | None = None,
        hide: Sequence[str] | None = None,
        raw: bool = False,
        virtual: bool = True,
    ) -> "Facets":
        """Summarise the neighbourhood of the current nodes — the next moves.

        ``by`` is one of ``"predicate"``, ``"pred-obj"``, ``"pred-obj-type"``.
        ``direction`` is ``"out"``, ``"in"``, or ``"both"``.

        The active :class:`Profile` (if any) curates which predicates/types
        appear and surfaces its named virtual edges as extra rows. Override
        per call with ``only=`` (allow) / ``hide=`` (deny), disable virtual-edge
        rows with ``virtual=False``, or ignore the profile entirely with
        ``raw=True``.
        """
        from .facets import compute_facets

        return compute_facets(
            self,
            by=by,
            direction=direction,
            limit=limit,
            only=only,
            hide=hide,
            raw=raw,
            virtual=virtual,
        )

    # ------------------------------------------------------------------
    # compilation / terminals
    # ------------------------------------------------------------------
    def _col_var(self, name: str) -> str:
        if name in ("focus", self._state.focus):
            return self._state.focus
        if name in self._state.marks:
            return self._state.marks[name]
        raise KeyError(f"unknown column {name!r} (marks: {sorted(self._state.marks)})")

    def _where_body(self) -> str:
        return "\n  ".join(p.render() for p in self._state.patterns)

    def to_sparql(self, *columns: str) -> str:
        """Compile this selection to a SPARQL ``SELECT DISTINCT`` query."""
        if columns:
            projections = [(self._col_var(c), c) for c in columns]
        else:
            projections = [(self._state.focus, "focus")]
        select = " ".join(f"(?{v} AS ?{alias})" for v, alias in projections)
        return f"SELECT DISTINCT {select}\nWHERE {{\n  {self._where_body()}\n}}"

    def _run(self, *columns: str) -> dict:
        return self.client.sparql_query(self.to_sparql(*columns), use_union=True)

    def nodes(self) -> list[str]:
        """Return the focus node URIs as a sorted list of strings."""
        res = self._run()
        out: set[str] = set()
        for row in res.get("rows", []):
            if row and isinstance(row[0], str):
                out.add(row[0])
        return sorted(out)

    def count(self) -> int:
        """Number of distinct focus nodes."""
        sparql = (
            f"SELECT (COUNT(DISTINCT ?{self._state.focus}) AS ?count)\n"
            f"WHERE {{\n  {self._where_body()}\n}}"
        )
        res = self.client.sparql_query(sparql, use_union=True)
        rows = res.get("rows", [])
        if not rows:
            return 0
        try:
            return int(rows[0][0])
        except (TypeError, ValueError):
            return 0

    def select(self, *columns: str, compact: bool = True) -> "pl.DataFrame":
        """Project marked columns (+ current focus) into a polars DataFrame."""
        import polars as pl

        if not columns:
            columns = ("focus",)
        res = self._run(*columns)
        cols = res.get("columns", list(columns))
        rows = res.get("rows", [])
        df = pl.DataFrame(rows, schema=list(cols), orient="row")
        if compact:
            # Literal columns already arrive as native Python types (the server
            # calls ``toPython`` on every cell), so only URI-bearing string
            # columns get compacted to ``prefix:local`` — numeric/boolean/date
            # columns keep their dtype. We build the compacted series in Python
            # rather than via the deprecated ``map_elements``; facet result sets
            # are small so the per-cell Python call is not on a hot path. Pass
            # the original dtype explicitly so an all-null String column (an
            # unmatched OPTIONAL) is not re-inferred to Null.
            for c in cols:
                if df.schema.get(c) == pl.String:
                    df = df.with_columns(
                        pl.Series(c, [self._compact(v) for v in df[c].to_list()],
                                   dtype=pl.String)
                    )
        return df

    def frame(self, *, compact: bool = True) -> "pl.DataFrame":
        """Return the focus nodes as a single-column polars DataFrame."""
        return self.select("focus", compact=compact)

    def suggest(self, text: str, kind: str | None = None, *, top_k: int = 5) -> list[dict]:
        """Preview embedding matches for ``text`` — ``[{curie, score, kind}, ...]``."""
        return _suggest(self.client, text, kind, top_k=top_k)

    # ------------------------------------------------------------------
    # data plane (timeseries)
    # ------------------------------------------------------------------
    def data(
        self,
        *,
        start: Any = None,
        end: Any = None,
        limit: int | None = None,
        order: str = "asc",
        cast_value: str | None = "float",
        value_mode: str = "default",
    ) -> "DataObject":
        """Fetch timeseries for the focus **data points** as a :class:`DataObject`.

        The focus nodes must carry ``ref:hasExternalReference`` (i.e. be the
        observable/actuatable property points). Marks on this selection become
        context columns (surfaced under the bare mark name), so
        ``.data().by("<mark>")`` groups the series by that waypoint and the
        narrow ``dataframe`` / ``metadata`` frames carry one column per mark.
        Series are aliased by their compacted point URI. A mark may not reuse a
        reserved data-column name (``time``, ``value_numeric``, …).

        Example::

            (g.instances("nawi:Pump").mark("pump")
               .follow("measures")
               .data(start=t0, end=t1).by("pump"))
        """
        from .data import build_data_object

        return build_data_object(
            self,
            start=start,
            end=end,
            limit=limit,
            order=order,
            cast_value=cast_value,
            value_mode=value_mode,
        )

    def dataframe(
        self,
        *,
        start: Any = None,
        end: Any = None,
        limit: int | None = None,
        order: str = "asc",
        shape: str = "wide",
        cast_value: str | None = "float",
        value_mode: str = "default",
    ) -> "pl.DataFrame":
        """Convenience: fetch timeseries and return a polars DataFrame.

        ``shape="wide"`` gives one column per point; ``"narrow"`` is long-form.
        """
        return self.data(
            start=start, end=end, limit=limit, order=order,
            cast_value=cast_value, value_mode=value_mode,
        ).dataframe(shape=shape)

    def latest_data(self, *, shape: str = "wide", cast_value: str | None = "float") -> "pl.DataFrame":
        """The most recent point per series (wide by default)."""
        return self.dataframe(limit=1, order="desc", shape=shape, cast_value=cast_value)

    def _compact(self, x: Any) -> str | None:
        if x is None:
            return None
        try:
            return self.client.compact_uri(x)
        except Exception:
            return str(x)

    def __repr__(self) -> str:
        marks = ",".join(sorted(self._state.marks)) or "-"
        return f"<Selection focus={self._state.focus} marks=[{marks}] patterns={len(self._state.patterns)}>"


# ---------------------------------------------------------------------------
# module helpers
# ---------------------------------------------------------------------------

# Characters that only occur in SPARQL property-path syntax, never in a bare
# CURIE or natural-language name. A full URI also contains "/", so callers must
# exclude URIs first (see below).
_PATH_META = frozenset("/|()*+?<")


def _is_path_expr(s: str) -> bool:
    """True if ``s`` should be parsed as a property path, not a single predicate.

    A full URI is *not* a path (it contains ``/`` but is one predicate), so it is
    excluded here; angle-bracketed URIs inside a real path start with ``<`` and
    are kept.
    """
    return not _is_uri(s) and any(c in _PATH_META for c in s)


def _iri(client: Any, x: Any) -> str:
    """Resolve a CURIE / URI / URIRef to a full URI string."""
    if isinstance(x, URIRef):
        return str(x)
    s = str(x)
    if _is_uri(s):
        return s
    return client.expand_uri(s)


def _inline(state: _State, target_name: str, counter: int) -> tuple[tuple[Pattern, ...], int]:
    """Rename a selection's patterns so its focus becomes ``target_name``.

    All other variables are remapped to fresh names starting at ``counter`` to
    avoid collisions with the enclosing query. Used for membership joins
    (``matching=``).
    """
    allvars = patterns_vars(state.patterns) | {state.focus}
    mapping: dict[str, str] = {state.focus: target_name}
    for v in sorted(allvars):
        if v == state.focus:
            continue
        mapping[v] = f"n{counter}"
        counter += 1
    renamed = tuple(p.rename(mapping) for p in state.patterns)
    return renamed, counter

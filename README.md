# Acquirium
A Data-Metadata Framework for Water Treatment Plants

Acquirium is a framework for storing, managing, querying, and integrating data and metadata for water treatment systems. It combines knowledge graphs and time series data to support analysis, monitoring, and experimentation.

## Getting Started

#### UV Package Manager

This repository requires having [uv package manager](https://docs.astral.sh/uv/getting-started/installation/)

After installing run this to make sure it's working:

```
uv sync
```

#### Running the Acquirium Server:

To run any example script using Acquirium, you must first start the backend services and Acquirium Server:
```
make up
```

If you want to recreate the instance (will delete all previously inserted data - graphs):

```
make up ACQUIRIUM_RECREATE=true
```
To stop:
```
make down
```

#### Running the WaterTAP and Streaming Simulations:

To run the WaterTAP simulation, streaming simulator, and API examples, start Docker using the WaterTAP profile instead:

```
make watertap-up
```

After that you can run our API example:
```
uv run scripts/api_example.py
```
or the notebook examples
[Example notebook](./notebooks/watertap-single-pump.ipynb)

After you're done, run this to stop containers:

```
make watertap-down
```

Note that every start of watertap workflow recreates the system (all data is lost)

#### Running Tests

To run pytest tests:
```
make test
```

#### Data Persistence Note

By default, every Docker run resets the system. This means all stored data and metadata are deleted when containers restart.

To preserve data across runs, set the following environment variable in compose.yaml:
```
ACQUIRIUM_RECREATE=false
```

## Logging

Acquirium supports insert user logs for each entity in the system. To see how it works check [this script](./scripts/logging_example.py):

```
make up
uv run scripts/logging_example.py
```

## Text Matcher

The application uses a text matcher for mapping natural language input to ontology URIs (classes, predicates, units, and quantity kinds).

The matching algorithm uses **semantic embedding similarity** powered by [FastEmbed](https://github.com/qdrant/fastembed) (default model: `BAAI/bge-small-en-v1.5`). Each ontology concept is represented by one or more surface strings, which are embedded and stored in an in-memory vector index. At query time, the input phrase is embedded and compared against the index using cosine similarity.

#### How the index is built

There are two separate matchers, each with its own embedding index:

1. **Graph matcher** — indexes classes and predicates from user-inserted RDF graphs. Surface strings are derived from `rdfs:label` values and CamelCase/underscore-split local names.
2. **QUDT matcher** — indexes units and quantity kinds from the QUDT ontology (fetched over HTTP with local fallback). Surface strings include `rdfs:label`, `skos:prefLabel`, `skos:altLabel`, symbols, UCUM codes, and split local names.

Both indexes are cached to disk and updated incrementally when graphs change.

#### Querying

Results can be filtered by `kind` (`class`, `predicate`, `unit`, `quantity_kind`) and are ranked by cosine similarity score. Duplicate URIs are deduplicated, keeping the highest-scoring surface match.

[Here's](./scripts/text_matcher_example.py) an example of how the text matcher works.

```
make up
uv run ./scripts/text_matcher_example.py
```

## Improvements
- Acquirium is still under development. We're working on the improvements listed [here](./improvements.md). 

Feel free to open an issue for noticed bugs or new feature ideas!
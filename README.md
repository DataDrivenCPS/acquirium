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
docker compose up -d
```

#### Running the WaterTAP and Streaming Simulations:

To run the WaterTAP simulation, streaming simulator, and API examples, start Docker using the WaterTAP profile instead:

```
docker compose --profile watertap_simulation up -d
```

After that you can run our API example:
```
uv run scripts/api_example.py
```
or the notebook examples
[Example notebook](./notebooks/watertap-single-pump.ipynb)



#### Running Tests

To run pytest tests:
```
docker compose --profile test up -d
uv run pytest tests
```

#### Data Persistence Note

By default, every Docker run resets the system. This means all stored data and metadata are deleted when containers restart.

To preserve data across runs, set the following environment variable in compose.yaml:
```
ACQUIRIUM_RECREATE=false
```




## Text Matcher

*Under Development*

The application uses a text matcher for mapping natural language input to ontology classes or predicates.

The text matching algorithm is lightweight, rule based pipeline designed for short phrases of one to four words. The matching happens between the given input phrase and a lexicon. The lexicon is generated automatically by simple rules that pulls related tags of classes from the ontology. These tags are:

1. Rdfs:labels
2. Local name (removing namespace URI)
3. abbreviation (if longer than 2 words)

[Here's](./scripts/lexicon_builder.py) the lexicon generator.

[Here's](./ontologies/lexicon.json) the lexicon.

[Here's](/scripts/text_matcher_example.py) an example on how text matcher works under the hoods. (Acquirium just uses the first match)

```
uv run ./scripts/text_matcher_example.py
```
### Known Issues:
- Basic abbreviations for common units (KG, M, S) doesn't work
- Equipments that have the same names in multiple ontologies (e.g., nawi:Pump, s223:pump) gives same result
    - Potential solution: matches with equal points, ask user

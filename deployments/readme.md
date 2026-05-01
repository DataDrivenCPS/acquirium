## Guide to External Reference Assignment for Existing Knowledge Graphs

This guide describes how to assign external references to existing knowledge graphs. This is required to connect Acquirium  to concrete data sources such as CSV files, MQTT streams for data ingestion.


### 1. What is an External Reference

An external reference is a node in the knowledge graph that describes how and where data can be retrieved. 

Key characteristics of an external reference in Acquirium:

- It is a first class node in the graph
- It is linked to a data node (e.g. Brick:Point, S223:QuantifiableObservableProperty), not directly to equipment
- It contains retrieval related metadata

This allows the same knowledge graph to be reused across different environments and data backends.

---

### 2. Distinguish Between Data Nodes and External References

When assigning external references, it is important to clearly separate responsibilities:

#### Data nodes represent meaning

A data node represents a measurable or computable quantity, such as temperature, flow rate, pressure, or power. It answers questions like:

- What does this value represent
- What is its unit
- What physical or conceptual context does it belong to

These nodes typically carry metadata such as quantity kind, unit, medium, substance, or enumeration kind.

#### External references represent access

An external reference represents how to retrieve the value. **This part is specific to Acquirium** and won't have a functionality otherwise. It answers questions like:

- Where does the data come from
- How is it accessed
- How should it be parsed

---

### 3. Attach External References Indirectly

External references should never replace or overload the data node itself. Instead:

- The data node links to the external reference using a dedicated predicate such as `hasExternalReference`
- The external reference node contains all access related metadata

This pattern allows:

- Multiple external references for the same data node
- Easy swapping of data sources without changing the semantic model
- Late binding of data during execution

This is especially useful when moving between simulated data and real deployments.

---

### 4. Use One External Reference Type Per Access Pattern

Each external reference node should correspond to a single access pattern.

The external reference schema is borrowed from [Brick Ref]().

Existing external reference types (open an issue or contact us for other!):

- "https://brickschema.org/schema/Brick/ref#FileReference"          : for connecting Parquet or CSV files
- "https://brickschema.org/schema/Brick/ref#MQTTReference"          : for connecting MQTT streams
- "https://brickschema.org/schema/Brick/ref#TimeseriesReference"    : for connection a database server


---
### 5. Reference Specific Triples

This section describes the minimum triples required for each external reference type. The goal is to keep external references declarative and consistent so Acquirium can resolve and ingest data reliably.

#### 5.1 Common Pattern for All Reference Types

`acq = urn:acquirium#`

`ref = https://brickschema.org/schema/Brick/ref#`

Every external reference assignment should include:

- A link from the data node to the reference node  
  `data_node ref:hasExternalReference ref_node`
  `data_node ref:hasFileReference ref_node`
  `data_node ref:hasMQTTReference ref_node`
  `data_node ref:hasTimeseriesReference ref_node`

- A type assertion for the reference node (pick the data format)
  `ref_node a ref:FileReference`
  `ref_node a ref:MQTTReference`  
  `ref_node a ref:TimeseriesReference`  

---

#### 5.2 FileReference

Use a File reference when data is stored in a local or mounted CSV or Parquet file. Acquirium will infer the type of the file.

Required triples for `ref:FileReference`:

- `ref_node a ref:FileReference`
- `ref_node acq:DataSource "..."`            In string format as rdflib.Literal (e.g. "Lab", "SCADA", ...)
- `ref_node ref:fileLocation "..."`          Path in string format as rdflib.Literal
- `ref_node ref:timeColumnID "..."`          Time column name or id (optional, Acquirium infers time column automatically, however if there's ambiguity it will raise an error)
- `ref_node ref:valueColumnID "..."`         Value column name or id (required)

---

#### 5.4 MQTTReference

Use an MQTT reference for live or replayed streaming data. (right now we assume all payload is a dictionary, this will be changed to Sparkplug Schema)

Required triples for `ref:MQTTReference`:

- `ref_node a ref:MQTTReference`
- `ref_node ref:MQTTBroker "..."`         Required. Accepts `host`, `host:port`, or a `mqtt(s)://...` URI (e.g. `localhost:1883`). Defaults to port 1883 when no port is given.
- `ref_node ref:MQTTTopic "..."`          Required
- `ref_node acq:timeKey "Timestamp"`      Acquirium-specific: name of the JSON key carrying the timestamp.
- `ref_node acq:valueKey "Value"`         Acquirium-specific: name of the JSON key carrying the value.

**Important Note:** We currently assume that the incoming message is in json format. If your MQTT stream sends data in other format, please open an issue or contact us!

---

#### 5.5 TimeseriesReference

Use an existing Postgres database to retrieve data (e.g. a historian).

Required triples for `ref:TimeseriesReference`:

- `ref_node a ref:TimeseriesReference`
- `ref_node ref:storedAt "postgresql://user:pass@localhost:5432/dpr"`   The DSN as a literal string. Acquirium recognises `postgresql://` and `postgres://` literals as external Postgres references.

THEN CHOOSE EITHER:
- `ref_node acq:timeseriesQuery "SELECT time, value FROM data WHERE point_uri = 'sensor1' ORDER BY time"`

OR:
- `ref_node acq:timeseriesTable "data"`
- `ref_node acq:timeseriesTimeColumn "time"`     (default: `time`)
- `ref_node acq:timeseriesValueColumn "value"`   (default: `value`)
- `ref_node acq:timeseriesPointFilter "point_uri"` (optional — filters by `point_uri` column in the external table)

**Important Note:**
- The `acq:timeseries*` predicates are Acquirium-specific (not part of the Brick `ref:` schema), since they describe how Acquirium reads the external table.
- When using `acq:timeseriesQuery`, the query must return exactly two columns: the first for timestamps and the second for values.
- When using `acq:timeseriesTable`, the table is expected to have a `point_uri` column if `acq:timeseriesPointFilter` is provided.

---

### 6. Example: Assigning an MQTT External Reference

The following example illustrates the full pattern using a pump work stream. It links a data node to an MQTT reference node and describes how to extract the timestamp and value from JSON payload fields.

```
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix wbs: <urn:ex/> .

wbs:Pump1-in-flow-mass-seawater a s223:QuantifiableObservableProperty ;
    s223:ofMedium nawi:Water-Seawater ;
    qudt:hasQuantityKind qudtqk:MassFlowRate ;
    qudt:hasUnit unit:KiloGM-PER-SEC ;
    ref:hasExternalReference wbs:pump_inlet_flow_mass_seawater_mqtt_ref .

wbs:pump_inlet_flow_mass_seawater_mqtt_ref a ref:MQTTReference ;
    acq:dataSource "SCADA" ;
    ref:MQTTBroker "localhost:1883" ;
    ref:MQTTTopic "saltwater_flow_mass_rate" ;
    acq:timeKey "Timestamp" ;
    acq:valueKey "Value" .
```
---

### 7. Example: Assigning an External Timeseries Reference (Postgres Historian)

The following example links a data node to a `ref:TimeseriesReference` whose `ref:storedAt` is a literal Postgres DSN — Acquirium uses the `postgresql://` prefix to recognise it as an external historian rather than an Acquirium-managed stream. The external table `data` stores rows in `(point_uri, time, value)` format, and `acq:timeseriesPointFilter` selects only the rows matching this sensor.

```
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix wbs: <urn:ex/> .

wbs:Pump1-in-flow-mass-seawater a s223:QuantifiableObservableProperty ;
    s223:ofMedium nawi:Water-Seawater ;
    qudt:hasQuantityKind qudtqk:MassFlowRate ;
    qudt:hasUnit unit:KiloGM-PER-SEC ;
    ref:hasExternalReference wbs:pump_inlet_flow_mass_seawater_pg_ref .

wbs:pump_inlet_flow_mass_seawater_pg_ref a ref:TimeseriesReference ;
    ref:storedAt "postgresql://user:pass@localhost:5432/dpr" ;
    acq:timeseriesTable "data" ;
    acq:timeseriesTimeColumn "time" ;
    acq:timeseriesValueColumn "value" ;
    acq:timeseriesPointFilter "saltwater_flow_mass_rate" .
```

Alternatively, supply a full SQL query (which must return exactly two columns: timestamp first, then value):

```
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix wbs: <urn:ex/> .

wbs:pump_inlet_flow_mass_seawater_pg_ref a ref:TimeseriesReference ;
    ref:storedAt "postgresql://user:pass@localhost:5432/dpr" ;
    acq:timeseriesQuery "SELECT time, value FROM data WHERE point_uri = 'saltwater_flow_mass_rate' ORDER BY time" .
```
---


## Guide to Add Thresholds

Use thresholds to capture regulatory or operational limits for a property. 
A threshold node anchors the context for a property and can either (a) store simple upper/lower bounds directly or (b) link to one or more limit nodes when you need multiple constraints or richer metadata. 
Compliance rules are separate nodes that encode cross-property logic and reference the affected properties explicitly.

To define thresholds for a property:

1. Create a new threshold node with a unique identifier.
2. Link the threshold node to the property node using `urn:acquirium#hasThreshold`.
3. Populate either simple bounds on the threshold or detailed limits as shown below.

The simple threshold form includes:
- Upper limit value (optional) (literal)
- Lower limit value (optional) (literal)
- Unit of measurement (optional) (URI-QUDT unit)
- Description or rationale (optional) (literal)

```
@PREFIX ns1: <urn:acquirium#>
@PREFIX wbs: <urn:ex/> 
wbs:Pump1-in-flow-mass-seawater a s223:QuantifiableObservableProperty ;
    s223:ofMedium nawi:Water-Seawater ;
    qudt:hasQuantityKind qudtqk:MassFlowRate ;
    qudt:hasUnit unit:KiloGM-PER-SEC ;
    ns1:hasThreshold wbs:pump_inlet_flow_mass_seawater_threshold .

wbs:pump_inlet_flow_mass_seawater_threshold a ns1:Threshold ;
    ns1:upper_limit "1000"^^xsd:decimal ;
    ns1:lower_limit "0"^^xsd:decimal ;
    ns1:unit unit:KiloGM-PER-SEC ;
    ns1:description "Operational threshold for pump inlet flow mass seawater." .
```

### Detailed Limits and Compliance Rules

Use limit nodes when a property has multiple constraints (for example, monthly and weekly limits) or when you need extra metadata per limit. 
Use compliance rules for cross-property logic (for example, percent removal), and link them to all affected properties with `acq:appliesTo`.

Predicate quick reference:

Threshold-level context:
- `acq:side` (literal) e.g., "influent", "effluent"
- `acq:monitoring_location` (literal)
- `acq:notes` (literal)
- `acq:hasLimit` (predicate from threshold to limit)

Limit-level details:
- `acq:kind` (literal) e.g., "effluent_limitation", "action_threshold"
- `acq:statistic` (literal) e.g., "average_monthly"
- `acq:operator` (literal) e.g., "<=", ">="
- `acq:value` (literal)
- `acq:unit` (URI-QUDT unit)
- `acq:notes` (literal, optional)
- `acq:original_value` and `acq:original_unit` (literals, optional when the source unit is not a QUDT URI)

Compliance rule details:
- `acq:kind` (literal)
- `acq:statistic` (literal)
- `acq:operator` (literal)
- `acq:value` (literal)
- `acq:unit` (URI-QUDT unit)
- `acq:equivalent_form` (literal, optional)
- `acq:appliesTo` (predicate from compliance rule to property)

Example with multiple limits and a compliance rule:

```
@PREFIX acq: <urn:acquirium#>
@PREFIX wbs: <urn:ex/>

wbs:Effluent_Pump-out-biochemical-oxygen-demand a s223:QuantifiableObservableProperty ;
    acq:hasThreshold wbs:Effluent_Pump-out-biochemical-oxygen-demand_threshold .

wbs:Effluent_Pump-out-biochemical-oxygen-demand_threshold a acq:Threshold ;
    acq:side "effluent" ;
    acq:monitoring_location "EFF-001" ;
    acq:hasLimit wbs:Effluent_Pump-out-biochemical-oxygen-demand_limit_1,
        wbs:Effluent_Pump-out-biochemical-oxygen-demand_limit_2 .

wbs:Effluent_Pump-out-biochemical-oxygen-demand_limit_1 a acq:Limit ;
    acq:kind "effluent_limitation" ;
    acq:statistic "average_monthly" ;
    acq:operator "<=" ;
    acq:value "30"^^xsd:decimal ;
    acq:unit unit:MilliGM-PER-L .

wbs:Effluent_Pump-out-biochemical-oxygen-demand_limit_2 a acq:Limit ;
    acq:kind "effluent_limitation" ;
    acq:statistic "average_weekly" ;
    acq:operator "<=" ;
    acq:value "45"^^xsd:decimal ;
    acq:unit unit:MilliGM-PER-L .

wbs:compliance_rule_1 a acq:ComplianceRule ;
    acq:kind "percent_removal" ;
    acq:statistic "average_monthly" ;
    acq:operator ">=" ;
    acq:value "85"^^xsd:decimal ;
    acq:unit unit:PERCENT ;
    acq:equivalent_form "Effluent monthly mean concentration must be <= 15% of influent monthly mean concentration." ;
    acq:appliesTo wbs:Influent_Pump-in-biochemical-oxygen-demand,
        wbs:Influent_Pump-in-tss-concentration,
        wbs:Effluent_Pump-out-biochemical-oxygen-demand,
        wbs:Effluent_Pump-out-tss-concentration .
```

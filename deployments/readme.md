# Guide to External Reference Assignment for Existing Knowledge Graphs

This guide describes how to assign external references to existing knowledge graphs. This is required to connect Acquirium  to concrete data sources such as CSV files, MQTT streams for data ingestion.


## 1. What is an External Reference

An external reference is a node in the knowledge graph that describes how and where data can be retrieved. 

Key characteristics of an external reference in Acquirium:

- It is a first class node in the graph
- It is linked to a data node (e.g. Brick:Point, S223:QuantifiableObservableProperty), not directly to equipment
- It contains retrieval related metadata

This allows the same knowledge graph to be reused across different environments and data backends.

---

## 2. Distinguish Between Data Nodes and External References

When assigning external references, it is important to clearly separate responsibilities:

### Data nodes represent meaning

A data node represents a measurable or computable quantity, such as temperature, flow rate, pressure, or power. It answers questions like:

- What does this value represent
- What is its unit
- What physical or conceptual context does it belong to

These nodes typically carry metadata such as quantity kind, unit, medium, substance, or enumeration kind.

### External references represent access

An external reference represents how to retrieve the value. **This part is specific to Acquirium** and won't have a functionality otherwise. It answers questions like:

- Where does the data come from
- How is it accessed
- How should it be parsed

---

## 3. Attach External References Indirectly

External references should never replace or overload the data node itself. Instead:

- The data node links to the external reference using a dedicated predicate such as `hasExternalReference`
- The external reference node contains all access related metadata

This pattern allows:

- Multiple external references for the same data node
- Easy swapping of data sources without changing the semantic model
- Late binding of data during execution

This is especially useful when moving between simulated data and real deployments.

---

## 4. Use One External Reference Type Per Access Pattern

Each external reference node should correspond to a single access pattern.

Existing external reference types (open an issue or contact us for other!):

- "urn:acquirium#CSVReference"      : for connecting CSV files
- "urn:acquirium#ParquetReference"  : for connecting Parquet files
- "urn:acquirium#MQTTReference"     : for connecting MQTT streams

---
## 5. Reference Specific Triples

This section describes the minimum triples required for each external reference type. The goal is to keep external references declarative and consistent so Acquirium can resolve and ingest data reliably.

### 5.1 Common Pattern for All Reference Types

`acq = urn:acquirium#`

Every external reference assignment should include:

- A link from the data node to the reference node  
  `data_node acq:hasExternalReference ref_node`

- A type assertion for the reference node (pick the data format)
  `ref_node a acq:CSVReference`  
  `ref_node a acq:MQTTReference`  
  `ref_node a acq:ParquetReference`

---

### 5.2 CSVReference

Use a CSV reference when data is stored in a local or mounted CSV file.

Required triples for `acq:CSVReference`:

- `ref_node a acq:CSVReference`
- `ref_node acq:DataSource "..."`           Path in string format as rdflib.Literal
- `ref_node acq:hasFilePath "..."`          Path in string format as rdflib.Literal
- `ref_node acq:hasTimeColumn <integer>`
- `ref_node acq:hasValueColumn <integer>`

---

### 5.2 ParquetReference

Use a Parquet reference when data is stored in a local or mounted Parquet file.

Required triples for `acq:ParquetReference`:

- `ref_node a acq:ParquetReference`
- `ref_node acq:DataSource "..."`           Path in string format as rdflib.Literal
- `ref_node acq:hasFilePath "..."`          Path in string format as rdflib.Literal
- `ref_node acq:hasTimeColumn <integer>`
- `ref_node acq:hasValueColumn <integer>`

---

### 5.4 MQTTReference

Use an MQTT reference for live or replayed streaming data.

Required triples for `acq:MQTTReference`:

- `ref_node a acq:MQTTReference`
- `ref_node acq:Broker "..."`           (e.g. `localhost`)
- `ref_node acq:Port <integer>`      
- `ref_node acq:Topic "..."`
- `ref_node acq:time_key "Timestamp"`
- `ref_node acq:value_key "Value"`

**Important Note:** We currently assume that the incoming message is in json format. If your MQTT stream sends data in other format, please open an issue or contact us!

---

## 6. Example: Assigning an MQTT External Reference

The following example illustrates the full pattern using a pump work stream. It links a data node to an MQTT reference node and describes how to extract the timestamp and value from JSON payload fields.

```
wbs:Pump1-in-flow-mass-seawater a s223:QuantifiableObservableProperty ;
    s223:ofMedium nawi:Water-Seawater ;
    qudt:hasQuantityKind qudtqk:MassFlowRate ;
    qudt:hasUnit unit:KiloGM-PER-SEC ;
    ns1:hasExternalReference wbs:pump_inlet_flow_mass_seawater_mqtt_ref .

wbs:pump_inlet_flow_mass_seawater_mqtt_ref a ns1:MQTTReference ;
    ns1:Broker "localhost" ;
    ns1:Port "1883" ;
    ns1:Topic "saltwater_flow_mass_rate" ;
    ns1:time_key "Timestamp" ;
    ns1:value_key "Value" .
```
---


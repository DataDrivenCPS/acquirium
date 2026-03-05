# Acquirium Improvement Plan

## Note

Immediate Improvements are items that are currently under active development.

If you have suggestions or improvement ideas, feel free to open an issue, submit a pull request, or contact us by email at [saka@mines.edu](mailto:saka@mines.edu).

---
---

## Work in Progress 

These are the improvements are currently under active development

### Infrastructure and Architecture
- [x] Text matcher improvements
    - [x] Explore small embedding models similar to RDF-MCP work
    - [x] Text matcher removed from client and moved to server as a service. This allows all computed properties to access in a simpler fashion.
 
### Registering WaterTAP Flowsheets in Acquirium
- [ ] Convert WaterTAP flowsheets into a knowledge graph
- [ ] Automatically detect input and output requirements
- [ ] Retrieve required inputs and calculation outputs
    
    We had a meeting with WaterTap related experts and received some feedback:
    - [ ] Current implementation (which resides in another repo and should be moved to here) is too much hard coded and not robust against watertap code updates.
    - [ ] We might extract property metadata programmatically and reduce hard coding of each equipment
    - [ ] The experts are also interested KG --> Watertap tool


---
---

## Future Improvements

### Visualization and Monitoring
- [ ] Grafana dashboard generation
    - [ ] Automatically generate dashboards using the soft sensor API
- [ ] More visualization api for incremental query building

### Infrastructure and Architecture
- [ ] Change union_graph, main_graph structure to named graphs. We should be able to support individual graph updates (e.g. new version of watr should only replace old version of watr)
- [ ] Robust traversal in the plant metadata considering plant topology (not graph topology)
    - [ ] For instance finding upstream downstream equipment, while being careful with cycles
- [ ] Documenting data provenance:
    - [ ] Soft sensor dependencies
    - [ ] Data source specification
- [ ] Build an MCP server for Acquirium
- [ ] Reduce overhead and improve security of the computed property execution environment
    - [ ] Limit Docker resources
    - [ ] Explore alternatives: Podman, Ray, Wasmer, Firecracker
- [ ] Add API endpoints for easier query graph adjustments (filtering)
- [ ] Support schema-based inputs for data objects
    - [ ] Require ambiguity resolution when multiple external references exist
    - [ ] Provide simple resolution mechanisms (for example, enumerated external references)
- [ ] Implement computed property auto-triggers
    - [ ] Update downstream soft sensors when underlying values change instead of using fixed schedules
- [ ] Support multi-step applications (for example, backwash cycles)
    - [ ] Define multiple computed properties connected through boolean or enum outputs
- [ ] Application requirements satisfaction (similar to Mortar and Seeq)
    - [ ] or providing multiple definitions for an app if one is not available
- [ ] Unit conversion tool
    - [ ] This might be implemented through soft sensor
    - [ ] Also should be supported in Grafana 
- [ ] Improve testing mechanism:
    - [ ] Use github actions 
    - [ ] Write unit tests that doesn’t require backend running (or create a fake backend)



### Examples and Demos
- [ ] Provide example scripts for users with existing models
    - [ ] Threshold detection
    - [ ] Multi-step event detection
    - [ ] Fault Detection and Isolation (FDI)
    - [ ] Sensor drift detection
    - [ ] Prediction
- [ ] Support NAWI knowledge graph and data integration efforts


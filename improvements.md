# Acquirium Improvement Plan

## Note

Immediate Improvements are items that are currently under active development.

If you have suggestions or improvement ideas, feel free to open an issue, submit a pull request, or contact us by email at [saka@mines.edu](mailto:saka@mines.edu).

---

## Immediate Improvements

### Registering WaterTAP Flowsheets in Acquirium
- Convert WaterTAP flowsheets into a knowledge graph
- Automatically detect input and output requirements
- Retrieve required inputs and calculation outputs

---

## Suggested Improvements

### Visualization and Monitoring
- Grafana dashboard generation
    - Automatically generate dashboards using the soft sensor API
- More visualization api for incremental query building

### Infrastructure and Architecture
- Build an MCP server for Acquirium
- Reduce overhead and improve security of the computed property execution environment
    - Limit Docker resources
    - Explore alternatives: Podman, Ray, Wasmer, Firecracker
- Add API endpoints for easier query graph adjustments (filtering)
- Support schema-based inputs for data objects
    - Require ambiguity resolution when multiple external references exist
    - Provide simple resolution mechanisms (for example, enumerated external references)
- Implement computed property auto-triggers
    - Update downstream soft sensors when underlying values change instead of using fixed schedules
- Support multi-step applications (for example, backwash cycles)
    - Define multiple computed properties connected through boolean or enum outputs
- Application requirements satisfaction (similar to Mortar and Seeq)
    - or providing multiple definitions for an app if one is not available
- Text matcher improvements
    - Explore small embedding models similar to RDF-MCP work

### Examples and Demos
- Provide example scripts for users with existing models
    - Threshold detection
    - Multi-step event detection
    - Fault Detection and Isolation (FDI)
    - Sensor drift detection
    - Prediction
- Support NAWI knowledge graph and data integration efforts

# WaterTap + WaTr 

## Introduction

In this work, we modeled a simple pump using watertap and simulated it over a period of time. 

We create a watr model of the same system with data connectors.

We access the data using graframe + acquirium

## WaterTap Info

- WaterTap is a py package for water treatement unit models, property packages, simulation, and optimization
- WaterTap uses IDAES for process and chemical engineering which uses Pyomo for mathematical modeling and optimization

### Property Models
- Property model represents the physical and chemical state of the water 
- Examples: Water, NaCl (saline solution), Seawater, coagulation, activated sludge, multi-component etc.

### Unit Models
- Unit model represents and equipment (or equipment group) that has a certain function in wtp
- These can be connected to build a process
- Examples: Pump, Reverse Osmosis, Thickener, Electrolyser

## Progress

- [x] Watertap simulation
- [x] Stream simulator (to generate varying conditions)
- [x] Watertap stream subscription for simulating timesteps
- [x] Watertap stream client for soft sensor
- [x] Model the same system on WaTr using buildingmotif
- [x] Add external references 
- [x] Graframe mqtt connector implementation
- [x] Docker deployment of watertap and mqtt
- [x] Graframe sample script for data access

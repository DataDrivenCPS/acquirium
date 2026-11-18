# Writing a driver

The files of the tutorial page "Writing a driver". Run every command from this folder.

1. Create the example files: `acquirium example init`
2. Start the server, in a terminal of its own: `acquirium server`
   The CSV driver of `acquirium.toml` starts with it and reads `example/data.csv`.
3. Start the tank driver: `acquirium driver start tank.toml`
4. Replace the CSV driver by the one that inserts the plant model and connects the streams to it:
   `acquirium driver stop --name CSVIngestDriver`, then `acquirium driver start plant.toml`
5. Read the data: `python read_data.py`, or open `read_data.ipynb`

| file | what it is |
|---|---|
| `acquirium.toml` | the server and the built-in CSV driver |
| `tank_driver.py`, `tank.toml` | a polling driver written by hand, and its config |
| `plant_driver.py`, `plant.toml` | a CSV driver that inserts the plant model and connects its streams to it, and its config |
| `mapping.json` | the property of the plant model each stream belongs to, read by `plant_driver.py` |
| `read_data.py`, `read_data.ipynb` | read what the drivers inserted |

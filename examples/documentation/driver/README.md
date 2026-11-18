# Writing a driver

The files of the tutorial page "Writing a driver". Run every command from this folder.

1. Create the example files: `acquirium example init`
2. Start the server, in a terminal of its own: `acquirium server`
   The CSV driver of `acquirium.toml` starts with it and reads `example/data.csv`.
3. Start the tank driver: `acquirium driver start tank.toml`
4. Insert the plant model: `python insert_model.py`
5. Replace the CSV driver by the one that connects the streams to the model:
   `acquirium driver stop --name CSVIngestDriver`, then `acquirium driver start plant.toml`
6. Read the data: `python read_data.py`, or open `read_data.ipynb`

| file | what it is |
|---|---|
| `acquirium.toml` | the server and the built-in CSV driver |
| `tank_driver.py`, `tank.toml` | a polling driver written by hand, and its config |
| `plant_driver.py`, `plant.toml` | a CSV driver that connects its streams to the plant model, and its config |
| `insert_model.py` | inserts the plant model |
| `read_data.py`, `read_data.ipynb` | read what the drivers inserted |

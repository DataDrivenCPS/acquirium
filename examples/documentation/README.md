# Documentation examples

The code of the tutorial pages, as Python scripts and Jupyter notebooks.

- Run pages 1 to 4 in number order, in the same folder. Each one continues with the data of the one before.
- Run `acquirium clear` before page 1, so the results match the pages. It erases the data Acquirium keeps in the current folder.
- Before page 5, run `acquirium clear` again, and create its files with `acquirium example init`.

| page | script | notebook |
|---|---|---|
| 1 - Inserting Data | `1_inserting_data.py` | `1_inserting_data.ipynb` |
| 2 - Querying Data | `2_querying_data.py` | `2_querying_data.ipynb` |
| 3 - Inserting Metadata | `3_inserting_metadata.py` | `3_inserting_metadata.ipynb` |
| 4 - Inserting a Plant Model | `4_inserting_plant_model.py` | `4_inserting_plant_model.ipynb` |
| 5 - A Full Example | `5_a_full_example.py` | `5_a_full_example.ipynb` |

## Advanced topics

| page | files |
|---|---|
| Writing a driver | the `driver/` folder, with its own README |
| Declaring and deploying Apps | `plant_apps.py`, `advanced_apps.py`, `advanced_apps.ipynb` |
| Running experiments | `advanced_experiments.py`, `advanced_experiments.ipynb` |

- The apps and experiments pages continue from page 5. Run them in the same folder, after `5_a_full_example`.
- The driver page uses a server of its own. Run it in the `driver/` folder.

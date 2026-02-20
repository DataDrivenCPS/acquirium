# Use the official Miniconda3 image as the base
FROM continuumio/miniconda3

# Define the environment name
ARG ENV_NAME=watertap


# Build deps for Pyomo extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake build-essential gfortran pkg-config git \
    && rm -rf /var/lib/apt/lists/*

# Create wrappers that drop -m64 (Pyomo extension build passes it, but aarch64 gcc rejects it)
RUN printf '%s\n' \
  '#!/usr/bin/env bash' \
  'args=()' \
  'for a in "$@"; do' \
  '  if [ "$a" != "-m64" ]; then args+=("$a"); fi' \
  'done' \
  'exec /usr/bin/gcc "${args[@]}"' \
  > /usr/local/bin/cc && chmod +x /usr/local/bin/cc

RUN printf '%s\n' \
  '#!/usr/bin/env bash' \
  'args=()' \
  'for a in "$@"; do' \
  '  if [ "$a" != "-m64" ]; then args+=("$a"); fi' \
  'done' \
  'exec /usr/bin/g++ "${args[@]}"' \
  > /usr/local/bin/c++ && chmod +x /usr/local/bin/c++

# 1. Create the Conda environment (Python 3.11)
# Ref: https://watertap.readthedocs.io/en/stable/getting_started.html#using-conda-environments
RUN conda create --name ${ENV_NAME} --yes python=3.11

# 2. "Activate" the environment
# By adding the environment's bin directory to the front of PATH, 
# we ensure that 'python', 'pip', and 'idaes' commands use this environment by default.
ENV PATH /opt/conda/envs/${ENV_NAME}/bin:$PATH

# Also ensure the environment is activated if a user logs in interactively
RUN echo "source activate ${ENV_NAME}" >> ~/.bashrc

# Install the Ipopt executable into the conda environment (so Pyomo can find it)
RUN conda install -n ${ENV_NAME} -y -c conda-forge ipopt && conda clean -afy

# 3. Install WaterTAP
RUN pip install --no-cache-dir watertap paho-mqtt pybind11
RUN pip install git+https://github.com/NREL/BuildingMOTIF.git@develop
RUN pip install streamlit pydantic


# 4. Install IDAES Extensions (Solvers like Ipopt)
# Ref: "After installing WaterTAP... run: idaes get-extensions"
RUN idaes get-extensions

RUN pyomo download-extensions
RUN pyomo build-extensions

# Set a working directory for your project files
WORKDIR /workspace

# Copy your simulation scripts into the image
COPY ./scripts /workspace
COPY ./models/test-model.ttl /workspace/test-model.ttl


CMD ["streamlit", "run", "/workspace/input_gui.py"]

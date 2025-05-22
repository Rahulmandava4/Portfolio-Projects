#!/bin/bash

# Used internally for the jupyter docker container - don't run manually

sudo apt update
sudo apt install -y openjdk-8-jdk

pip install pyspark scikit-learn numpy pandas tqdm pyarrow matplotlib
start-notebook.sh --NotebookApp.token=''

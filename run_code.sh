#!/bin/bash

# Function to print error message and exit
error_exit() {
    echo "[ERROR] $1" 1>&2
    exit 1
}

# Function to run the setup_venv.sh script
run_setup_venv() {
    echo "Virtual environment 'venv' not found. Running setup_venv.sh to create it..."
    ./setup_venv.sh || error_exit "Failed to set up the virtual environment."
}

# Check if venv directory exists
if [ ! -d "venv" ]; then
    run_setup_venv
fi

# Activate the virtual environment
source venv/bin/activate || error_exit "Failed to activate the virtual environment."

# Check if the main.py file exists
if [ ! -f "main.py" ]; then
    error_exit "main.py not found in the current directory."
fi

# Run the Python script and capture its exit status
python3 main.py
python_exit_status=$?

# Deactivate the virtual environment
deactivate

# Check if Python script execution was successful
if [ $python_exit_status -ne 0 ]; then
    error_exit "Python script 'main.py' failed with exit status $python_exit_status."
else
    echo "Script executed successfully."
fi


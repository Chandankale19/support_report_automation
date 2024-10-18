#!/bin/bash

# Function to print error message and exit
error_exit() {
    echo "[ERROR] $1" 1>&2
    exit 1
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to install python3 if not installed
install_python3() {
    if ! command_exists python3; then
        echo "Python3 is not installed. Installing..."
        sudo apt-get update || error_exit "Failed to update package list."
        sudo apt-get install -y python3 || error_exit "Failed to install Python3."
    else
        echo "Python3 is already installed."
    fi
}

# Function to install pip3 if not installed
install_pip3() {
    if ! command_exists pip3; then
        echo "pip3 is not installed. Installing..."
        sudo apt-get install -y python3-pip || error_exit "Failed to install pip3."
    else
        echo "pip3 is already installed."
    fi
}

# Function to create a virtual environment and install dependencies
create_venv() {
    echo "Creating virtual environment..."
    python3 -m venv venv || error_exit "Failed to create virtual environment."

    echo "Activating virtual environment..."
    source venv/bin/activate || error_exit "Failed to activate the virtual environment."

    if [ ! -f "requirements.txt" ]; then
        error_exit "requirements.txt not found. Cannot install dependencies."
    fi

    echo "Installing dependencies from requirements.txt..."
    pip3 install -r requirements.txt || error_exit "Failed to install dependencies."

    echo "Virtual environment created and dependencies installed successfully."
}

# Check if Python3 is installed, if not, install it
install_python3

# Check if pip3 is installed, if not, install it
install_pip3

# Check if venv directory exists
if [ ! -d "venv" ]; then
    echo "Virtual environment 'venv' not found. Creating a new one..."
    create_venv
else
    echo "Virtual environment 'venv' already exists."
fi

echo "Setup completed successfully."

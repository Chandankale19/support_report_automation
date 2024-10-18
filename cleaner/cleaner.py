# scripts/cleaner.py
import os
import pandas as pd
import numpy as np
import shutil
import sys
import logging
import time

class DataFrameCleaner:
    def __init__(self, log_directory='logs'):
        """
        Initialize the DataFrameCleaner with logging configuration.
        
        Args:
            log_directory (str): Directory to store log files.
        """
        self.setup_logging(log_directory)

    def setup_logging(self, log_directory):
        """
        Set up logging configuration.
        
        Args:
            log_directory (str): Directory to store log files.
        """
        os.makedirs(log_directory, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(log_directory, 'processor.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def remove_columns(self, df, columns_to_remove):
        """
        Remove specified columns from the DataFrame.

        Parameters:
        df (pd.DataFrame): The DataFrame from which columns are to be removed.
        columns_to_remove (list): List of column names to be removed.

        Returns:
        pd.DataFrame: The DataFrame with specified columns removed.
        """
        # Validate that df is a pandas DataFrame
        if not isinstance(df, pd.DataFrame):
            logging.error("The input is not a pandas DataFrame.")
            raise ValueError("Input must be a pandas DataFrame.")

        # Validate that columns_to_remove is a list
        if not isinstance(columns_to_remove, list):
            logging.error("The columns_to_remove parameter is not a list.")
            raise ValueError("columns_to_remove must be a list of column names.")

        # Validate that the columns_to_remove are in the DataFrame
        missing_columns = [col for col in columns_to_remove if col not in df.columns]
        if missing_columns:
            logging.warning(f"The following columns were not found in the DataFrame and will not be removed: {missing_columns}")

        try:
            # Use the drop() method to remove columns
            df = df.drop(columns=columns_to_remove, axis=1, errors='ignore')
            logging.info(f"Removed columns: {columns_to_remove}")
        except Exception as e:
            logging.error(f"Error removing columns: {str(e)}")
            raise

        return df

    def split_timestamp(self, df):
        """
        Process the DataFrame by splitting 'Created Time' into date and time.

        Parameters:
        df (pd.DataFrame): The DataFrame to process.

        Returns:
        pd.DataFrame: The modified DataFrame with new date and time columns.
        """
        # Validate that df is a pandas DataFrame
        if not isinstance(df, pd.DataFrame):
            logging.error("The input is not a pandas DataFrame.")
            raise ValueError("Input must be a pandas DataFrame.")

        # Check if 'Created Time' column exists in the DataFrame
        if 'Created Time' not in df.columns:
            logging.error("'Created Time' column not found in the DataFrame.")
            raise KeyError("'Created Time' column is required.")

        try:
            # Trim whitespace from 'Created Time'
            df['Created Time'] = df['Created Time'].str.strip()

            # Step 1: Split 'Created Time' into date and time
            split_col = df['Created Time'].astype(str).str.split(" ", n=1, expand=True)

            # Step 2: Create new columns 'Ticket Creation Date' and 'Ticket Creation Time'
            df['Ticket Creation Date'] = split_col[0]  # The first part is the date
            df['Ticket Creation Time'] = split_col[1]  # The second part is the time

            # Step 3: Convert 'Ticket Creation Date' to datetime
            df['Ticket Creation Date'] = pd.to_datetime(df['Ticket Creation Date'], errors='coerce')

            # Step 4: Convert 'Ticket Creation Time' to a proper time data type
            df['Ticket Creation Time'] = pd.to_datetime(df['Ticket Creation Time'], format='%H:%M:%S', errors='coerce').dt.time

            logging.info("Successfully processed DataFrame and created new date and time columns.")
        except Exception as e:
            logging.error(f"Error processing DataFrame: {str(e)}")
            raise

        return df

    def impute_null_values(self, df, imputation_values):
        """
        Impute missing values in specified columns for a single DataFrame.
        Converts columns to string, replaces certain patterns with NaN,
        and fills NaN with specified imputation values.

        Parameters:
        df (pd.DataFrame): The DataFrame to process.
        imputation_values (dict): A dictionary where keys are column names and values are the imputation values.

        Returns:
        pd.DataFrame: The DataFrame with imputed values.
        """
        # Validate that df is a pandas DataFrame
        if not isinstance(df, pd.DataFrame):
            logging.error("The input is not a pandas DataFrame.")
            raise ValueError("Input must be a pandas DataFrame.")

        # Validate that imputation_values is a dictionary
        if not isinstance(imputation_values, dict):
            logging.error("The imputation_values parameter is not a dictionary.")
            raise ValueError("imputation_values must be a dictionary.")

        for column, replacement_value in imputation_values.items():
            # Ensure the column exists in the DataFrame
            if column in df.columns:
                try:
                    # Convert the column to string type
                    df[column] = df[column].astype(str)

                    # Replace occurrences of 'nan' and '-' with NaN
                    df[column] = df[column].replace({'nan': np.nan, '-': np.nan})

                    # Fill NaN values with the specified imputation value
                    df[column] = df[column].fillna(replacement_value)

                    logging.info(f"Imputed null values in column '{column}' with value '{replacement_value}'.")
                except Exception as e:
                    logging.error(f"Error processing column '{column}': {str(e)}")
                    raise
            else:
                logging.warning(f"Column '{column}' not found in the DataFrame. Skipping imputation for this column.")

        return df
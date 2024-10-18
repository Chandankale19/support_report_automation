# scripts/main.py
import os
import pandas as pd
import sys
import logging
# Add the parent directory to the system path to import processor
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from cleaner.cleaner import DataFrameCleaner
from processor.processor import TicketProcessor
import shutil


# List of columns to remove
columns_to_remove = [
    'Hardware ID/Serial Number', 'Child Ticket Count', 'Similar issue Count', 'State Name',
    'UDISE_Code', 'Block_Name', 'School_Code', 'School_Name', 'ICT-Reported-Problems',
    'Infra-Status', 'NIC Code', 'Server A or B', 'Block', 'UDISE Code', 'Block Name',
    'District Names', 'Admin Code/School Code', 'Project Code', 'Project Name',
    'Partner Name', 'Device Model Number', 'School Name', 'District', 'FMS Email',
    'Reason for On-Hold', 'Select Your Issue', 'Steps Performed', 'Actual Problem',
    'Classification', 'Time to Respond', 'Team', 'Team Id', 'Tags', 'Ticket On Hold Time',
    'Category', 'Sub Category', 'Resolution', 'To Address', 'Account Name', 'Account Id', 'Due Date'
]

# Define the columns and their respective imputation values
imputation_values = {
    'Problem Reported': 'Problem not found',
    'Bot Name': 'Unidentified Bot Name'
}


def setup_logging(log_directory='logs', log_level=logging.INFO):
    """
    Set up logging configuration.

    Args:
        log_directory (str): Directory to store log files.
        log_level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    """
    # Create the log directory if it doesn't exist
    os.makedirs(log_directory, exist_ok=True)

    # Configure the logging settings
    logging.basicConfig(
        filename=os.path.join(log_directory, 'processor.log'),
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Logging is set up successfully.")

def validate_csv_file_path(file_path):
    """Validate if the given file path exists and is a CSV file.

    Args:
        file_path (str): The path to the CSV file to validate.

    Returns:
        bool: True if the file path exists and is a CSV file, False otherwise.
    """
    # Check if the file_path is a string
    if not isinstance(file_path, str):
        logging.error("The provided file path is not a string.")
        sys.exit(1)  # Exit with status code 1 for invalid input

    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            sys.exit(1)  # Exit with status code 1 for file not found

        # Check if the file has a .csv extension
        if not file_path.lower().endswith('.csv'):
            logging.error(f"File is not a CSV: {file_path}")
            sys.exit(1)  # Exit with status code 1 for invalid file type
        
        # Additional check: Ensure the file is not a directory
        if os.path.isdir(file_path):
            logging.error(f"Provided path is a directory, not a file: {file_path}")
            sys.exit(1)  # Exit with status code 1 for directory path

        logging.info(f"Validated CSV file path: {file_path}")
        return True

    except Exception as e:
        logging.error(f"An error occurred while validating the CSV file path: {str(e)}")
        sys.exit(1)  # Exit with status code 1 for unexpected errors


def csv_to_df(file_path):
    """Process a single CSV file by loading it into a Pandas DataFrame."""
    try:
        # Validate file path before processing
        if not validate_csv_file_path(file_path):
            return None
        
        # Read CSV file into pandas DataFrame
        df = pd.read_csv(file_path)
        logging.info(f"Successfully loaded file: {file_path} with {len(df)} rows.")

        # Create an instance of DataFrameCleaner
        cleaner = DataFrameCleaner()
        df = cleaner.remove_columns(df, columns_to_remove)
        df = cleaner.split_timestamp(df)
        df = cleaner.impute_null_values(df, imputation_values)
        

        return df
    
    except pd.errors.EmptyDataError:
        logging.error(f"File is empty: {file_path}")
    except pd.errors.ParserError:
        logging.error(f"Error parsing CSV file: {file_path}")
    except Exception as e:
        logging.error(f"Unexpected error while processing {file_path}: {str(e)}")
    
    return None

def process_csv_files_in_folder(input_folder):
    """Process all CSV files in the specified folder one by one."""
    if not os.path.isdir(input_folder):
        logging.error(f"Invalid folder path: {input_folder}")
        return

    # Loop through all files in the input folder
    for file_name in os.listdir(input_folder):
        file_path = os.path.join(input_folder, file_name)
        
        # Process only CSV files
        if file_name.endswith('.csv'):
            logging.info(f"Loading file: {file_name}")
            # Read CSV file into pandas DataFrame
            df = pd.read_csv(file_path)
            logging.info(f"Successfully loaded file: {file_path} with {len(df)} rows.")

            # Create an instance of DataFrameCleaner
            cleaner = DataFrameCleaner()
            df = cleaner.remove_columns(df, columns_to_remove)
            df = cleaner.split_timestamp(df)
            main_df = cleaner.impute_null_values(df, imputation_values)

            processor = TicketProcessor()
            current_week_df = processor.filter_last_8_days(df, date_column='Ticket Creation Date', current_date=None)
            bot_name_list = list(main_df['Bot Name'].unique())
            splitted_df_dict = processor.split_dataframe_by_bot_name(current_week_df,bot_name_list)
            splitted_df_key_list = list(splitted_df_dict.keys())

            comparison_values = list(main_df['Mode'].unique())
            column_name='Mode'
            total_ticket_received_count_dict = processor.total_ticket_received_count(splitted_df_dict,splitted_df_key_list,comparison_values,column_name)
            # print(total_ticket_received_count_dict)

            status_column='Status'
            status_value='Closed'
            mode_list = list(main_df['Mode'].unique())
            column_name = 'Mode'
            total_closed_ticket_dict = processor.count_tickets_for_unique_modes(splitted_df_dict,splitted_df_key_list,mode_list,status_column,status_value)

            comparison_values = list(main_df['Mode'].unique())
            column_name='Mode'
            total_ticket_received_count_dict_modewise = processor.total_ticket_received_count_modewise(splitted_df_dict, splitted_df_key_list, comparison_values,column_name)

            status_column='Status'
            status_value='Open'
            column_name = 'Mode'
            total_open_ticket_dict = processor.count_tickets_for_unique_modes(splitted_df_dict,splitted_df_key_list,mode_list,status_column,status_value)

            status_column='Status'
            status_value='On Hold'
            column_name = 'Mode'
            total_onhold_ticket_dict = processor.count_tickets_for_unique_modes(splitted_df_dict,splitted_df_key_list,mode_list,status_column,status_value)
            # print(total_onhold_ticket_dict)

            # Total Close Tickets {Email + Phone + Web + Chat}
            status_column='Status'
            status_value='Closed'
            column_name = 'Mode'
            total_closed_ticket_dict = processor.count_bot_wise_status_tickets(splitted_df_dict,splitted_df_key_list,mode_list,status_column,status_value)
            # print(total_closed_ticket_dict)

            # Total Open Tickets {Email + Phone + Web + Chat}
            status_column='Status'
            status_value='Open'
            column_name = 'Mode'
            total_open_ticket_dict = processor.count_bot_wise_status_tickets(splitted_df_dict,splitted_df_key_list,mode_list,status_column,status_value)
            # print(total_open_ticket_dict)

            # Total On Hold Tickets {Email + Phone + Web + Chat}
            status_column='Status'
            status_value='On Hold'
            column_name = 'Mode'
            total_onhold_ticket_dict = processor.count_bot_wise_status_tickets(splitted_df_dict,splitted_df_key_list,mode_list,status_column,status_value)
            # print(total_onhold_ticket_dict)

            last_archive_df = processor.filter_before_last_8_days(main_df)

            splitted_df_dict_last = processor.split_dataframe_by_bot_name(last_archive_df,bot_name_list)

            # Total Open Tickets {Email + Phone + Web + Chat}
            status_column='Status'
            status_value='Open'
            splitted_df_key_list = list(splitted_df_dict_last.keys())
            column_name = 'Mode'
            total_archive_open_ticket_dict = processor.count_bot_wise_status_tickets(splitted_df_dict_last,splitted_df_key_list,mode_list,status_column,status_value)
            # print(total_archive_open_ticket_dict)

            # Create a mapping of variable names to the actual dictionaries
            dict_mapping = {
                'Total Closed Tickets (Current Week)': total_closed_ticket_dict,
                'Total Open Tickets (Current Week)': total_open_ticket_dict,
                'Total On Hold Tickets (Current Week)': total_onhold_ticket_dict,
                'Total Archive Open Tickets': total_archive_open_ticket_dict
            }

            # Define a mode-wise pivot dictionary (You can replace this with the actual one)
            mode_wise_data_dict = total_ticket_received_count_dict_modewise
            # File path for saving the Excel report
            output_file_path = 'data/output/Report.xlsx'
            processor.create_excel_with_pivot_and_dicts(mode_wise_data_dict, dict_mapping,output_file_path)

            archive_folder = "data/archive"

            shutil.move(file_path, archive_folder)
            logging.info(f"Moved file {file_name} to archive: {archive_folder}")

            if df is not None:
                logging.info(f"Processing completed for file: {file_name}")
            else:
                logging.error(f"Failed to process file: {file_name}")
        else:
            logging.info(f"Skipping non-CSV file: {file_name}")

def main():
    input_folder = "data/input"
    process_csv_files_in_folder(input_folder)

    # Create an instance of DataFrameCleaner
    cleaner = DataFrameCleaner()

if __name__ == "__main__":
    main()


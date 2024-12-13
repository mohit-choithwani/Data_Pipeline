from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from data_validation_check import validate_data
from data_preprocessing import transform_data
from data_analysis import aggregate_data_by_station, add_metadata_to_grouped_aggregated_data
import time
import pandas as pd
from db_connection import send_data_postgresql
from logger import create_logger
import os

# Creating instance for the logger object
logger = create_logger()

class FileHandler(FileSystemEventHandler):
    """Monitored the directory for new .csv files"""
    def on_created(self, event):
        if event.src_path.endswith('.csv'):
            logger.info(f"New file detected: {event.src_path}")
            print(f"New file detected: {event.src_path}")
            process_file(event.src_path)
    
def process_file(file_path):
    """Processes the newly added .csv file"""
    
    try:
        data = read_csv_to_df(file_path)
        if data.empty:
            logger.info(f"No data found in file: {file_path}")
            return
   
        valid_records = validate_and_transform_data(data)
        
        if valid_records is not None:
            handle_valid_records(valid_records, file_path)
            
    except Exception as error:
        print(f"Error while processing file: {error}")
        logger.error(f"Error while processing file: {error}")

def read_csv_to_df(file_path):
    """Converts csv file into dataframe"""
    try:
        time.sleep(1)
        data = pd.read_csv(file_path, engine='python', encoding='utf-8')
        return data
    except pd.errors.ParserError as e:
        logger.error(f"ParserError: {e}")
        return pd.DataFrame()
    
def handle_valid_records(processed_records, file_path):
    """Perform transformations on valid data"""
    try:
        # get the numeric columns
        numerical_columns = [col for col in processed_records.columns if pd.api.types.is_numeric_dtype(processed_records[col])]
        
        # get the file name
        file_name = os.path.basename(file_path)
        
        # perform aggregation and add metadata
        grouped_data = aggregate_data_by_station(processed_records, numerical_columns)
        
        grouped_data_with_metadata = add_metadata_to_grouped_aggregated_data(grouped_data, file_name)
        
        try:
            logger.info("Uploading data to the PostgreSQL.")
            send_data_postgresql(processed_records, grouped_data_with_metadata)
            
        except Exception as error:
            print(f"Error while sending data to db: {error}")
            logger.error(f"Error while sending data to db: {error}")
        
    except Exception as error:
        logger.error(f"Error while handling valid records: {error}", exc_info=True)

def validate_and_transform_data(data):
    """Validate and transform data"""
    try:
        # Calling validate_data method from data_validation_check file
        valid_records = validate_data(data)
        if valid_records.empty:
            logger.warning("No valid records found.")
            return None
        logger.info("Validation completed.")
        return valid_records
    
    except Exception as error:
        logger.error(f"Error during data validation or transformation: {error}", exc_info=True)
        return None

if __name__ == "__main__":
    # Get the path of the data folder
    script_dir = os.path.dirname(os.path.abspath(__file__)) 
    project_root = os.path.dirname(script_dir)  
    path = os.path.join(project_root, "data")  
    
    observer = Observer()
    observer.schedule(FileHandler(), path=path, recursive=False)
    observer.start()
    print("Monitoring data folder...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

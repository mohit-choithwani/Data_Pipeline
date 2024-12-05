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

logger = create_logger()

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path.endswith('.csv'):
            logger.info(f"New file detected: {event.src_path}")
            print(f"New file detected: {event.src_path}")
            process_file(event.src_path)

def process_file(file_path):
    # Read the CSV file
    data = read_csv_to_df(file_path)
    
    try:
        # Call validate_data function from data_validation_check.py
        valid_records = validate_data(data)
        logger.info
        
    except Exception as error:
        print(f"Error while validating data: {error}")
        logger.error(f"Error while validating data: {error}")
           
    if not(valid_records.empty):
        logger.info("Valid records found.")
        processed_records = transform_data(valid_records)
        
        # get the numeric columns
        numerical_columns = [col for col in processed_records.columns if pd.api.types.is_numeric_dtype(processed_records[col])]
        
        # get the file name without the path
        file_name = file_path.split('\\')[-1]
        
        # perform aggregation and add metadata
        grouped_data = aggregate_data_by_station(processed_records, numerical_columns)
        
        grouped_data_with_metadata = add_metadata_to_grouped_aggregated_data(grouped_data, file_name)
        
        try:
            logger.info("Connected to postgresql")
            send_data_postgresql(processed_records, grouped_data_with_metadata)
            
        except Exception as error:
            print(f"Error while sending data to db: {error}")
            logger.error(f"Error while sending data to db: {error}")

def read_csv_to_df(file_path):
    try:
        time.sleep(1)
        data = pd.read_csv(file_path, engine='python', encoding='utf-8')
        return data
    except pd.errors.ParserError as e:
        logger.error(f"ParserError: {e}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the directory of main.py
    project_root = os.path.dirname(script_dir)  # Navigate one level up to "SeaBreeze_Analytics_Data_Pipeline"
    path = os.path.join(project_root, "data")  # Create the full path to "data"
    observer = Observer()
    observer.schedule(FileHandler(), path=path, recursive=False)
    observer.start()
    print("Monitoring data folder...")
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

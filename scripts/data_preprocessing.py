import numpy as np
import pandas as pd
from logger import create_logger

# Create logger instance
logger = create_logger()

def transform_data(data):
    logger.info("Starting data transformation process.")
    data = check_duplicates(data)
    data = split_timestamp(data, 'Measurement Timestamp')
    logger.info("Data transformation process completed.")
    return data

def check_duplicates(data):
    logger.info("Checking for duplicate rows.")
    duplicate_rows = data.duplicated()
    data = data[~duplicate_rows]
    logger.info(f"Removed {duplicate_rows.sum()} duplicate rows.")
    return data

def split_timestamp(data, timestamp_column):
    logger.info(f"Splitting timestamp column: {timestamp_column}")
    data[timestamp_column] = pd.to_datetime(data[timestamp_column])
    data['date'] = data[timestamp_column].dt.date
    data['time'] = data[timestamp_column].dt.time
    data['day_name'] = data[timestamp_column].dt.day_name()
    logger.info(f"Timestamp column {timestamp_column} split into date, time, and day_name.")
    return data


# def fill_missing_values(data):
#     """
#     Fills missing values in a DataFrame with the mean of the column.
    
#     Args:
#         data (pd.DataFrame): The input DataFrame.
    
#     Returns:
#         pd.DataFrame: A DataFrame with missing values filled.
#     """
#     # Fill missing values with the mean of the column
#     data.fillna(data.mean(), inplace=True)
    
#     return data
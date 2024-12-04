import numpy as np
import pandas as pd

def transform_data(data):
    
    # Methods to check for missing values, data types, value ranges and timestamps
    data = check_duplicates(data)
    # fill_missing_values(data)
    data = split_timestamp(data, 'Measurement Timestamp')
    
    return data

def check_duplicates(data):
    """
    Checks for and removes duplicate rows in a DataFrame.
    
    Args:
        data (pd.DataFrame): The input DataFrame.
    
    Returns:
        pd.DataFrame: A DataFrame with duplicates removed.
    """
    # Check for duplicates in the data
    duplicate_rows = data.duplicated()
    
    # Remove duplicates
    data = data[~duplicate_rows]
    
    return data

def split_timestamp(data, timestamp_column):
    """
    Splits a timestamp column into separate date, time, and day_name columns.
    
    Args:
        data (pd.DataFrame): The input DataFrame.
        timestamp_column (str): The name of the timestamp column in the DataFrame.
    
    Returns:
        pd.DataFrame: A DataFrame with added 'date', 'time', and 'day_name' columns.
    """

    # Convert the timestamp column to datetime if not already
    data[timestamp_column] = pd.to_datetime(data[timestamp_column])
        
    # Extract date, time, and day name
    data['date'] = data[timestamp_column].dt.date
    data['time'] = data[timestamp_column].dt.time
    data['day_name'] = data[timestamp_column].dt.day_name()
        
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
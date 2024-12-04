import pandas as pd
from datetime import datetime

def aggregate_data_by_station(data, numerical_columns):
    """
    Groups the data by 'Station Name' and calculates aggregated metrics for each sensor type within each station.

    :param data: The DataFrame containing the sensor data.
    :param numerical_columns: The list of columns to aggregate (e.g., sensor columns).
    :return: A DataFrame with aggregated metrics for each 'Station Name'.
    """
    
    # Grouping data by 'Station Name'
    grouped_data = data.groupby('Station Name')[numerical_columns].agg(
        ['min', 'max', 'mean', 'std'] 
    )
    
    grouped_data.columns = [f'{col}_{metric}' for col, metric in grouped_data.columns]
    grouped_data.reset_index(inplace=True)
    
    # print(grouped_data.head())
    
    return grouped_data

def add_metadata_to_grouped_aggregated_data(grouped_aggregated_data, file_name):
    """
    Adds metadata (data source, timestamp, file name) to each row in the grouped and aggregated data.

    :param grouped_aggregated_data: The DataFrame containing the aggregated data grouped by 'Station Name'.
    :param file_name: The name of the file (used as metadata).
    :return: A DataFrame with metadata and grouped aggregated data.
    """
    # Add metadata columns directly to the DataFrame
    # grouped_aggregated_data['data_source'] = grouped_aggregated_data['Station Name'] 
    grouped_aggregated_data.rename(columns={'Station Name': 'data_source'}, inplace=True) 
    grouped_aggregated_data['processed_timestamp'] = pd.to_datetime(datetime.now())  
    grouped_aggregated_data['file_name'] = file_name  

    return grouped_aggregated_data

def test():
    print("worked")
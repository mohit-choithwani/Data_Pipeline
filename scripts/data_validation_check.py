import pandas as pd
import os
import shutil
from datetime import datetime
from logger import create_logger

logger = create_logger()

def validate_missing_values(data):
    required_columns = ['Station Name', 'Measurement Timestamp']
    for col in required_columns:
        if pd.isnull(data[col]):
            raise ValueError("Missing values in either 'Station Name' or 'Measurement Timestamp' columns.")
        
def validate_missing_sensor_values(data):
    # list of sensor columns
    sensor_columns = ['Air Temperature',	'Wet Bulb Temperature',	'Humidity',	'Rain Intensity',	'Interval Rain',	'Total Rain', 	
                        'Precipitation Type',	'Wind Direction',	'Wind Speed',	'Maximum Wind Speed',	'Barometric Pressure',	
                        'Solar Radiation',	'Heading',	'Battery Life']

    # Iterate over each row in the DataFrame
    for index, row in data.iterrows():
        # Find which columns have missing values
        missing_cols = [col for col in sensor_columns if pd.isnull(row[col])]
        
        # If there are any missing values, raise an error with the details
        if missing_cols:
            missing_cols_str = ', '.join(missing_cols)
            raise ValueError(f"Missing values in the following column/columns: {missing_cols_str}")
    
def validate_data_types(record):
    if pd.notnull(record['Air Temperature']) and not isinstance(record['Air Temperature'], (int, float)):
        raise TypeError(f"Temperature must be numeric, found: {type(record['Air Temperature'])}.")

def validate_value_ranges(record):
    if pd.notnull(record['Air Temperature']) and not (-50 <= record['Air Temperature'] <= 50):
        raise ValueError(f"Air Temperature {record['Air Temperature']} is out of range (-50 to 50).")
    
def validate_timestamps(record):
    # if not isinstance(record['Measurement Timestamp'], pd.Timestamp):
    try:
        pd.to_datetime(record['Measurement Timestamp'])
    except Exception:
        raise ValueError(f"Invalid timestamp format: {record['Measurement Timestamp']}.")

def validate_value_ranges(record, numerical_cols, data):
    """
    Validates if any value in the given record (row) is an outlier based on the IQR method.
    If any value is an outlier, it raises a ValueError with the column name and the outlier value.

    :param record: A single row (record) of data (Series)
    :param numerical_cols: List of numerical column names to check for outliers
    :raises ValueError: If any value in the record is found to be an outlier
    """
    for col in numerical_cols:
        # Get the value from the record for the current column
        value = record[col]

        # Ensure the value is not null
        if pd.notnull(value):
            # Calculate Q1, Q3 and IQR for the column's values
            Q1 = data[col].quantile(0.25)
            Q3 = data[col].quantile(0.75)
            IQR = Q3 - Q1
            
            # Define the scale (usually 1.5 or 2)
            scale = 2
            lower_lim = Q1 - scale * IQR
            upper_lim = Q3 + scale * IQR

            # Check if the value is an outlier
            if value < lower_lim or value > upper_lim:
                raise ValueError(f"Outlier detected in column '{col}': {value} is out of range ({lower_lim}, {upper_lim})")

    
def quarantine_file(failed_records, error_messages):
    # Create a new folder for the quarantine files
    quarantine_folder = os.path.join("quarantine")
    os.makedirs(quarantine_folder, exist_ok=True)
    
    # Get the current timestamp for unique file naming
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    new_filename = f"{timestamp_str}_failed_records.csv"
    quarantine_file_path = os.path.join(quarantine_folder, new_filename)
    
    # Add the error_message column to the failed records
    failed_records["error_message"] = error_messages

    # Save the updated DataFrame to a CSV file
    failed_records.to_csv(quarantine_file_path, index=False)

    logger.error(f"Failed records saved to: {quarantine_file_path}")
    
def validate_data(data):
    
    # define 3 empty lists to store the valid records, error_records and the error messages
    valid_records = []
    failed_records = []
    error_messages = []
    
    # define the numerical columns
    numerical_cols = ['Air Temperature', 'Wet Bulb Temperature', 'Humidity', 'Rain Intensity', 'Interval Rain', 'Total Rain', 
                        'Wind Direction', 'Wind Speed', 'Maximum Wind Speed', 'Barometric Pressure', 'Solar Radiation', 'Heading', 'Battery Life']
              
    for _, record in data.iterrows():
        try:
            validate_missing_values(record)
            logger.info("Missing values validated")   
            
            validate_data_types(record)
            logger.info("Data types validated")
            
            # validate_value_ranges(record)
            validate_value_ranges(record, numerical_cols, data)
            logger.info("Value ranges validated")
            
            validate_timestamps(record)
            logger.info("Timestamps validated")
            
            valid_records.append(record)
        
        # if any record fails the validation then append error message.
        except Exception as e:    
            failed_records.append(record.to_dict())
            error_messages.append(str(e))
            
    # if there are any failed records then quarantine.
    if failed_records:
        # print(failed_records)
        logger.error(error_messages)
        failed_df = pd.DataFrame(failed_records)
        quarantine_file(failed_df, error_messages)
     
    logger.info(f"Validation completed. {len(valid_records)} valid records found.")   
    return pd.DataFrame(valid_records)

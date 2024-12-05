import pandas as pd
import os
from datetime import datetime
from logger import create_logger

# Create logger instance
logger = create_logger()

def validate_missing_values(record):
    required_columns = ['Station Name', 'Measurement Timestamp']
    for col in required_columns:
        if pd.isnull(record[col]):
            logger.error(f"Missing value in column: {col} for record: {record}")
            raise ValueError(f"Missing values in either 'Station Name' or 'Measurement Timestamp' columns.")

def validate_temperature_range(record):
    if not (-50 <= record['Air Temperature'] <= 100):
        logger.error(f"Air Temperature {record['Air Temperature']} is out of range for record: {record}")
        raise ValueError(f"Air Temperature {record['Air Temperature']} is out of range (-50 to 100).")

def validate_missing_sensor_values(record):
    sensor_columns = ['Air Temperature', 'Wet Bulb Temperature', 'Humidity', 'Rain Intensity', 'Interval Rain', 
                      'Total Rain', 'Precipitation Type', 'Wind Direction', 'Wind Speed', 'Maximum Wind Speed', 
                      'Barometric Pressure', 'Solar Radiation', 'Heading', 'Battery Life']
    missing_cols = [col for col in sensor_columns if pd.isnull(record[col])]
    if missing_cols:
        logger.error(f"Missing sensor values in columns {missing_cols} for record: {record}")
        raise ValueError(f"Missing values in the following columns: {', '.join(missing_cols)}")

def validate_data_types(record):
    if pd.notnull(record['Air Temperature']) and not isinstance(record['Air Temperature'], (int, float)):
        logger.error(f"Temperature data type invalid: {type(record['Air Temperature'])} for record: {record}")
        raise TypeError(f"Temperature must be numeric, found: {type(record['Air Temperature'])}.")

def validate_value_ranges(record, numerical_cols, data):
    for col in numerical_cols:
        value = record[col]
        if pd.notnull(value):
            Q1 = data[col].quantile(0.25)
            Q3 = data[col].quantile(0.75)
            IQR = Q3 - Q1
            scale = 2
            lower_lim = Q1 - scale * IQR
            upper_lim = Q3 + scale * IQR
            if value < lower_lim or value > upper_lim:
                logger.error(f"Outlier detected for {col}: {value} out of range ({lower_lim}, {upper_lim})")
                raise ValueError(f"Outlier detected in column '{col}': {value} is out of range ({lower_lim}, {upper_lim})")

def validate_timestamps(record):
    try:
        pd.to_datetime(record['Measurement Timestamp'])
    except Exception:
        logger.error(f"Invalid timestamp format: {record['Measurement Timestamp']} for record: {record}")
        raise ValueError(f"Invalid timestamp format: {record['Measurement Timestamp']}.")

def quarantine_file(failed_records, error_messages):
    quarantine_folder = os.path.join("quarantine")
    os.makedirs(quarantine_folder, exist_ok=True)
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    new_filename = f"{timestamp_str}_failed_records.csv"
    quarantine_file_path = os.path.join(quarantine_folder, new_filename)
    
    failed_records["error_message"] = error_messages
    failed_records.to_csv(quarantine_file_path, index=False)

    logger.error(f"Failed records saved to: {quarantine_file_path}")

def validate_data(data):
    valid_records = []
    failed_records = []
    error_messages = []
    
    numerical_cols = ['Air Temperature', 'Wet Bulb Temperature', 'Humidity', 'Rain Intensity', 'Interval Rain', 
                      'Total Rain', 'Wind Direction', 'Wind Speed', 'Maximum Wind Speed', 'Barometric Pressure', 
                      'Solar Radiation', 'Heading', 'Battery Life']
    
    for _, record in data.iterrows():
        try:
            validate_missing_values(record)
            validate_temperature_range(record)
            validate_missing_sensor_values(record)
            validate_data_types(record)
            # validate_value_ranges(record, numerical_cols, data)
            validate_timestamps(record)
            valid_records.append(record)
        except Exception as e:
            failed_records.append(record.to_dict())
            error_messages.append(str(e))
    
    if failed_records:
        logger.error(f"Validation failed with {len(failed_records)} records.")
        failed_df = pd.DataFrame(failed_records)
        quarantine_file(failed_df, error_messages)
    
    logger.info(f"Validation completed. {len(valid_records)} valid records found.")   
    return pd.DataFrame(valid_records)

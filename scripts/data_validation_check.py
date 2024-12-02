import pandas as pd
import os
import shutil
from datetime import datetime

def validate_missing_values(data):
    required_columns = ['Station Name', 'Measurement Timestamp']
    if data[required_columns].isnull().any().any():
        raise ValueError("Missing values in required columns.")
    
def validate_data_types(data):
    if not pd.api.types.is_numeric_dtype(data['temperature']):
        raise TypeError("Temperature column must be numeric.")
    
def validate_value_ranges(data):
    if (data['temperature'] < -50).any() or (data['temperature'] > 50).any():
        raise ValueError("Temperature values out of range.")
    
def validate_timestamps(data):
    try:
        data['timestamp'] = pd.to_datetime(data['timestamp'])
    except Exception:
        raise ValueError("Invalid timestamp format.")

def quarantine_file(error_message):
    try:
        # Get the current timestamp
        current_time = datetime.now()
        
        # Format the current timestamp as a string
        timestamp_str = current_time.strftime('%Y%m%d_%H%M%S')
        
        # Create a new quarantine file name
        new_filename = f"{timestamp_str}_error.log"
        quarantine_folder = os.path.join("quarantine")
        
        # Ensure the quarantine folder exists
        os.makedirs(quarantine_folder, exist_ok=True)
        
        # Full path for the quarantine file
        quarantine_file_path = os.path.join(quarantine_folder, new_filename)
        
        # Write the error message to the quarantine file
        with open(quarantine_file_path, "w") as file:
            file.write(f"Validation Error Occurred:\n{error_message}\n")
        
        print(f"Error message logged in: {quarantine_file_path}")
        
    except Exception as e:
        print(f"Error while creating quarantine file: {e}")
    
def validate_data(data):
    try:        
        validate_missing_values(data)
        validate_data_types(data)
        validate_value_ranges(data)
        validate_timestamps(data)
        
        print(f"File passed all validation checks.")
        return data  # Return valid data for further processing
    
    except Exception as e:
        print(f"Validation failed: {e}")
        quarantine_file(str(e))
        return None

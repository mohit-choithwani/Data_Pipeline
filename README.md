# SeaBreeze-Project

In this project, I try to implement real-time data pipelines and associated operations in an efficient manner. It integrates data validation, preprocessing, aggregation, and analysis with logging mechanisms and more.

![Architecture Design](/assests/datapipelinearchi.png)

## Table of Contents

- [Installation](#installation)
- [Dataset](#dataset)
- [Usage](#usage)
- [Future Work](#future-work)
- [License](#license)

## Installation

To set up and install the project locally, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/mohit-choithwani/Data_Pipeline.git
   cd DATA_PIPELINE
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv env
   source env/bin/activate
   ```

3. **Install the dependencies**:
   ```bash
   pip install .
   ```

4. **Run the project**:
   ```bash
   python scripts/main.py
   ```

## Dataset

The dataset used in this project is the **Beach Weather Dataset** from Kaggle. You can access the dataset through the following [link](https://www.kaggle.com/code/sanjanchaudhari/beach-weather-stations-analysis?select=Beach_Weather_Stations_-_Automated_Sensors.csv). This dataset consists of weather measurements collected from beach weather stations, including parameters like air temperature, humidity, wind speed, and precipitation. 

## Database Setup

To store the processed data and aggregated data, a PostgreSQL database is used. You can set up a PostgreSQL database locally or use a cloud-based service like Amazon RDS or Google Cloud SQL. I have set up a PostgreSQL database using Docker for local development. You can follow the steps from this [blog](https://docs.chaicode.com/postgresql-installation/) to set up a PostgreSQL database using Docker. 

Once you have set up the database, you can update the database connection details in the `db_connection.py` file.

## Usage

After executing the script/main.py file, the program will continuously monitor the data folder for any new .csv files. When a .csv file is detected, the program will automatically read the data and perform a series of operations, including data validation, transformation, and analysis. The processed data and the aggregated data will then be uploaded to a PostgreSQL database. All logs related to the process will be stored in the logs folder.

To test the pipeline, add a new .csv file to the data folder (you can create a new csv or subset the existing data stored in the data folder). If the data passes all the validation checks then the data is stored to a PostgreSQL database. The details about the validation checks and processing operations are documented under the docs folder. You can also modify the data_validation_check.py, data_preprocessing.py, and data_analysis.py files to customize the data processing steps according to your requirements.


## Future Work

Some potential improvements and future work for this project include:
1. Implementing data normalization and optimizing storage for large datasets.
2. Using distributed processing frameworks like Apache Kafka, Apache Spark, or cloud services such as AWS Lambda or Google Cloud Pub/Sub to make the pipeline scalable.
3. Adding error handling and recovery mechanisms to log failures and retry failed operations.

## License

This project is licensed under the terms of the MIT License, which allows for free use, modification, and distribution of the code, with the exception that the original authors are credited.

Dataset License
The dataset used in this project is from Kaggle and may have its own licensing terms. Please review the terms and conditions on the dataset page for more information on usage rights and restrictions.
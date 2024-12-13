# Project Documentation

## Table of Contents
- Project Overview
- Architecture and Design
- Tech Stack
- Possible solutions for scaling the Pipeline for Production
- Future Improvements

## Project Overview
This project implements an end-to-end data processing pipeline for weather data collected by multiple beach stations. The pipeline performs a series of operations including data validation, transformation, analysis, and stores both the processed data and aggregated data (data after performing analysis) in a PostgreSQL database. The goal is to automate the entire process of reading sensor data, analyzing it, and storing the insights in a structured database while logging all the actions and errors.

## Architecture and Design

The pipeline is designed to handle sensor data collected from beach weather stations and process it through the following steps:

![Architecture Design](/assests/datapipelinearchi.png)

1. File Monitoring: The pipeline constantly monitors a data folder for incoming .csv files. Multiple csv files can be added to the data folder, and the pipeline processes each file sequentially.
2. Data Validation: Each file undergoes validation checks to ensure data quality and integrity     
    a. First it check for the missing values in the 'Station Name', 'Measurement Timestamp' and all the sensor values.
    b. In my use case, I have focused on the "Air Temperature" sensor value. So, I have predefined the range of values. If the value is not in the range then it is considered as an invalid value.
    c. I have also checked the data type of the sensor values. If the data type is not float or integer then it is considered as an invalid value.
    d. I have also validated the timestamp format. If the timestamp format is not in the correct format then it is considered as an invalid value.
    c. If any of the above checks fail, then the failed records are moved quarantined with the reason of failure.
3. Data Transformation: The data is cleaned and transformed into a more usable format (e.g., splitting timestamps, removing duplicates).
4. Data Analysis: Statistical analysis is performed on the processed data, providing insights for each sensor such as mean, standard deviation, minimum value and maximum value. This information is aggregated by station and then stored into a dataframe.
Data Storage: The processed data and the aggregated data are stored in a PostgreSQL database for future analysis. The database connection details are stored in db_connection.py. The processed data is stored in 'processed_data' table while the aggregated data 'aggregated_data' table. I have also created indexes on the 'Station Name' and 'Measurement Timestamp' columns for faster query performance. 
Logging: All steps in the pipeline are logged for tracking and error management. The logs are stored in the logs directory with the timestamp of the log creation.

### Design Flow
Data Monitoring: The main.py file monitors the data directory for .csv files using Python’s built-in file handling.
Validation and Transformation: Once a new file is detected, the data undergoes a series of checks in data_validation_check.py and transformations in data_preprocessing.py. If any errors are encountered, they are logged, and the affected records are quarantined.
Data Aggregation and Analysis: In data_analysis.py, the data is grouped by station and key metrics (e.g., min, max, mean, std) are calculated.
Data Insertion: The processed data and aggregated data are inserted into the PostgreSQL database using db_connection.py.
Logging: The logger, configured in logger.py, captures all information and errors throughout the pipeline and stores them in the logs directory.

## Tech Stack

Python >= 3.6
Pandas and NumPy
PostgreSQL
Watchdog
Logging

## Possible solutions for scaling the Pipeline

To scale this pipeline such that it handles larger dataset in real-time (or near real-time), we need to incorporate some external tools/services to improve reliability, performance, and fault tolerance.

1. Data Ingestion Strategies:
We can use Apache Kafka to handle large volumes of incoming data streams. Kafka can manage data ingestion at scale.

2. Data Processing Frameworks:
We have to use distributed data processing in order to handle large dataset. We can employ Apache Spark Framework, Spark can be integrated to handle distributed processing of data. Spark can process data across multiple nodes in parallel, significantly improving performance.


3. Data Storage Setup:
We can store raw data in data lakes like AWS S3 or Google Cloud Storage, which can store large volumes of data cost-effectively and perform analytics on it later. We can also use cloud-based data warehouses like Amazon Redshift or Google BigQuery for storing and querying large datasets.

4. Scheduling the data workflow/processing:
We can use Apache Airflow to schedule and orchestrate the data processing workflow. Airflow can manage dependencies between tasks, and provide monitoring and alerting capabilities.

5. Resiliency and Fault Tolerance:
Retry Mechanisms: Implement retry logic using built-in retry policies in cloud services. This ensures that if a failure occurs, the pipeline will attempt to reprocess the failed steps.

6. Monitoring and Alerting: Use monitoring tools such as Grafana to track the health and performance of the pipeline. Set up alerts for critical failures or performance degradation.

7. CI/CD Pipelines:
We can implement continuous integration/continuous deployment (CI/CD) pipelines using tools like GitHub Actions, or Jenkins. This will allow us to automatically test, build, and deploy the application to production when code changes are made.

## Future Improvements

Data Normalization: Implement additional data normalization steps to ensure that the data being processed is consistent and aligned with business needs.
Distributed Processing: Integrate distributed processing frameworks like Apache Spark for faster processing of large datasets.
Data Validation Enhancements: Extend the data validation checks to include more comprehensive rules and real-time anomaly detection.
Monitoring and Alerting: Implement monitoring tools for the production pipeline to track performance and health.
Automated Testing: Add unit tests and integration tests to ensure the pipeline works as expected before deployment.

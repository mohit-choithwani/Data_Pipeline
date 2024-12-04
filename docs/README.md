# Project Documentation

## Table of Contents
- Project Overview
- Architecture and Design
- Tech Stack
- Possible solutions for scaling the Pipeline for Production
- Future Improvements

## Project Overview
This project implements an end-to-end data processing pipeline for weather data collected by multiple beach stations. The pipeline performs a series of operations including data validation, transformation, analysis, and stores both the processed data and aggregated data (data after performing analysis) in a PostgreSQL database. The goal is to automate the process of reading sensor data, analyzing it, and storing the insights in a structured database while logging all the actions and errors.

## Architecture and Design

The pipeline is designed to handle sensor data collected from beach weather stations and process it through the following steps:

![Architecture Design](/assests/datapipelinearchi.png)

File Monitoring: The pipeline constantly monitors a data folder for incoming .csv files.
Data Validation: Each file undergoes validation checks to ensure data quality and integrity (e.g., missing values, outliers, correct data types).
Data Transformation: The data is cleaned and transformed into a more usable format (e.g., splitting timestamps, removing duplicates).
Data Analysis: Statistical analysis is performed on the data, providing insights into the weather conditions.
Data Storage: The processed data is stored in a PostgreSQL database for future retrieval and analysis.
Logging: All steps in the pipeline are logged for tracking and error management.

### Design Flow
Data Monitoring: The main.py file monitors the data directory for .csv files using Python’s built-in file handling.
Validation and Transformation: Once a new file is detected, the data undergoes a series of checks in data_validation_check.py and transformations in data_preprocessing.py. If any errors are encountered, they are logged, and the affected records are quarantined.
Data Aggregation and Analysis: In data_analysis.py, the data is grouped by station and key metrics (e.g., min, max, mean, std) are calculated.
Data Insertion: The final transformed data is inserted into a PostgreSQL database using SQLAlchemy.
Logging: The logger, configured in logger.py, captures all information and errors throughout the pipeline and stores them in the logs directory.

## Tech Stack

Python >= 3.6
Pandas and NumPy
PostgreSQL
Watchdog
Logging

## Possible solutions for scaling the Pipeline for Production

To scale this pipeline for production, several considerations need to be addressed to improve reliability, performance, and fault tolerance.

1. Distributed Processing:
Apache Kafka: To handle large volumes of incoming data streams, you can integrate Kafka. Kafka can manage data ingestion at scale, decoupling the data producers (e.g., weather stations) from the consumers (e.g., data processing services).
Apache Spark: For large datasets, Spark can be integrated to handle distributed processing of data. Spark can process data across multiple machines in parallel, significantly improving performance.
AWS Lambda / Google Cloud Functions: These serverless computing platforms can handle the processing without needing to manage servers. They can be triggered by events, such as a new file being added to the data folder (stored on S3 or Cloud Storage).
2. Resiliency and Fault Tolerance:
Retry Mechanisms: Implement retry logic using libraries like tenacity or built-in retry policies in cloud services (e.g., AWS Lambda retries). This ensures that if a failure occurs, the pipeline will attempt to reprocess the failed steps.
Error Handling: Enhance logging and error handling to ensure any failures (e.g., file corruption, DB unavailability) are logged, and the system can recover gracefully without crashing the pipeline.
3. Efficient Data Storage:
Partitioning: In PostgreSQL, use table partitioning strategies to store data in separate partitions based on time, station, or other relevant factors. This will improve query performance and data management.
Compression: Use compression techniques (e.g., Parquet or ORC) for storing data in cloud storage or on disk, which will reduce the storage cost and improve I/O operations.
4. Scalable File Handling:
Use cloud storage services (e.g., AWS S3, Google Cloud Storage) to store incoming files, which automatically scale as more data is uploaded. Combine this with cloud functions or containers to process data as it's uploaded to the storage service.
5. CI/CD Pipelines:
CI/CD: Implement continuous integration/continuous deployment (CI/CD) pipelines using tools like GitHub Actions, CircleCI, or Jenkins. This will allow you to automatically test, build, and deploy the application to production when code changes are made.

## Future Improvements

Data Normalization: Implement additional data normalization steps to ensure that the data being processed is consistent and aligned with business needs.
Distributed Processing: Integrate distributed processing frameworks like Apache Spark for faster analysis of large datasets.
Data Validation Enhancements: Extend the data validation checks to include more comprehensive rules and real-time anomaly detection.
Monitoring and Alerting: Implement monitoring tools (e.g., Prometheus, Grafana) for the production pipeline to track performance and health.
Automated Testing: Add unit tests and integration tests to ensure the pipeline works as expected before deployment.


# RapidAPI - NASDAQ Data Pipeline 

# AWS Data Engineering Project - Data Pipeline
https://nasdaq.nenene.info/

## Introduction

This project's goal is to build an end-to-end data pipeline that integrates with the RapidAPI, extracts data from a NASDAQ stock, processes it, and loads it into AWS Simple Storage (S3) and AWS Athena. The pipeline is intended to be completely automated using the AWS Platform.

Data extraction, transformation, data movement, and loading into S3 for storage and Athena for analysis are all part of the project. Each component is critical in ensuring that the data is processed efficiently, organised, and easily accessible for analysis.

### Authors

- [@nenene](https://github.com/nenene1812)

## Acknowledgements

- [@Darshil Parmar](https://github.com/darshilparmar)

## Architecture

![Project Architecture](https://github.com/nenene1812/DE-stock-aws/blob/main/Data_engineering_architecture_nasdaq.png?raw=true)

## Project description
- Step 1: Using Jupyter Notebook, test data and read json: RapidAPI -> Format dataframe -> planning structure. 
- Step 2: Create a trigger and run a cronjob every day to receive messages. 
- Step 3: Design a Lambda function to receive the trigger and extract the data. 
- Step 4: Save data to S3 storage as raw data for later transformation. 
- Step 5: Add a trigger to receive event and store the object in S3. 
- Step 6: Write a Lambda function that receives object triggers and converts data to structure format (CSV).
- Step 7: Save the data as transformation data to S3. 
- Step 8: Build a crawler to collect data for anthena. 
- Step 9: Define the schema with AWS Glue. 
- Step 10: Analysing data with Athena. 
- Step 11: Use streamlit to visualise data.

## Tech Stack

**Programming Language**
- Python 

**AWS cloud** 
- AWS Simple Storage (S3)
- AWS Lambda
- AWS Cloudwatch Events
- AWS Crawler
- AWS Glue
- AWS Athena

**Data Visualization** 
- Streamlit

## Pipeline

1. Data Extraction
- AWS Lambda function triggered daily by CloudWatch
- Fetches latest data from RapidAPI (NASDAQ)
- Uploads raw data upon successful extraction
2. Data Transformation
- Triggered automatically after data extraction
- Transforms newly uploaded raw data
- Moves transformed data to respective folders
- Removes raw data to keep it clean
3. Data Loading
- Loads transformed data into AWS Athena
- Provides centralized database for analytics
- Enables generating insights and reports


# Conclusion

This end-to-end data pipeline project provided great experience in designing and implementing an automated ETL process using AWS cloud services. 

Building the extraction, transformation, and loading modules required learning new skills like AWS Lambda, CloudWatch, S3, Glue, Athena, and Python. The project demonstrated how these services can be integrated to ingest, process, store, and analyze data efficiently at scale.

Automating the pipeline with triggers and schedulers was an important learning. This ensures latest data is processed daily without any manual intervention.

Transforming the raw JSON data into an optimized CSV structure required problem-solving skills to parse and clean the data into the target schema.

Loading the processed data into S3 and Athena enabled running SQL queries on terabytes of data and generating insights rapidly. These are powerful big data capabilities unlocked by the pipeline.

Overall, this project provided hands-on experience in architecting, developing, and deploying a robust data pipeline on AWS. The skills learned will be invaluable for tackling more complex data engineering challenges in the future.

# Customer 360  Big Data Pipeline Airflow

## Overview

Customer 360 Airflow is a data processing pipeline that aims to provide a comprehensive view of customer information by integrating order data from an S3 bucket with customer data stored in a MySQL database. The pipeline utilizes Apache Airflow to orchestrate the workflow, processing the data on an edge node, creating a Hive table, and storing the final result in HBase.

## Objective

The primary objective of this pipeline is to ensure that the order file is made available in the S3 bucket every day at a specific time. The customer-related information is stored in a MySQL database. The workflow involves bringing the order file to the edge node, processing it, creating a Hive table, and performing a join operation with the customer table from MySQL. The final result is then dumped into HBase for further analysis and reporting.

## Prerequisites
- Apache Airflow
- Apache Hive
- Apache HBase
- MySQL
- Amazon S3
  
## Workflow

1. **Data Ingestion:**
   - The order file is expected to be available in the specified S3 bucket every day.
   - Make sure to setup the bucket policy for making it publicly accessible. You can use the policy generator.
     ![s3](https://github.com/aryanR27/Customer-360-BigData-pipeline/assets/60980375/901d3032-8448-4e25-9ab8-c5378b640957)


2. **Data Processing:**
   - The data is fetched from the S3 bucket to the edge node for processing.

3. **Hive Table Creation:**
   - A Hive table is created over the processed data to facilitate structured querying.

4. **MySQL Data Retrieval:**
   - Customer information is fetched from the MySQL database.

5. **Data Join:**
   - A join operation is performed between the order data (Hive table) and customer data (MySQL) to create a unified dataset.

6. **HBase Storage:**
   - The result of the join operation is dumped into HBase for efficient storage and retrieval.

## Dag View
![airflow](https://github.com/aryanR27/Customer-360-BigData-pipeline/assets/60980375/4cdb1b7e-7504-45a5-ae71-f55c54331856)

## Configuration

Ensure that the necessary configurations are set in the Airflow DAG file:

- S3 Bucket details for order file ingestion.
- MySQL database connection details.
- Hive and HBase configurations.
- Place the jar file on the hdfs.

## Usage

- Create dags folder inside airflow
- Place the python file inside dags folder.
- airflow webserver -p 8080 for starting the airflow web server.
- Open another terminal and start the airflow scheduler — airflow scheduler
- Access the Airflow UI at http://localhost:8080.
- Navigate to the "DAGs" section and find the "customer_360_dag" DAG.
- Trigger the DAG manually.

## Review
I personally use the bash operator for connecting to the machine but it can be done in more concise way via SSH Operator with connection setup in airflow. But my VM having some required libraries missing for SSH Operator and having dependencies on other.

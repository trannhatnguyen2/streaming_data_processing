# Data Streaming with Debezium, Kafka, Spark Streaming, Delta Lake, and MinIO

## Overview

- Produce persistent data to **PostgreSQL**.
- Identify and track changes to data in a database using **Debezium** Connector.
- Read the streaming data from **Kafka topic** using **PySpark (Spark Streaming)**.
- Convert to **Delta Lake** format and write the streaming data to **MinIO (AWS Object Storage)**.
<!-- - Write the streaming data to **Elasticsearch**, and visualize it using **Kibana**. -->
- Use **Trino** to query data and display in **DBeaver**.

## System Architecture

<p align="center">
<img src="./img/system_architecture.png" width=80% height=80%>

<p align="center">
    System Architecture
</p>

## Prerequisites

Before running this script, ensure you have the following installed:

- Python 3.10
- Docker
- Debezium (Debezium UI)
- PostgreSQL
- Confluent Containers (Zookeeper, Kafka, Schema Registry, Connect, Control Center)
- Apache Spark
- Delta Lake
- MinIO, Trino, DBeaver

## Getting Started

1. **Clone the repository**:

   ```bash
   git clone https://github.com/trannhatnguyen2/streaming_data_processing
   ```

2. **Start our data streaming infrastructure**

   ```bash
   docker compose -f docker-compose.yaml -f storage-docker-compose.yaml up -d
   ```

   This command will download the necessary Docker images, create containers, and start the services in detached mode.

3. **Setup environment**

Activate your conda environment and install required packages:

    ```bash
    conda create -n streaming python==3.10
    y
    conda activate streaming
    pip install -r requirements.txt
    ```

4. **Access the Services**

   - Postgres is accessible on the default port `5432`.
   - Debezium UI is accessible at `http://localhost:8080`.
   - Kafka Control Center is accessible at `http://localhost:9021`.
   - MinIO is accessible at ``http://localhost:9001`.

## How-to Guide

1. **Create Connector Postgres to Debezium**

   Firstly, modifying your config in config/postgresql-cdc.example.json

   ```bash
   bash streaming/run.sh register_connector configs/postgresql-cdc.json
   ```

2. **Create an empty table in PostgreSQL and insert new record to the table**

   ```bash
   python streaming/utils/create_table.py
   python streaming/utils/insert_table.py
   ```

   **Note:** Print records as json format in terminal

   ```bash
   python streaming/json_consume_message.py
   ```

3. **Produce data to Kafka topic**

   ```bash
   cd data_ingestion/kafka_producer
   python produce_json.py
   ```

4. **Store data to MinIO**

Read data in `Kafka Topic`then push them to `MinIO` with `delta lake` format

    ```bash
    python spark_streaming/delta_spark_to_minio.py
    ```

## Read data streaming in MinIO

### Create data schema

After putting your files to ` MinIO``, please execute  `trino` container by the following command:

```shell
docker exec -ti datalake-trino bash
```

When you are already inside the `trino` container, typing `trino` to in an interactive mode

After that, run the following command to register a new schema for our data:

```sql
CREATE SCHEMA IF NOT EXISTS datalake.device
WITH (location = 's3://datalake/');

CREATE TABLE IF NOT EXISTS datalake.device.device (
event_timestamp TIMESTAMP(3) WITH TIME ZONE,
created VARCHAR,
device_id TINYINT,
feature_0 DOUBLE,
feature_1 DOUBLE,
feature_3 DOUBLE,
feature_4 DOUBLE,
feature_5 DOUBLE,
feature_6 DOUBLE,
feature_8 DOUBLE,
if_movement VARCHAR
) WITH (
location = 's3://datalake/device'
);
```

## Query with DBeaver

1. Install `DBeaver` as in the following [guide](https://dbeaver.io/download/)
2. Connect to our database (type `trino`) using the following information (empty `password`):
   ![DBeaver Trino](./imgs/trino.png)

---

<p>&copy; 2024 NhatNguyen</p>

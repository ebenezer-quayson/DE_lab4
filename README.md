# Week 4 Labs: Big Data Analysis and Streaming Pipelines

## Overview
This lab consists of two exercises on big data technologies and streaming pipelines using **Apache Spark**, **BigQuery**, and **Apache Kafka**. Participants will process data, query insights, and build a streaming data pipeline.

---

## Lab Exercise 1: Wikipedia Pageviews Analysis

### Objective
By the end of this lab, participants will:
- Use Spark DataFrames and SQL to process Wikipedia pageviews data.
- Write data to BigQuery and query insights.
- Visualize results with Pandas.

---

### Dataset
The **Wikipedia Pageviews Dataset** contains:
- **Date and Time:** When the interaction occurred.
- **Language:** Language version of the Wikipedia page.
- **Title:** Page title.
- **View Counts:** Number of views.

---

### Prerequisites
1. **GCP Setup:**
   - Create a Dataproc cluster with Jupyter & Component Gateway.
2. **Apache Spark:** Set up Spark and Jupyter on Dataproc.

---

### Tasks
Follow the [**Lab Instructions**](#) for the following:

1. **Load Data into Spark**:
   - Read the BigQuery table into a Spark DataFrame.

2. **Filter Data**:
   - Focus on English Wikipedia versions (`en` and `en.m`) with more than 100 views.

3. **Analyze Data**:
   - Group by `title` and order by `views` to identify top pages.

4. **Write to BigQuery**:
   - Save the result to a BigQuery table.

5. **Advanced Query**:
   - Retrieve top 10 pages where the title contains **"United"**.

6. **Spark SQL**:
   - Repeat the steps above using Spark SQL.

7. **Visualization**:
   - Use Pandas to plot **total views** by `datehour`.

---

### Expected Output
- **Top Pages by Views:** Most-viewed pages.
- **Query Result:** Top 10 pages with "United" in the title.
- **Visualization:** Total views across `datehour`.

---

## Lab Exercise 2: Building a Streaming Data Pipeline with Kafka

### Objective
By the end of this lab, participants will:
- Set up a Kafka cluster.
- Create a streaming data pipeline with Kafka Streams.
- Inspect output data via console consumers.

---

### Overview
In this exercise, participants will build an end-to-end Kafka streaming pipeline:
1. **Kafka Cluster Setup:** Start Kafka on a single machine using GCP.
2. **Stream Processing:** Write data to Kafka topics and process it.
3. **Inspect Output:** View processed data with console consumer.

---

### Prerequisites
- Active **Cloud Skills Boost** account.

---

### Tasks
Follow the [**Lab Instructions**](#) for these steps:

1. **Set Up Kafka Cluster**: Start and configure Kafka.
2. **Prepare Topics and Data**: Define topics and load input data.
3. **Process Data**: Use Kafka Streams to process the input data.
4. **Inspect Output**: View results with console consumer.
5. **Tear Down**: Stop the Kafka cluster.

---

### Expected Output
- Functional streaming pipeline with processed data.

---

## Setup Notes
### Environment Preparation
- **Exercise 1:** Ensure the Dataproc cluster has Spark and Jupyter.
- **Exercise 2:** Use a single GCP Compute Engine instance for Kafka.

---

## Submission
- **Due Date:** Monday, 25th November 2024.
- Submit code, queries, visualizations, and Kafka logs.

---

## Helpful Links
- [Dataproc with Jupyter Setup](https://cloud.google.com/dataproc/docs/concepts/jupyter)
- [BigQuery Spark Connector](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/streams)

---

### Acknowledgments
This lab offers practical exposure to big data and streaming technologies, preparing students for real-world data engineering tasks.

End-to-End Data Pipeline: Apache NiFi → AWS S3 → Snowflake (SCD Type 2)

This project demonstrates a complete real-time data pipeline that ingests, processes, and loads data using Apache NiFi, AWS S3, and Snowflake. The solution includes auto-ingestion using Snowpipe, transformation using SQL, and scheduled updates via Snowflake Tasks implementing Slowly Changing Dimension Type 2 (SCD2) logic.

Project Overview:

Flow Diagram:
Apache NiFi ➝ AWS S3 ➝ Snowflake Stage ➝ Snowpipe ➝ Variant Table ➝ SCD2 Merge ➝ Final Dimension Table

Key Features:

Real-time JSON file ingestion using Apache NiFi
Storage and integration with AWS S3 Bucket
Auto-ingestion via Snowpipe using external stage and file format
Use of Variant data type in staging table
Scheduled Snowflake Task for merging data into final user_dim table
Implementation of SCD Type 2 pattern using `MERGE` statements

Tools & Technologies
| Component    	| Technology        |
|---------------|-------------------|
| Orchestration	| Apache NiFi       |
| Cloud Storage	| AWS S3            |
| Data Warehouse| Snowflake         |
| Language      | SQL               |
| Integration   | Snowflake External Stage & Snowpipe |
| Automation    | Snowflake Task    |

Folder Structure:

├── SnowflakeCode.sql # All Snowflake objects (staging, pipe, tasks, merge)
├── DataStoringInS3Bucket.PNG # Screenshot: AWS S3 bucket with incoming files
├── NIFI_Flow.PNG # Screenshot: NiFi flow to S3
├── Snowflake_StageTable.PNG # Screenshot: Staging table data in Snowflake
├── SnowflakeTargetTable.PNG # Screenshot: Final dimension table (user_dim)
├── README.md # Project documentation (this file)


Project Workflow

1. Apache NiFi:
Generates and sends JSON files to S3 bucket
Uses processors like `GenerateFlowFile`, `UpdateAttribute`, and `PutS3Object`

2. AWS S3:
Acts as the landing zone for incoming JSON files
Bucket: `nifi-scd2-project`

3. Snowflake Configuration:
Storage Integration created with proper IAM Role
External Stage defined to connect Snowflake with S3
File Format set to JSON
Snowpipe created with AUTO_INGEST = TRUE
Staging Table (user_stage_json) using `VARIANT` data type
Final Dimension Table (user_dim) stores structured data

4. Auto Merge Logic:
MERGE statement handles inserts and updates
Snowflake Task runs every minute to apply merge logic continuously

5. Sample SQL Highlights
CREATE OR REPLACE PIPE user_pipe
AUTO_INGEST = TRUE
AS
COPY INTO user_stage_json
FROM @user_stage
FILE_FORMAT = json_format;

MERGE INTO user_dim AS target
USING (
  SELECT raw:id::NUMBER AS id, raw:name::STRING AS name, raw:email::STRING AS email
  FROM user_stage_json
) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET name = source.name, email = source.email
WHEN NOT MATCHED THEN
  INSERT (id, name, email)
  VALUES (source.id, source.name, source.email);

Learnings:
Connecting NiFi to AWS S3 for streaming data
Creating auto-ingest pipelines using Snowpipe
Writing SCD2-style merge logic in SQL
Automating data processing using Snowflake Tasks
Designing robust pipelines for real-time ingestion

Connect & Repository:
If you found this useful, feel free to connect on LinkedIn or fork the repository and build upon it.
GitHub Repository: [View Project on GitHub](https://github.com/siriGH/Nifi-AWS-S3-scd2-Snowflake-pipeline.git)
LinkedIn: [Sirisha Karusala](https://www.linkedin.com/in/sirishakarusala)






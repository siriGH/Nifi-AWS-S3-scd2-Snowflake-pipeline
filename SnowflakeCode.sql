//Create Schema

CREATE WAREHOUSE scd2_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

//Create table with variant type

CREATE OR REPLACE TABLE user_stage_json (
  raw VARIANT
);

select * from user_stage_json;

//Create file Format

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON';

//Create storage intigration

CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::548775194136:role/User_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nifi-scd2-project/');

  DESC INTEGRATION s3_int;

 //Create external stage
 
 CREATE OR REPLACE STAGE user_stage
  URL = 's3://nifi-scd2-project/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = json_format;
 

//Create snowpipe

CREATE OR REPLACE PIPE user_pipe
AUTO_INGEST = TRUE
AS
COPY INTO user_stage_json
FROM @user_stage
FILE_FORMAT = json_format;


DESC pipe user_pipe;

//Check manually

SELECT
  raw:id::NUMBER AS id,
  raw:name::STRING AS name,
  raw:email::STRING AS email
FROM user_stage_json;


//Target table

CREATE OR REPLACE TABLE user_dim (
  id NUMBER,
  name STRING,
  email STRING
);

//Load data in target table manualy

MERGE INTO user_dim AS target
USING (
  SELECT
    raw:id::NUMBER AS id,
    raw:name::STRING AS name,
    raw:email::STRING AS email
  FROM user_stage_json
) AS source
ON target.id = source.id

WHEN MATCHED THEN
  UPDATE SET
    name = source.name,
    email = source.email

WHEN NOT MATCHED THEN
  INSERT (id, name, email)
  VALUES (source.id, source.name, source.email);


  //Create task to make dataload automatically
 
 CREATE OR REPLACE TASK auto_merge_user_dim
WAREHOUSE = scd2_wh  -- replace with your warehouse name
SCHEDULE = '1 MINUTE'  -- adjust frequency as needed
AS
MERGE INTO user_dim AS target
USING (
  SELECT
    raw:id::NUMBER AS id,
    raw:name::STRING AS name,
    raw:email::STRING AS email
  FROM user_stage_json
) AS source
ON target.id = source.id

WHEN MATCHED THEN
  UPDATE SET
    name = source.name,
    email = source.email

WHEN NOT MATCHED THEN
  INSERT (id, name, email)
  VALUES (source.id, source.name, source.email);

ALTER TASK auto_merge_user_dim RESUME;

SHOW TASKS LIKE 'auto_merge_user_dim';

//Verify data
  
Select * from user_dim;





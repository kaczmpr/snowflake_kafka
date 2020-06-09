USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET TIMEZONE='Europe/Warsaw';
SELECT CURRENT_TIMESTAMP;

USE ROLE SYSADMIN;
CREATE OR REPLACE DATABASE LOMBARDIA;

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOMBARDIA;
CREATE OR REPLACE SCHEMA STAGE COMMENT = 'Staging area';

USE ROLE ACCOUNTADMIN;
USE DATABASE LOMBARDIA;
ALTER SCHEMA IF EXISTS STAGE ENABLE MANAGED ACCESS;

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOMBARDIA;
CREATE OR REPLACE SCHEMA PROD COMMENT = 'Production area';

USE ROLE ACCOUNTADMIN;
USE DATABASE LOMBARDIA;
ALTER SCHEMA IF EXISTS PROD ENABLE MANAGED ACCESS;

USE ROLE SYSADMIN;
USE DATABASE LOMBARDIA;
GRANT USAGE ON DATABASE LOMBARDIA TO ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA STAGE TO ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA PROD TO ROLE SECURITYADMIN;

USE ROLE SECURITYADMIN;
USE DATABASE LOMBARDIA;
CREATE OR REPLACE ROLE KAFKA_CONNECTOR
COMMENT = 'Role for connecting with Kafka';
GRANT ROLE KAFKA_CONNECTOR TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE LOMBARDIA TO ROLE KAFKA_CONNECTOR;
GRANT USAGE ON SCHEMA STAGE TO ROLE KAFKA_CONNECTOR;
GRANT USAGE ON SCHEMA PROD TO ROLE KAFKA_CONNECTOR;

GRANT CREATE FUNCTION --Enables creating a new UDF in a schema.
	, CREATE MATERIALIZED VIEW
	, CREATE PROCEDURE
	, CREATE SEQUENCE --Enables creating a new sequence in a schema, including cloning a sequence.
	, CREATE TABLE --Enables creating a new table in a schema, including cloning a table.
	, CREATE VIEW --Enables creating a new view in a schema.
	, USAGE --Enables using a schema, including executing SHOW SCHEMAS commands to list the schema details in a database.

	, CREATE EXTERNAL TABLE --Enables creating a new external table in a schema.
	, CREATE STAGE --Enables creating a new stage in a schema, including cloning a stage.
	, CREATE FILE FORMAT --Enables creating a new file format in a schema, including cloning a file format.
	, CREATE FUNCTION --Enables creating a new UDF in a schema.
	, CREATE PIPE --Enables creating a new pipe in a schema.
	, CREATE STREAM --Enables creating a new stream in a schema, including cloning a stream.
	, CREATE TASK --Enables creating a new task in a schema, including cloning a task.
ON SCHEMA LOMBARDIA.STAGE TO ROLE KAFKA_CONNECTOR;

GRANT CREATE FUNCTION --Enables creating a new UDF in a schema.
	, CREATE MATERIALIZED VIEW
	, CREATE PROCEDURE
	, CREATE SEQUENCE --Enables creating a new sequence in a schema, including cloning a sequence.
	, CREATE TABLE --Enables creating a new table in a schema, including cloning a table.
	, CREATE VIEW --Enables creating a new view in a schema.
	, USAGE --Enables using a schema, including executing SHOW SCHEMAS commands to list the schema details in a database.
	, CREATE EXTERNAL TABLE --Enables creating a new external table in a schema.
	, CREATE STAGE --Enables creating a new stage in a schema, including cloning a stage.
	, CREATE FILE FORMAT --Enables creating a new file format in a schema, including cloning a file format.
	, CREATE FUNCTION --Enables creating a new UDF in a schema.
	, CREATE PIPE --Enables creating a new pipe in a schema.
	, CREATE STREAM --Enables creating a new stream in a schema, including cloning a stream.
	, CREATE TASK --Enables creating a new task in a schema, including cloning a task.
ON SCHEMA LOMBARDIA.PROD TO ROLE KAFKA_CONNECTOR;


USE ROLE SECURITYADMIN;
CREATE OR REPLACE USER KAFKA_CON
PASSWORD = 'KAFKA_CON'
MUST_CHANGE_PASSWORD = FALSE
DEFAULT_ROLE = KAFKA_CONNECTOR;

GRANT ROLE KAFKA_CONNECTOR TO USER KAFKA_CON;

ALTER USER KAFKA_CON SET RSA_PUBLIC_KEY='--insert-here-public-key--'
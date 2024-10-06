-----------------------------------------------------------------------------------------------------------
    -- "End-to-End College Database Solution: Data Integration, Pipeline Automation, and Alerts in Snowflake"
-----------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
                            -- [1] Database and Schema Creation:
------------------------------------------------------------------------------------------

-- Create Database
CREATE DATABASE IF NOT EXISTS college;

-- Use the Database
USE DATABASE college;

-- Create All 6 Schemas
CREATE SCHEMA IF NOT EXISTS Faculty
CREATE SCHEMA IF NOT EXISTS Student;
CREATE SCHEMA IF NOT EXISTS Library;
CREATE SCHEMA IF NOT EXISTS Canteen;
CREATE SCHEMA IF NOT EXISTS Hostel;
CREATE SCHEMA IF NOT EXISTS Events;

---------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
                            -- [2] Tables Creation in Each Schema:
------------------------------------------------------------------------------------------

-- [2.1] Faculty Table Creation

CREATE OR REPLACE TABLE COLLEGE.FACULTY.FACULTY_DETAILS (
    Faculty_ID INT AUTOINCREMENT PRIMARY KEY,
    Name STRING,
    Department STRING,
    Email STRING,
    Phone_Number STRING,
    Hire_Date DATE
);

-- [2.2] Student Table Creation

CREATE OR REPLACE TABLE COLLEGE.STUDENT.STUDENT_DETAILS (
    Student_ID INT AUTOINCREMENT PRIMARY KEY,
    Name STRING,
    Age INT,
    Major STRING,
    Enrollment_Year INT,
    Email STRING
);

-- [2.3] Library Table Creation

CREATE OR REPLACE TABLE COLLEGE.LIBRARY.BOOKS (
    Book_ID INT AUTOINCREMENT PRIMARY KEY,
    Title STRING,
    Author STRING,
    ISBN STRING,
    Published_Year INT,
    Available_Copies INT
);

-- [2.4] Canteen Menu Table Creation

CREATE OR REPLACE TABLE COLLEGE.CANTEEN.MENU (
    Item_ID INT AUTOINCREMENT PRIMARY KEY,
    Item_Name STRING,
    Price FLOAT,
    Category STRING
);

-- [2.5] Hostel Details Table Creation

CREATE OR REPLACE TABLE COLLEGE.HOSTEL.HOSTEL_DETAILS (
    Hostel_ID INT AUTOINCREMENT PRIMARY KEY,
    Name STRING,
    Capacity INT,
    Warden_Name STRING,
    Contact STRING
);

-- [2.6] Events Table Creation

CREATE OR REPLACE TABLE COLLEGE.EVENTS.EVENT_DETAILS (
    Event_ID INT AUTOINCREMENT PRIMARY KEY,
    Event_Name STRING,
    Date DATE,
    Location STRING,
    Organizer STRING
);

---------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
                            -- [3] Sample CSV Data Generation:
------------------------------------------------------------------------------------------

/*
For each table, you need at least few records in a CSV file to load data into the tables via S3. Here's a general structure for the CSV files:

faculty_details.csv
student_details.csv
books.csv
menu.csv
hostel_details.csv
event_details.csv

Please create these CSV files with the required sample data. If you need help with specific data, I can assist with a sample data script attached in this repo.
*/

---------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
                            -- [4] Create a Common Stage for AWS S3:
------------------------------------------------------------------------------------------

/* Important Queries
select GET_DDL('FILE_FORMAT','MY_CSV_FORMAT');
*/

/*
AWS Process for S3 Configuration:
1. login to account
2. create a S3 Bucket
3. Create a Access Key if not Created
4. load the file into s3
*/

-- Create a Stage for S3 Data Loading
-- NOTE : Please Change AWS_KEY_ID and AWS_SECRET_KEY

CREATE STAGE COLLEGE.PUBLIC.COLLEGE_CSV 
    URL = 's3://college-data-loading/' 
    CREDENTIALS = ( AWS_KEY_ID = '********' AWS_SECRET_KEY = '*****' ) 
    DIRECTORY = ( ENABLE = true AUTO_REFRESH = true ) 
    COMMENT = 'All Latest Data File are Comming Daily Basis';

---------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
                            -- [5] Create a Common FILE_FORMAT for Loading data:
------------------------------------------------------------------------------------------

-- Create a file format for CSV with options to skip the header

CREATE OR REPLACE FILE FORMAT COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE;

---------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
                            -- [6] Create a Procedure for Data Loading:
------------------------------------------------------------------------------------------

-- [6.1] Data Loading for USP_FACULTY_DETAILS

CREATE OR REPLACE PROCEDURE COLLEGE.FACULTY.USP_FACULTY_DETAILS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    TRUNCATE TABLE COLLEGE.FACULTY.FACULTY_DETAILS;
    
    INSERT INTO COLLEGE.FACULTY.FACULTY_DETAILS (Faculty_ID, Name, Department, Email, Phone_Number, Hire_Date)
    SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => ''COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT'', PATTERN => ''.*faculty.*[.]csv'') t;

    RETURN ''Data loaded into FACULTY_DETAILS'';
END;
';

-- CALL PROCEDURE
CALL COLLEGE.FACULTY.USP_FACULTY_DETAILS();

-- CROSS CHECK
SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => 'COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT', PATTERN => '.*faculty.*[.]csv') t;

    
-- [6.2] Data Loading for USP_STUDENT_DETAILS

CREATE OR REPLACE PROCEDURE COLLEGE.STUDENT.USP_STUDENT_DETAILS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    TRUNCATE TABLE COLLEGE.STUDENT.STUDENT_DETAILS;
    
    INSERT INTO COLLEGE.STUDENT.STUDENT_DETAILS (Student_ID, Name, Age, Major, Enrollment_Year, Email)
    SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => ''COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT'', PATTERN => ''.*student.*[.]csv'') t;

    RETURN ''Data loaded into STUDENT_DETAILS'';
END;
';

-- CALL PROCEDURE
CALL COLLEGE.STUDENT.USP_STUDENT_DETAILS();

-- CROSS CHECK
SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => 'COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT', PATTERN => '.*student.*[.]csv') t;


-- [6.3] Data Loading for USP_BOOKS

CREATE OR REPLACE PROCEDURE COLLEGE.LIBRARY.USP_BOOKS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    TRUNCATE TABLE COLLEGE.LIBRARY.BOOKS;
    
    INSERT INTO COLLEGE.LIBRARY.BOOKS (Book_ID, Title, Author, ISBN, Published_Year, Available_Copies)
    SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => ''COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT'', PATTERN => ''.*books.*[.]csv'') t;

    RETURN ''Data loaded into BOOKS'';
END;
';

-- CALL PROCEDURE
CALL COLLEGE.LIBRARY.USP_BOOKS();

-- CROSS CHECK
SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => 'COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT', PATTERN => '.*books.*[.]csv') t;


-- [6.4] Data Loading for USP_MENU

CREATE OR REPLACE PROCEDURE COLLEGE.CANTEEN.USP_MENU()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    TRUNCATE TABLE COLLEGE.CANTEEN.MENU;
    
    INSERT INTO COLLEGE.CANTEEN.MENU (Item_ID, Item_Name, Price, Category)
    SELECT t.$1, t.$2, t.$3, t.$4
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => ''COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT'', PATTERN => ''.*menu.*[.]csv'') t;

    RETURN ''Data loaded into MENU'';
END;
';

-- CALL PROCEDURE
CALL COLLEGE.CANTEEN.USP_MENU();

-- CROSS CHECK
SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => 'COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT', PATTERN => '.*menu.*[.]csv') t;


-- [6.5] Data Loading for USP_HOSTEL_DETAILS

CREATE OR REPLACE PROCEDURE COLLEGE.HOSTEL.USP_HOSTEL_DETAILS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    TRUNCATE TABLE COLLEGE.HOSTEL.HOSTEL_DETAILS;
    
    INSERT INTO COLLEGE.HOSTEL.HOSTEL_DETAILS (Hostel_ID, Name, Capacity, Warden_Name, Contact)
    SELECT t.$1, t.$2, t.$3, t.$4, t.$5
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => ''COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT'', PATTERN => ''.*hostel.*[.]csv'') t;

    RETURN ''Data loaded into HOSTEL_DETAILS'';
END;
';

-- CALL PROCEDURE
CALL COLLEGE.HOSTEL.USP_HOSTEL_DETAILS();

-- CROSS CHECK
SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => 'COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT', PATTERN => '.*hostel.*[.]csv') t;


-- [6.6] Data Loading for USP_EVENT_DETAILS

CREATE OR REPLACE PROCEDURE COLLEGE.EVENTS.USP_EVENT_DETAILS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    TRUNCATE TABLE COLLEGE.EVENTS.EVENT_DETAILS;
    
    INSERT INTO COLLEGE.EVENTS.EVENT_DETAILS (Event_ID, Event_Name, Date, Location, Organizer)
    SELECT t.$1, t.$2, t.$3, t.$4, t.$5
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => ''COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT'', PATTERN => ''.*event.*[.]csv'') t;

    RETURN ''Data loaded into EVENT_DETAILS'';
END;
';

-- CALL PROCEDURE
CALL COLLEGE.EVENTS.USP_EVENT_DETAILS();

-- CROSS CHECK
SELECT t.$1, t.$2, t.$3, t.$4, t.$5
    FROM @COLLEGE.PUBLIC.COLLEGE_CSV
    (FILE_FORMAT => 'COLLEGE.PUBLIC.COLLEGE_CSV_FORMAT', PATTERN => '.*event.*[.]csv') t;


------------------------------------------------------------------------------------------
                -- [7] Create a Task to call all 6 Procedure for Data Loading:
------------------------------------------------------------------------------------------

-- [7.1] CREATE TASK FOR FACULTY_DETAILS PROCEDURE
CREATE OR REPLACE TASK COLLEGE.FACULTY.TASK_FACULTY_DETAILS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS
    -- Call the USP_FACULTY_DETAILS
    CALL COLLEGE.FACULTY.USP_FACULTY_DETAILS();

    
-- [7.2] CREATE TASK FOR STUDENT_DETAILS PROCEDURE
CREATE OR REPLACE TASK COLLEGE.STUDENT.TASK_STUDENT_DETAILS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS
    -- Call the USP_STUDENT_DETAILS
    CALL COLLEGE.STUDENT.USP_STUDENT_DETAILS();

    
-- [7.3] CREATE TASK FOR BOOKS PROCEDURE
CREATE OR REPLACE TASK COLLEGE.LIBRARY.TASK_BOOKS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS
    -- Call the USP_BOOKS
    CALL COLLEGE.LIBRARY.USP_BOOKS();


-- [7.4] CREATE TASK FOR MENU PROCEDURE
CREATE OR REPLACE TASK COLLEGE.CANTEEN.TASK_MENU
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS
    -- Call the USP_MENU
    CALL COLLEGE.CANTEEN.USP_MENU();


-- [7.5] CREATE TASK FOR HOSTEL_DETAILS PROCEDURE
CREATE OR REPLACE TASK COLLEGE.HOSTEL.TASK_HOSTEL_DETAILS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS
    -- Call the USP_HOSTEL_DETAILS
    CALL COLLEGE.HOSTEL.USP_HOSTEL_DETAILS();


-- [7.6] CREATE TASK FOR EVENT_DETAILS PROCEDURE
CREATE OR REPLACE TASK COLLEGE.EVENTS.TASK_EVENT_DETAILS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS
    -- Call the USP_EVENT_DETAILS
    CALL COLLEGE.EVENTS.USP_EVENT_DETAILS();


-- CREATE A MAIN TASK FOR EXECUTE ALL THE TASKS    
CREATE OR REPLACE TASK COLLEGE.PUBLIC.TASK_COLLEGE_DATA_LOAD
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 38 16 * * * UTC'
AS
  BEGIN
    
    EXECUTE TASK COLLEGE.FACULTY.TASK_FACULTY_DETAILS;
    EXECUTE TASK COLLEGE.STUDENT.TASK_STUDENT_DETAILS;
    EXECUTE TASK COLLEGE.LIBRARY.TASK_BOOKS;
    EXECUTE TASK COLLEGE.CANTEEN.TASK_MENU;
    EXECUTE TASK COLLEGE.HOSTEL.TASK_HOSTEL_DETAILS;
    EXECUTE TASK COLLEGE.EVENTS.TASK_EVENT_DETAILS;

END;  

-- CHECK DATA IS PROPER LOAD OR NOT

ALTER TASK COLLEGE.PUBLIC.TASK_COLLEGE_DATA_LOAD SUSPEND;
ALTER TASK COLLEGE.PUBLIC.TASK_COLLEGE_DATA_LOAD RESUME;

-- TRUNCATE TABLE COLLEGE.FACULTY.FACULTY_DETAILS;

EXECUTE TASK COLLEGE.PUBLIC.TASK_COLLEGE_DATA_LOAD;


------------------------------------------------------------------------------------------
                -- [8] Create a Table to Track Task Log status
------------------------------------------------------------------------------------------

-- Create a Table to Track Task Status with Additional Columns

CREATE OR REPLACE TABLE COLLEGE.PUBLIC.TASK_STATUS (
    TASK_NAME STRING,
    SCHEMA_NAME STRING,
    DATABASE_NAME STRING,
    SCHEDULED_TIME TIMESTAMP_LTZ,
    COMPLETED_TIME TIMESTAMP_LTZ,
    STATUS STRING,
    STATE STRING,
    QUERY_START_TIME TIMESTAMP_LTZ,
    ERROR_CODE STRING,
    ERROR_MESSAGE STRING,
    ATTEMPT_NUMBER NUMBER,
    QUERY_TEXT STRING,
    TIMESTAMP TIMESTAMP_LTZ
);


------------------------------------------------------------------------------------------
                -- [8] Create a Stored Procedure for Status Logging data loading
------------------------------------------------------------------------------------------ 

-- Create or Replace Procedure to Update Task Status for Current Day
CREATE OR REPLACE PROCEDURE COLLEGE.PUBLIC.UPDATE_TASK_STATUS()
RETURNS STRING
LANGUAGE SQL
AS
'
BEGIN
    -- Clear the previous days data
    TRUNCATE TABLE COLLEGE.PUBLIC.TASK_STATUS;

    -- Insert the latest task statuses for the current day
    INSERT INTO COLLEGE.PUBLIC.TASK_STATUS (
        TASK_NAME, 
        SCHEMA_NAME, 
        DATABASE_NAME, 
        SCHEDULED_TIME, 
        COMPLETED_TIME, 
        STATUS, 
        STATE, 
        QUERY_START_TIME, 
        ERROR_CODE, 
        ERROR_MESSAGE, 
        ATTEMPT_NUMBER, 
        QUERY_TEXT, 
        TIMESTAMP
    )
    SELECT
        NAME,  -- Task name
        SCHEMA_NAME,  -- Schema name where task is defined
        DATABASE_NAME,  -- Database name where task is defined
        SCHEDULED_TIME,  -- Scheduled time of task execution
        COMPLETED_TIME,  -- Time when task execution was completed
        CASE
            WHEN STATE = ''SUCCEEDED'' THEN ''SUCCESS''
            WHEN STATE = ''FAILED'' THEN ''FAILED''
            ELSE ''UNKNOWN''
        END AS STATUS,  -- Simplified status derived from state
        STATE,  -- Actual state of the task
        QUERY_START_TIME,  -- Query start time
        ERROR_CODE,  -- Error code if task failed
        ERROR_MESSAGE,  -- Error message if task failed
        ATTEMPT_NUMBER,  -- Attempt number of the task
        QUERY_TEXT,  -- Query text executed by the task
        CURRENT_TIMESTAMP() AS TIMESTAMP  -- Timestamp of record entry
    FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
    WHERE QUERY_START_TIME >= CAST(CURRENT_DATE() AS TIMESTAMP) 
      AND QUERY_START_TIME < CAST(CURRENT_DATE() + 1 AS TIMESTAMP)
      AND DATABASE_NAME != ''SNOWFLAKE''
      AND NAME != ''TASK_COLLEGE_DATA_LOAD'';

    RETURN ''Task status updated for the current day'';
END;
';

-- CALL THE PROCEDURE UPDATE_TASK_STATUS FOR UPDATE TASK LOG DATA

CALL COLLEGE.PUBLIC.UPDATE_TASK_STATUS();

-- CREATE A STATUS TASK FOR EXECUTE UPDATE_TASK_STATUS PROCEDURE FOR LOG   
CREATE OR REPLACE TASK COLLEGE.PUBLIC.TASK_UPDATE_TASK_STATUS
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 50 16 * * * UTC'
AS   
    CALL COLLEGE.PUBLIC.UPDATE_TASK_STATUS(); 
    

-- CHECK DATA IS PROPER LOAD OR NOT

ALTER TASK COLLEGE.PUBLIC.TASK_UPDATE_TASK_STATUS SUSPEND;
ALTER TASK COLLEGE.PUBLIC.TASK_UPDATE_TASK_STATUS RESUME;

-- TRUNCATE TABLE COLLEGE.PUBLIC.TASK_STATUS;

SELECT * FROM COLLEGE.PUBLIC.TASK_STATUS;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY;

-------------------------------------------------------------------------------- extra

-- SETUP CRONTAB ACCORDING TO IST 

select CURRENT_TIMESTAMP();

select LOCALTIMESTAMP();

select SYSTIMESTAMP();

update CURRENT_TIMESTAMP()='2024-09-10 20:32:56.533 -0700';

ALTER USER Anu SET TIMEZONE = 'Asia/Kolkata';

--------------------------------------------------------------------------------
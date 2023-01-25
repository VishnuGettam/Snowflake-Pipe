--database dev 
create or replace database DEV;

--Schema Source
create or replace schema source;


--storage integration object 
use role accountadmin;

create or replace storage integration snowflake_aws_integration
type = 'external_stage'
storage_provider = 's3'
enabled = true
storage_allowed_locations = ('s3://snowflake-dev-s3/Load/CSV/')
storage_aws_role_arn = 'arn:aws:iam::819162972734:role/Snowflake-Aws-Integration-Role'; 

desc integration snowflake_aws_integration;




--file format object for CSV 
use role sysadmin;
create or replace file format ff_csv
type='csv'
field_delimiter = ','
record_delimiter = '\n'
skip_header = 1
comment = 'file format for CSV objects';

use role accountadmin;

--create external stage for CSV 
create or replace stage aws_s3_csv
url = 's3://snowflake-dev-s3/Load/CSV/'
storage_integration = snowflake_aws_integration
file_format = (format_name = 'ff_csv');


--check the list of files 
list @aws_s3_csv;

--access the data and check for columns 
select 
metadata$filename,
metadata$file_row_number :: int ,
emp.$1 ,emp.$2,emp.$3,emp.$4,emp.$5,emp.$6 :: date
from @aws_s3_csv 
(file_format => 'ff_csv') emp;


--create source table 
create or replace transient table tblemployeesource
(
    Filename varchar(100),
    RowId int,
    FirstName varchar(100),
    LastName varchar(100),
    Email varchar(100),
    Address varchar(200),
    Location varchar(100),
    Doj date
)

--create snowpipe to load the data into tblemployeesource
create or replace pipe snowflake_aws_csv_pipe
auto_ingest = true
as
copy into tblemployeesource
from 
(
select 
metadata$filename,
metadata$file_row_number :: int ,
emp.$1 ,emp.$2,emp.$3,emp.$4,emp.$5,emp.$6 :: date
from @aws_s3_csv 
(file_format => 'ff_csv') emp
) 
file_format = (format_name = 'ff_csv')
on_error = continue;

--add the SQS ARN in the bucket to set up the notification channel
desc pipe snowflake_aws_csv_pipe;

--check the status of pipe 
select parse_json(system$pipe_status('snowflake_aws_csv_pipe'));
-- "2023-01-25T15:02:28.51

--to load the historical files into queue 
alter pipe snowflake_aws_csv_pipe refresh;

--validate the pipe load history 
--use validate_pipe_load function
--show any errors in loading data .
select * from table(validate_pipe_load(PIPE_NAME => 'snowflake_aws_csv_pipe' , START_TIME => dateadd(hour,-1,current_timestamp()) ));

--check the data 
select * from tblemployeesource;


--validate the pipe load history 
--use copy_history table function 
select * from table(information_schema.copy_history(table_name=>'tblemployeesource', START_TIME => dateadd(hour,-1,current_timestamp()) ));
























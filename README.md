# Snowflake-Pipe

1.Create a database (DEV) with schema (SOURCE)

2.Created required AWS S3 bucket (s3://snowflake-dev-s3/Load/CSV/) . 

3.Create a AWS Role with AWS account and mention trust entity relationship .

4.Add Snowflake integration object with the ARN for th aws role .

5.Create file format (CSV) and stage object with the allowed URL's mentioned.

6.Create pipe object and update the SNS ARN in the bucket event notification.

7.Check the status of pipe and load the file in S3 bucket.

8.Validate the data loading process and errors can be checked either using "validate_pipe_load"/"copy_history" table functions.

9.Alternatively error integration to message channel (AWS) which is a Notification Integration in Snowflake to SNS topic can be added for error validations. 

  a.create a policy with allow publish to a SNS topic .
  
  b.create a SNS topic and update the policy with ARN of the SNS topic .
  
  c.create AWS role with AWS account and add dummy value under external identity and assing the policy to the role .
  
  d.create notification integration object in snowflake and update SF SF_AWS_IAM_USER_ARN/SF_AWS_EXTERNAL_ID values into AWS role.
  
  e.add the integration object into the pipe and add Email subscription under SNS. 

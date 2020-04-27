# SnowflakeNiFiProcessors
This project contains 4 NiFi processors for ingesting data into Snowflake


# New Features (v1.0.0)
Initial Release



## PutSnowflake

---
## Purpose

The PutSnowflake processor moves a file from a local directory to the Snowflake Internal staging area provided.  
This is implemented using a JDBC connection to snowflake where it formulates and sends a Snowflake PUT command.  
Options for compression and concurrency are provided as parameters in the Processor configuration.


## ExecuteSnowPipe

---
## Purpose

The ExecuteSnowPipe processor requests Snowflake to execute the SnowPipe provided against the provided filename from Snowflakes internal staging area.
No status on the success or failure of the SnowPipe operation is returned to the client.  A value of Success merely means the REST call was successfully received.
To obtain the status of the SnowPipe operation the QuerySnowPipeHistory or AwaitSnowPipeResults processors must be used.
This is implemented using a REST API connection to Snowflake with an unencrypted asymmetrical key for authentication.


## QuerySnowPipeHistory

---
## Purpose

The QuerySnowPipeHistory returns a JSON list of all SnowPipe results for a given SnowPipe name over the time window provided.
This processor accepts a Trailing Minutes configuration to provide how long ago to start the report.

The QuerySnowPipeHistory processor may not receive an input relationship.  It will run on a schedule and output a flow file only if results were returned for the given SnowPipe name and time range.

This is implemented using a REST API connection to Snowflake with an unencrypted asymmetrical key for authentication.


## AwaitSnowPipeResults

---
## Purpose

The AwaitSnowPipeResults accepts flow files as an input.  On each execution it will pull every file in the input queue to obtain the SnowPipe list and time range to query.
It will then query the appropriate information from Snowflake.  Finally it will compare the FlowFiles in the queue with the results from Snowflake to properly route each flow file.
Flow files that have not received a result yet will be routed to the WAITING relationship.  Flow files that remain in the WAITING relationship longer than the configured minutes will be routed to the UNMATCHED relationship.

This is implemented using a REST API connection to Snowflake with an unencrypted asymmetrical key for authentication.

# Recommended NiFi Flow

This section briefly describes how these NiFi processors can be used together to complete an ingestion pipeline into Snowflake.

The typical snowflake ingestion happens in two phases
- Uploading the File
- Parsing and Load the File


## Uploading the File
Files may be uploaded to your cloud storage location directly using PutAzureBlobStorage, PutS3Object, etc.  You may also choose to load files directly into Snowflake's Internal staging area.  This lets Snowflake manage the storage location for you.  Storage rates do apply to internal staging.

The PutSnowflake processor is used to upload to Snowflake's internal staging area only.  In order to use this processor you must first create a stage area in snowflake.
https://docs.snowflake.com/en/sql-reference/sql/create-stage.html

These files may come in many formats: CSV, JSON, Avro, etc.  You will be able to define how these files are parsed in the definition of the SnowPipe later.

The PutSnowflake NiFi processor uses the JDBC command 'PUT' to move the file from your local directory to Snowflake's internal staging.  This can fail if the file name you provided was not found or if the Internal Staging area was not defined in Snowflake.

If your flow file is routed to the success relationship, then the file was successfully transmitted to the internal staging area you identified in the processor configuration.

# Parse and Load the File
We will use a Snowflake object called SnowPipe to perform the parsing of the file and loading the data into a target table. 
https://docs.snowflake.com/en/sql-reference/sql/create-pipe.html

The SnowPipe is made up  a Copy Into Statement and a File Format object
### Copy Into
https://docs.snowflake.com/en/user-guide/data-load-local-file-system-copy.html
### File Format
https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html

It may take some time to get the syntax of your Copy Into Statement to work properly.  I recommend performing this work in the Snowflake Web UI for rapid prototyping.  Once you have your Copy Into statement and File Format object functioning as expected, you can use this to create a SnowPipe object.

### Execute Snow Pipe
The SnowPipe essentially applies a name to your Copy Into statement that we can call from NiFi.
This is the SnowPipe name used in the ExecuteSnowPipe processor.  
You will also configure the ExecuteSnowPipe Processor with the location of your uploaded file.  This can be from Snowflake's internal staging or your Cloud Service Provider's Storage location.

If your relationship is routed to the failure relationship, it may mean the uploaded file was not found (or not accessible) or the SnowPipe name was not recognized.  
If your relationship is routed to success, it only means that both the file and the SnowPipe were recognized.  It does not indicate your file was successfully parsed with the data loaded into the target table.

In order to understand the success or failure of your SnowPipe call, you will need to make use of the AwaitSnowPipeResults NiFi Processor. 

### Await SnowPipe Results
Route the 'success' relationship from the ExecuteSnowPipe processor to the AwaitSnowPipeResults processor.  Next Route the 'Waiting' relationship of the AwaitSnowPipeResults processor back to itself.

When the processor runs, it will look at all flow files waiting to be processed by this processor.  From that list the processor will determine which SnowPipe to query for results.  It will also discover the minimal window of time to request (preventing unnecessary load on your Snowflake warehouse).  Once it has this information, it will submit a 'insertReport' API call to Snowflake, which returns a report for all requested SnowPipes for the requested time span.

The processor then will compare the list returned from Snowflake with the list of FlowFiles waiting to be matched.  Any matches will be routed on the 'Success' or 'Failure' relationships according to the SnowPipe results.  The remaining flow files that were not matched in the report from Snowflake will be routed to the 'Waiting' relationship for consideration next time.

## Final Processing
Once you have a successful or failure response from your SnowPipe you should probably log it for correction or auditing as required.

# Disclaimer
All code is provided as-is, with no guarantee or obligations, implied or otherwise.

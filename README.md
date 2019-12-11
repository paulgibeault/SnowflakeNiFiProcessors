# SnowflakeNiFiProcessors
This project contains 3 NiFi processors for ingesting data into Snowflake


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
This is implemented using a REST API connection to Snowflake with asymetrical key authentication.


## QuerySnowPipeHistory

---
## Purpose

The QuerySnowPipeHistory returns a JSON list of all SnowPipe results for a given SnowPipe name over the time window provided.
This processor accepts a Trailing Minutes configuration to provide how long ago to start the report.

The QuerySnowPipeHistory processor may not receive an input relationship.  It will run on a schedule and output a flow file only if results were returned for the given SnowPipe name and time range.

This is implemented using a REST API connection to Snowflake with asymetrical key authentication.


## AwaitSnowPipeResults

---
## Purpose

The AwaitSnowPipeResults accepts flow files as an input.  On each execution it will pull every file in the input queue to obtain the SnowPipe list and time range to query.
It will then query the appropriate information from Snowflake.  Finally it will compare the FlowFiles in the queue with the resutls from Snowflake to properly route each flow file.
Flow files that have not received a result yet will be routed to the WAITING relationsip.  Flow files that remain in the WAITING relationship longer than the configured minutes will be routed to the UNMATCHED relationship.

# Disclaimer
All code is provided as-is, with no garuntee or obligations, implied or otherwise.

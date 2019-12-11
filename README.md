# SnowflakeNiFiProcessors

This project contains 3 NiFi processors for ingesting data into Snowflake

#PutSnowflake
  Uses JDBC PUT command to move a file from a local mount to Snowflake's internal staging
  
#ExecuteSnowPipe
  Sends a REST API command to Snowflake invoking a named Snowpipe on a staged file
  
#AwaitSnowPipeResults
  Sends a REST API to obtain recent Snowpipe results, routes waiting flowfiles accordingly

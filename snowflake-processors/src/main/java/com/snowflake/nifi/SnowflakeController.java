package com.snowflake.nifi;

import org.apache.nifi.controller.ControllerService;
import net.snowflake.ingest.SimpleIngestManager;

// Public interface for Snowflake Controller services
// This should be broken into it's own NAR with the
// SimpleIngestManager class, and dependent classes
// wrapped in interface classes to mask snowflake dependency
public interface SnowflakeController extends ControllerService {
    SimpleIngestManager getIngestManager(String inPipe);
}

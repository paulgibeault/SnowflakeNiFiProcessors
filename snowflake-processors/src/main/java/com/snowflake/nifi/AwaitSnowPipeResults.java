package com.snowflake.nifi;

import java.time.Instant;
import java.util.*;

import net.snowflake.ingest.connection.IngestStatus;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.joda.time.DateTime;

import com.google.gson.Gson;

import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse.FileEntry;



@TriggerWhenEmpty
@Tags({"range", "history", "pipe", "cloud", "database", "stage", "s3", "snowflake"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Queries Snowflake for SnowPipe results, compares them with queued Flow Files and routes them appropriately.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "10 sec")
@SeeAlso({ExecuteSnowPipe.class, QuerySnowPipeHistory.class, PutSnowflake.class})
@ReadsAttributes({@ReadsAttribute(attribute="AwaitSnowPipeResults.queuedTimestamp", description="The epoch timestamp when AwaitSnowPipe first received this flow file.")})
@WritesAttributes({
        @WritesAttribute(attribute="AwaitSnowPipeResults.queuedTimestamp", description="The error description resulting in a flow file routed to error or partially_loaded relationships."),
        @WritesAttribute(attribute="AwaitSnowPipeResults.error", description="The error description resulting in a flow file routed to error or partially_loaded relationships."),
        @WritesAttribute(attribute="AwaitSnowPipeResults.errorCount", description="The error description resulting in a flow file routed to error or partially_loaded relationships."),
})
public class AwaitSnowPipeResults extends AbstractProcessor {

    // Private class to hold information from FlowFile queue
    // and SnowPipe History results
    private class SnowPipeResult {
		// Member variables
		Long flowFileAgeMinutes;
		String snowPipeName;
		String fileName;
		FileEntry fileResult;

		//Constructor
		public SnowPipeResult(String inPipeName, String inFileName, Long age) {
			this.snowPipeName = inPipeName;
			this.fileName = inFileName;
			this.flowFileAgeMinutes = age;
		}
    }

    // Private const strings
    private static final String queuedTimestampAttribute = "AwaitSnowPipeResults.queuedTimestamp";
    private static final String errorAttribute = "AwaitSnowPipeResults.error";
    private static final String errorCountAttribute = "AwaitSnowPipeResults.errorCount";

	//properties
	public static final PropertyDescriptor SNOWFLAKE_SERVICE = new PropertyDescriptor
			.Builder().name("Snowflake Connection Service")
			.description("Provides connection to Snowflake REST API")
			.required(true)
			.identifiesControllerService(SnowflakeController.class)
			.build();

	public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor
			.Builder().name("FILE_NAME")
			.displayName("Staged File Name")
			.description("The name of the file in Cloud Stage that was processed by Snow Pipe")
			.required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor PIPE = new PropertyDescriptor
			.Builder().name("Pipe")
			.displayName("Snowflake pipe name")
			.description("Fully qualified Snowflake pipe name")
			.required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor MAX_FLOW_FILE_AGE = new PropertyDescriptor
			.Builder().name("MAX_FLOW_FILE_AGE")
			.displayName("Max File Age")
			.description("The maximum number of minutes the flow file may remain in queue awaiting results.  Flow files that exceed this limit are routed to the unmatched relationship.")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	//relationships
    public static final Relationship REL_LOADED = new Relationship.Builder()
            .name("Loaded")
            .description("SnowPipe successfully processed the specified file.")
            .build();

    public static final Relationship REL_PARTIALLY_LOADED = new Relationship.Builder()
            .name("Partially Loaded")
            .description("SnowPipe processed the specified file, but encountered some errors.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("Load Failure")
			.description("SnowPipe encountered an error when processing this flow file and was unable to continue")
			.build();

	public static final Relationship REL_UNMATCHED = new Relationship.Builder()
			.name("Unmatched")
			.description("Flow files that failed to receive results in the allowed time frame.")
			.build();

	public static final Relationship REL_WAITING = new Relationship.Builder()
			.name("Waiting")
			.description("Flow files that are still awaiting results.")
			.build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;



	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(SNOWFLAKE_SERVICE);
		descriptors.add(PIPE);
		descriptors.add(FILE_NAME);
		descriptors.add(MAX_FLOW_FILE_AGE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_LOADED);
        relationships.add(REL_PARTIALLY_LOADED);
		relationships.add(REL_FAILURE);
		relationships.add(REL_UNMATCHED);
		relationships.add(REL_WAITING);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

		final ComponentLog logger = getLogger();
		final SnowflakeController snowflakeController = context.getProperty(SNOWFLAKE_SERVICE).asControllerService(SnowflakeController.class);
		final int maxAgeMinutes = context.getProperty(MAX_FLOW_FILE_AGE).asInteger();

        // Storage for State of FlowFile queue and SnowPipe results
        Map<String, Map<String, SnowPipeResult>> snowPipeResultMap = new HashMap<>();

		// Calculate current Epoch seconds
		Instant instant = Instant.now();
		Long currentEpochMilli = instant.toEpochMilli();

		// Grab the entire list of queued flow files
        final int flowFileCount = session.getQueueSize().getObjectCount();
		List<FlowFile> flowFileList = session.get(flowFileCount);
		if ( flowFileList.size() < 1 ) {
			return; // Abort if no flow files returned
		}

		// The first iteration of the FlowFile List is to:
        // 1. Calculate the time window to query SnowPipe History results
        // 2. Collate the list of unique SnowPipe names to query for History
        // 3. PrePopulate the SnowPipe Result Map with SnowPipe and Filenames we care about
		Integer oldestFlowFileMinutes = 1;  // Minimum of 1 minute
		Set<String> snowPipeSet = new HashSet<>();
		for (FlowFile flowfile : flowFileList) {

		    // Get the SnowPipe Name and FileName
            String snowPipeName = context.getProperty(PIPE).evaluateAttributeExpressions(flowfile).getValue();
            String currentFileName = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowfile).getValue();

            // Get queued start time to measure how long this flow file sits in the Waiting queue
            Long flowFileAgeMinutes = new Long(0);
            String flowFileQueuedTimestampStr = flowfile.getAttribute(queuedTimestampAttribute);
            if (null != flowFileQueuedTimestampStr && !flowFileQueuedTimestampStr.isEmpty()) {
                Long flowFileQueuedTimestamp = new Long(flowFileQueuedTimestampStr);
                flowFileAgeMinutes = (currentEpochMilli - flowFileQueuedTimestamp) / 60000;
            }

            if (null != snowPipeName && null != currentFileName) {

				// Find the oldest flow file, used in Query to SnowPipe History
				Long flowFileQueuedMinutes = (currentEpochMilli - flowfile.getLineageStartDate()) / 60000;  // 60K milliseconds in a minute
				if (flowFileQueuedMinutes > oldestFlowFileMinutes) {
					oldestFlowFileMinutes = Math.toIntExact(flowFileQueuedMinutes);
				}

				// Make sure this SnowPipe is listed in the distinct list of SnowPipe names
				snowPipeSet.add(snowPipeName);

				// Create the Snow Pipe Hash Map it doesn't already exist
				if (!snowPipeResultMap.containsKey(snowPipeName)) {
					snowPipeResultMap.put(snowPipeName, new HashMap<>());
				}

				// Create the File Name Hash Map it doesn't already exist
				if (!snowPipeResultMap.get(snowPipeName).containsKey(currentFileName)) {
					// PrePopulate Snow Pipe Result Map with flow file information
					snowPipeResultMap.get(snowPipeName).put(currentFileName,
							                                new SnowPipeResult(snowPipeName, currentFileName, flowFileAgeMinutes));
				}
			}
		}

        // Now we query SnowPipe history for each SnowPipe found in the FlowFile queue
        // Using the time frame calculated from the same queue
        // If we receive a result from Get History Range that is defined in our Snow Pipe Result Map
        // Then populate the corresponding entry in the Snow Pipe Result Map with the File Result object provided.
		try {
			// Calculate time window
			DateTime now = new DateTime();
			DateTime then = now.minusMinutes(Math.abs(oldestFlowFileMinutes));

			// For each distinct snow pipe, query Snowflake for results on that SnowPipe
			for (String snowPipe : snowPipeSet ) {
				SimpleIngestManager mgr = snowflakeController.getIngestManager(snowPipe);

				HistoryRangeResponse resp = mgr.getHistoryRange(null, then.toString(), now.toString());

                for (FileEntry entry : resp.files) {
                    if (snowPipeResultMap.get(snowPipe).containsKey(entry.getPath())) {
                        snowPipeResultMap.get(snowPipe).get(entry.getPath()).fileResult = entry;
                    }
                }
            }
        }catch(Exception e) {
            logger.error("Failure invoking historyRange: {}",
                    new Object[]{e});
        }

        // Iterate the FlowFile list once more to
        // 1. Populate flow file contents with results, if provide
        // 2. Route relationships that have results to the appropriate relationship: LOADED, FAILED, PARTIALLY_LOADED
        // 3. FlowFiles that have not received results and have exceeded MaxAge are routed to: UNMATCHED
        for (FlowFile flowfile : flowFileList) {
            // Get the SnowPipe Name and FileName
            String snowPipeName = context.getProperty(PIPE).evaluateAttributeExpressions(flowfile).getValue();
            String currentFileName = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowfile).getValue();

            // Set queued start timestamp for this flow file, if not already set
            String flowFileQueuedTimestampStr = flowfile.getAttribute(queuedTimestampAttribute);
            if (null == flowFileQueuedTimestampStr  || flowFileQueuedTimestampStr.isEmpty()) {
                flowfile = session.putAttribute(flowfile, queuedTimestampAttribute,  currentEpochMilli.toString());
            }

            // Fail the flow file if SnowPipe Name is not found
            if (null == snowPipeName) {
				flowfile = session.putAttribute(flowfile, errorAttribute, "Flow file does not contained configured attribute: " + PIPE);
				session.transfer(flowfile, REL_FAILURE);
			}
            // Fail the flow file if File Name is not found
			else if (null == currentFileName) {
				flowfile = session.putAttribute(flowfile, errorAttribute, "Flow file does not contained configured attribute: " + FILE_NAME);
				session.transfer(flowfile, REL_FAILURE);
			}
            // Otherwise, route the flow file to the appropriate relationship
            else if (snowPipeResultMap.containsKey(snowPipeName)) {
                if (snowPipeResultMap.get(snowPipeName).containsKey(currentFileName)) {

                    SnowPipeResult currentResult = snowPipeResultMap.get(snowPipeName).get(currentFileName);

                    // We have results, populate the flow file with a JSON representation of the object returned
                    if (null != currentResult.fileResult)
                    {
                        // Write Contents of SnowPipe Results to corresponding flow file
                        flowfile = session.write(flowfile, out -> {
							Gson gson = new Gson();
							out.write(gson.toJson(currentResult.fileResult).getBytes());
                        });

                        // Route flow file to appropriate relationship
                        if (currentResult.fileResult.getStatus().equals(IngestStatus.LOADED)) {
                            session.transfer(flowfile, REL_LOADED);
                        }
                        else if (currentResult.fileResult.getStatus().equals(IngestStatus.PARTIALLY_LOADED)) {
                            flowfile = session.putAttribute(flowfile, errorAttribute, currentResult.fileResult.getFirstError());
                            flowfile = session.putAttribute(flowfile, errorCountAttribute, currentResult.fileResult.getErrorsSeen().toString());
                            session.transfer(flowfile, REL_PARTIALLY_LOADED);
                        }
                        else if (currentResult.fileResult.getStatus().equals(IngestStatus.LOAD_FAILED)) {
                            flowfile = session.putAttribute(flowfile, errorAttribute, currentResult.fileResult.getFirstError());
                            flowfile = session.putAttribute(flowfile, errorCountAttribute, currentResult.fileResult.getErrorsSeen().toString());
                            session.transfer(flowfile, REL_FAILURE);
                        }
                        else {
                            session.transfer(flowfile,REL_WAITING);
                        }
                    }
                    else
                    {
                        if (maxAgeMinutes < currentResult.flowFileAgeMinutes) {
                            // This flow file has waited too long for results.
                            // Transfer the flow file to the UNMATCHED relationship
							flowfile = session.removeAttribute(flowfile, queuedTimestampAttribute); // Remove waiting attribute so it may wait again some other day
							session.transfer(flowfile,REL_UNMATCHED);
                        }
                        else {
                            session.transfer(flowfile,REL_WAITING);
                        }
                    }
                }
            }
            else {
                session.transfer(flowfile,REL_WAITING);
            }
        }
	}

}

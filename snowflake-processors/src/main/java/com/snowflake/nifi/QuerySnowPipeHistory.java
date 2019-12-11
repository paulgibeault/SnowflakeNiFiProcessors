package com.snowflake.nifi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
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



@TriggerWhenEmpty
@Tags({"range", "history", "pipe", "cloud", "database", "stage", "s3", "snowflake"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "5 min")
@CapabilityDescription("Used to invoke Snowflake SnowPipe call to query loaded files for a given pipe in a time range")
@SeeAlso({ExecuteSnowPipe.class, AwaitSnowPipeResults.class, PutSnowflake.class})
public class QuerySnowPipeHistory extends AbstractProcessor {

	//properties
	public static final PropertyDescriptor SNOWFLAKE_SERVICE = new PropertyDescriptor
			.Builder().name("Snowflake Connection Service")
			.description("Provides connection to Snowflake REST API")
			.required(true)
			.identifiesControllerService(SnowflakeController.class)
			.build();

	public static final PropertyDescriptor PIPE = new PropertyDescriptor
			.Builder().name("Pipe")
			.displayName("Snowflake pipe name")
			.description("Fully qualified Snowflake pipe name")
			.required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor TRAIL = new PropertyDescriptor
			.Builder().name("Trailing Minutes")
			.displayName("Trailing Minutes")
			.description("Number of trailing minutes from the current time to define the time range")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	//relationships
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("Successfully accepted SnowPipe ingest call")
			.build();

	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("Failed SnowPipe ingest calls")
			.build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;



	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(SNOWFLAKE_SERVICE);
		descriptors.add(PIPE);
		descriptors.add(TRAIL);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
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
		final String pipe = context.getProperty(PIPE).getValue();
		final int trailMin = context.getProperty(TRAIL).asInteger();


		try {
			SimpleIngestManager mgr = snowflakeController.getIngestManager(pipe);
			DateTime now = new DateTime();
			DateTime then = now.minusMinutes(Math.abs(trailMin));
			HistoryRangeResponse resp = mgr.getHistoryRange(null, then.toString(), now.toString());
			
			if (resp.files.size()>0) {
			
				FlowFile targetFlowFile = session.create();
				targetFlowFile = session.write(targetFlowFile, out -> {
                    Gson gson = new Gson();
                    out.write(gson.toJson(resp.files).getBytes());
                });

				session.transfer(targetFlowFile, REL_SUCCESS);
			}


		}catch(Exception e) {
			logger.error("Failure invoking historyRange: {}",
                    new Object[]{e});


		}
		

	}

}

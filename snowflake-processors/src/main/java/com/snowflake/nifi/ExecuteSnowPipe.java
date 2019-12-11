/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.snowflake.nifi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.BackOffException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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

import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;

@Tags({"execute", "ingest", "pipe", "cloud", "database", "stage", "s3", "snowflake"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Used to invoke Snowflake SnowPipe call to asynchronously process staged file")
@SeeAlso({AwaitSnowPipeResults.class, QuerySnowPipeHistory.class, PutSnowflake.class})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
					@WritesAttribute(attribute="ExecuteSnowPipe.reponse_code", description="HTTP status code returned from SnowPipe invocation"),
					@WritesAttribute(attribute="ExecuteSnowPipe.error_message", description="HTTP status code returned from SnowPipe invocation")
				  })
public class ExecuteSnowPipe extends AbstractProcessor {

	// Private const strings
	private static final String responseCodePattern = ".*HTTP Status: (\\d+)";
	private static final String resposneCodeAttribute = "ExecuteSnowPipe.reponse_code";
	private static final String errorMessageAttribute = "ExecuteSnowPipe.error_message";

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

	public static final PropertyDescriptor FILE = new PropertyDescriptor
			.Builder().name("File")
			.displayName("File Name")
			.description("Name of file to be processed by the given Pipe")
			.required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.defaultValue("${filename}")
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

	public static final Relationship REL_RETRY = new Relationship.Builder()
			.name("retry")
			.description("The original FlowFile will be routed on any status code that can be retried.")
			.build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	//attributes
	private static final String RESPONSE_ATT = "snowpipe.ingest.reponsecode";

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(SNOWFLAKE_SERVICE);
		descriptors.add(PIPE);
		descriptors.add(FILE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		relationships.add(REL_RETRY);
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
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}

		final ComponentLog logger = getLogger();
		final SnowflakeController snowflakeController = context.getProperty(SNOWFLAKE_SERVICE).asControllerService(SnowflakeController.class);
		final String pipe = context.getProperty(PIPE).evaluateAttributeExpressions(flowFile).getValue();
		final String file = context.getProperty(FILE).evaluateAttributeExpressions(flowFile).getValue();

		try {
			SimpleIngestManager mgr = snowflakeController.getIngestManager(pipe);
			IngestResponse resp = mgr.ingestFile(new StagedFileWrapper(file), null);
			flowFile = session.putAttribute(flowFile, resposneCodeAttribute,  "200");  // Assume 200 if no exceptions are raised.  This information is not available otherwise
			session.transfer(flowFile, REL_SUCCESS);
		} catch (BackOffException backOffException) {
			// We hit a throttling threshold on the Snowflake service
			// They are telling us to back off, route flow file to RETRY and Yield processor

			logger.error("Exceeded Snowpipe usage threshold, backing off. {}",
					new Object[]{pipe, file, backOffException});
			context.yield();
			flowFile = session.putAttribute(flowFile, errorMessageAttribute,  backOffException.toString());
			session.transfer(flowFile, REL_RETRY);

		} catch (IngestResponseException ingestResponseException) {
			logger.error("Unable to call pipe {} on file {} due to {}; routing to failure",
					new Object[]{pipe, file, ingestResponseException});

			// Parse Error Code:
			// The current implementation of IngestResponseException does not expose HTTP Status or Codes Directory
			// Create a Pattern object
			String httpResponseCode = "0";
			Pattern regExPattern = Pattern.compile(responseCodePattern);
			Matcher regExMatcher = regExPattern.matcher(ingestResponseException.toString());
			if (regExMatcher.find( )) {
				httpResponseCode = regExMatcher.group(1);
			}

			// Update the flow file attributes
			flowFile = session.putAttribute(flowFile, resposneCodeAttribute,  httpResponseCode);
			flowFile = session.putAttribute(flowFile, errorMessageAttribute,  ingestResponseException.toString());

			// Route the flow file appropriately
			// 400 — Failure. Invalid request due to an invalid format. 	-> Route to FAILURE
			// 404 — Failure. pipeName not recognized.						-> Route to FAILURE
			// 429 — Failure. Request rate limit exceeded.					-> Route to RETRY, and Yield Processor
			// 500 — Failure. Internal error occurred.						-> Route to RETRY, and Yield Processor
			if (httpResponseCode.contains("429") || httpResponseCode.contains("500")) {
				// Route to RETRY, and Yield Processor
				session.transfer(flowFile, REL_RETRY);
				context.yield();
			} else {
				// Route to FAILURE
				session.transfer(flowFile, REL_FAILURE);
			}
		}
		catch(Exception e) {
			logger.error("Unable to call pipe {} on file {} due to {}; routing to failure",
                    new Object[]{pipe, file, e});
			flowFile = session.putAttribute(flowFile, errorMessageAttribute,  e.toString());
			session.transfer(flowFile, REL_FAILURE);
		}
	}
}

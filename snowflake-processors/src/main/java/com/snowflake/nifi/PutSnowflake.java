package com.snowflake.nifi;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"put", "file", "pipe", "cloud", "database", "stage", "s3", "snowflake"})
@SeeAlso({ExecuteSnowPipe.class, QuerySnowPipeHistory.class, AwaitSnowPipeResults.class})
@CapabilityDescription("Executes a Snowflake Put statement. Files will be staged in Snowflake's internal stage area in S3.  A single flow file will be returned for each file staged to Snowflake's internal staging area.")
@WritesAttributes({
        @WritesAttribute(attribute = "PutSnowflake.source_filename", description = "Source file name of the staged file."),
        @WritesAttribute(attribute = "PutSnowflake.target_filename", description = "Target file name of the staged file."),
        @WritesAttribute(attribute = "PutSnowflake.source_size", description = "Size of the staged file at Source."),
        @WritesAttribute(attribute = "PutSnowflake.target_size", description = "Size of the staged file at Target."),
        @WritesAttribute(attribute = "PutSnowflake.source_compression", description = "Compression of the staged file at Source."),
        @WritesAttribute(attribute = "PutSnowflake.target_compression", description = "Compression of the staged file at Target."),
        @WritesAttribute(attribute = "PutSnowflake.status", description = "Status of the Stage operation [UPLOADED , SKIPPED, FAILED]."),
        @WritesAttribute(attribute = "PutSnowflake.encryption", description = "Indicates if the data is encrypted"),
        @WritesAttribute(attribute = "PutSnowflake.message", description = "Explanation of the status message, if not UPLOADED."),
        @WritesAttribute(attribute = "snowflake.duration", description = "Duration of the put in seconds.")
})
@ReadsAttributes({
//        @ReadsAttribute(attribute = "binary.format", description = "format to use columns formatted in HEX")
})
public class PutSnowflake extends AbstractProcessor {

    // Static Property Values
    public static final String AUTO_COMPRESS_TRUE = "true";
    public static final String AUTO_COMPRESS_FALSE = "false";
    public static final String AUTO_COMPRESS_DEFAULT = "default";
    public static final String SOURCE_COMPRESSION_AUTO_DETECT = "AUTO_DETECT";
    public static final String SOURCE_COMPRESSION_GZIP = "GZIP";
    public static final String SOURCE_COMPRESSION_BZ2 = "BZ2";
    public static final String SOURCE_COMPRESSION_BROTLI = "BROTLI";
    public static final String SOURCE_COMPRESSION_ZSTD = "ZSTD";
    public static final String SOURCE_COMPRESSION_DEFLATE = "DEFLATE";
    public static final String SOURCE_COMPRESSION_RAW_DEFLATE = "RAW_DEFLATE";
    public static final String SOURCE_COMPRESSION_NONE = "NONE";
    public static final String SOURCE_COMPRESSION_DEFAULT = "default";
    public static final int PARALLEL_DEFAULT = 4;

    //////////////////////////////////////////////////////////
    //  Property Descriptor Declarations
    //////////////////////////////////////////////////////////
    public static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("ps-jdbc-connection-pool")
            .displayName("JDBC Connection Pool")
            .description("Specifies the Snowflake JDBC Connection Pool to use for the PUT operation")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();
    public static final PropertyDescriptor INPUT_DIRECTORY = new PropertyDescriptor.Builder()
            .name("ps-input-directory")
            .displayName("Input Directory")
            .description("Input directory from which to pull files.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("ps-file-name")
            .displayName("File Name")
            .description("Lists the specific file to stage to Snowflake with a PUT statement.  This supports the * and ? wild-cards.  "
                       + "If used, wild-cards will potentially result in multiple flow files, one per staged file.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor INTERNAL_STAGE = new PropertyDescriptor.Builder()
            .name("ps-internal-stage")
            .displayName("Internal Stage")
            .description("The Snowflake internal staging area to which the file will be loaded.  If the specified staging area does not exist, the flow-file will be routed to the Error relationship.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor AUTO_COMPRESS = new PropertyDescriptor.Builder()
            .name("ps-auto-compress")
            .displayName("Auto Compress")
            .description("Specifies whether Snowflake compresses files during upload")
            .allowableValues(AUTO_COMPRESS_TRUE, AUTO_COMPRESS_FALSE, AUTO_COMPRESS_DEFAULT)
            .defaultValue(AUTO_COMPRESS_DEFAULT)
            .build();
    public static final PropertyDescriptor SOURCE_COMPRESSION = new PropertyDescriptor.Builder()
            .name("ps-source-compression")
            .displayName("Source Compression")
            .description("Specifies the method of compression used on already-compressed files that are being staged:.")
            .allowableValues(
                    new AllowableValue(SOURCE_COMPRESSION_AUTO_DETECT, "Auto Detect", "Compression algorithm detected automatically, except for Brotli-compressed files, which cannot currently be detected automatically. If loading Brotli-compressed files, explicitly use BROTLI instead of AUTO_DETECT."),
                    new AllowableValue(SOURCE_COMPRESSION_GZIP, "Gzip", "GNU zip"),
                    new AllowableValue(SOURCE_COMPRESSION_BZ2, "BZ2", "Compress files using the Burrows-Wheeler block sorting text compression algorithm, and Huffman coding"),
                    new AllowableValue(SOURCE_COMPRESSION_BROTLI, "Brotli", "Must be used if loading Brotli-compressed files."),
                    new AllowableValue(SOURCE_COMPRESSION_ZSTD, "ZSTD", "Zstandard v0.8 (and higher) supported."),
                    new AllowableValue(SOURCE_COMPRESSION_DEFLATE, "Deflate", "Deflate-compressed files (with zlib header, RFC1950)."),
                    new AllowableValue(SOURCE_COMPRESSION_RAW_DEFLATE, "Raw Deflate","Raw Deflate-compressed files (without header, RFC1951)."),
                    new AllowableValue(SOURCE_COMPRESSION_NONE, "None", "Files for loading data have not been compressed."),
                    new AllowableValue(SOURCE_COMPRESSION_DEFAULT, "default", "Do not specify a value for Source Compression.  Relies on JDBC Driver default behavior."))
            .defaultValue(SOURCE_COMPRESSION_DEFAULT)
            .build();
    public static final PropertyDescriptor PARALLEL = new PropertyDescriptor.Builder()
            .name("ps-parallel")
            .displayName("Parallel")
            .description("[1-99] Specifies the number of threads to use for uploading files. The upload process separate batches of data files by size:\n" +
                    "\n" +
                    "Small files (< 16MB compressed or uncompressed) are staged in parallel as individual files\n" +
                    "Larger files are automatically split into chunks, staged concurrently and reassembled in the target stage. A single thread can upload multiple chunks.\n" +
                    "Increasing the number of threads can improve performance when uploading large files.")
            .defaultValue(String.valueOf(PARALLEL_DEFAULT))
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    //////////////////////////////////////////////////////////
    //  Relationship Declarations
    //////////////////////////////////////////////////////////
    static final Relationship REL_UPLOADED = new Relationship.Builder()
            .name("uploaded")
            .description("A FlowFile is routed to this relationship after the specified file is successfully Uploaded to Snowflake's Internal Staging area.  If a File Name with a wild-card (* | ?) is provided, multiple flow files will be sent to this relationship.  One per staged file.")
            .build();
    static final Relationship REL_SKIPPED = new Relationship.Builder()
            .name("skipped")
            .description("A FlowFile is routed to this relationship after if Snowflake determines this file is already in the Staging area.  This is determined by considering both filename and MF5 hash values. If a File Name with a wild-card (* | ?) is provided, multiple flow files may be sent to this relationship.  One per skipped file.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the file fails to upload to Snowflake Internal Staging area. If a File Name with a wild-card (* | ?) is provided, multiple flow files may be sent to this relationship.  One per failed file.")
            .build();


    //////////////////////////////////////////////////////////
    //  Return property Descriptors
    //////////////////////////////////////////////////////////
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(INPUT_DIRECTORY);
        properties.add(FILE_NAME);
        properties.add(INTERNAL_STAGE);
        properties.add(AUTO_COMPRESS);
        properties.add(SOURCE_COMPRESSION);
        properties.add(PARALLEL);

        return properties;
    }

    //////////////////////////////////////////////////////////
    //  Return Relationships
    //////////////////////////////////////////////////////////
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_UPLOADED);
        rels.add(REL_SKIPPED);
        rels.add(REL_FAILURE);
        return rels;
    }

    //////////////////////////////////////////////////////////
    //  Invoked when flow file comes in
    //////////////////////////////////////////////////////////
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        long triggerProcessorTime = System.currentTimeMillis();

        final long startNanos = System.nanoTime();
        PutSnowflakeProperties properties = getSnowflakeProperties(context, flowFile);

        // Establish the connection to Snowflake
        final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        try (final Connection conn = dbcpService.getConnection(flowFile.getAttributes())) {

            // The current implementation does not accept flow file contents
            // TODO: Write the flow file contents to a temp file (using configuration properties) and call PUT against that file.

            // Generate and execute the Put statement from configuration provided
            List<PutSnowflakeResult> sfResults = this.executePutStatement(conn, properties);

            // Calculate elapsed time and nanoseconds
            String totalProcessTimeInSeconds = Long.toString((System.currentTimeMillis() - triggerProcessorTime) / 1000);
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            // Determine the database URL (for provenance events below)
            String url = "jdbc://unknown-host";
            try {
                url = conn.getMetaData().getURL();
            } catch (final SQLException e) { }

            // Route the FlowFile(s) to their intended relationship(s)
            if (sfResults.size() > 0)
            {
                // There may be multiple results returned.
                int resultIdx = 0;
                Iterator<PutSnowflakeResult> sfResultIterator = sfResults.iterator();
                while (sfResultIterator.hasNext()) {

                    // Get the appropriate flow file to route.
                    // only create more flow files if there are multiple results
                    if (resultIdx > 0) { flowFile = session.create( flowFile ); }
                    resultIdx++;

                    // Update the flow file with the results from snowflake
                    PutSnowflakeResult currentResult = sfResultIterator.next();
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_DURATION, totalProcessTimeInSeconds);
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_SOURCE_FILENAME, currentResult.getSourceFilename());
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_TARGET_FILENAME, currentResult.getTargetFilename());
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_SOURCE_SIZE, currentResult.getSourceSize());
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_TARGET_SIZE, currentResult.getTargetSize());
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_SOURCE_COMPRESSION, currentResult.getSourceComnpression());
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_TARGET_COMPRESSION, currentResult.getTargetCompression());
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_STATUS, currentResult.getStatus());
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_ENCRYPTION, currentResult.getEncryption());
                    flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_MESSAGE, currentResult.getMessage());

                    // Route the flow file to the appropriate relationship
                    if (currentResult.getStatus().equals("UPLOADED")) { session.transfer(flowFile, REL_UPLOADED); }
                    else if (currentResult.getStatus().equals("SKIPPED")) { session.transfer(flowFile, REL_SKIPPED); }
                    else { session.transfer(flowFile, REL_FAILURE); }

                    // Emit a Provenance SEND event
                    session.getProvenanceReporter().send(flowFile, url, transmissionMillis, true);
                }
            }
            else // No flow files returned, there was an error.  We should at least route the original flow file to error
            {
                flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_DURATION, totalProcessTimeInSeconds);
                flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_ERROR, "No results received from PUT statement: " + properties.getPUTStatement());
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (Exception e1) {
            // Failed to open database connection, return FlowFile to it's original queue and yield
            context.yield();
            getLogger().error("Unable to complete PutSnowflake operation due to {}", new Object[]{ e1.getCause()}, e1);
            HandleException(e1, flowFile, session, properties);
        }
    }

    protected PutSnowflakeProperties getSnowflakeProperties(ProcessContext context, FlowFile flowFile) {
        return new PutSnowflakeProperties(this, context, flowFile);
    }

    // Execute the PUT statement generated by the properties object and parse the results into an List of PutSnowflakeResult objects
    private List<PutSnowflakeResult> executePutStatement(Connection con, PutSnowflakeProperties properties)
            throws SQLException {
        List<PutSnowflakeResult> sfResults = new ArrayList<>();

        getLogger().debug("Preparing this Snowflake SQL statement for execution: " + properties.getPUTStatement());

        // Creating a statement object from an active connection
        try (Statement statement = con.createStatement();
             ResultSet resultSet = statement.executeQuery(properties.getPUTStatement())) {

            // Build the list of result objects.
            // This is safe to return whereas the ResultSet will disappear as soon as the statement object goes out of scope.
            while (resultSet.next()) {
                // Create and populate the next snowflakeResult object
                PutSnowflakeResult sfResult = new PutSnowflakeResult();
                sfResult.setSourceFilename(resultSet.getString(1));
                sfResult.setTargetFilename(resultSet.getString(2));
                sfResult.setSourceSize(resultSet.getString(3));
                sfResult.setTargetSize(resultSet.getString(4));
                sfResult.setSourceCompression(resultSet.getString(5));
                sfResult.setTargetCompression(resultSet.getString(6));
                sfResult.setStatus(resultSet.getString(7));
                sfResult.setEncryption(resultSet.getString(8));
                sfResult.setMessage(resultSet.getString(9));
                sfResults.add(sfResult);
            }
        }
        return sfResults;
    }


    private void HandleException(Exception e, FlowFile flowFile, ProcessSession session, PutSnowflakeProperties properties) {
        getLogger().error("Caught Exception during Snowflake ingest", e);

        flowFile = session.putAttribute(flowFile, PutSnowflakeResult.RESULT_ERROR, e.getMessage());
        session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }


    }


    //////////////////////////////////////////////////////////////
    //  Put Snowflake Result Class
    //////////////////////////////////////////////////////////////
    // This is a POCO to hold the results of a PUT statement
    //////////////////////////////////////////////////////////////
    class PutSnowflakeResult {

        // Static Attribute Names
        public static final String RESULT_SOURCE_FILENAME= "PutSnowflake.source_filename";
        public static final String RESULT_TARGET_FILENAME= "PutSnowflake.target_filename";
        public static final String RESULT_SOURCE_SIZE= "PutSnowflake.source_size";
        public static final String RESULT_TARGET_SIZE= "PutSnowflake.target_size";
        public static final String RESULT_SOURCE_COMPRESSION= "PutSnowflake.source_compression";
        public static final String RESULT_TARGET_COMPRESSION= "PutSnowflake.target_compression";
        public static final String RESULT_STATUS= "PutSnowflake.status";
        public static final String RESULT_ENCRYPTION= "PutSnowflake.encryption";
        public static final String RESULT_MESSAGE = "PutSnowflake.message";
        public static final String RESULT_DURATION = "PutSnowflake.duration";
        public static final String RESULT_ERROR = "PutSnowflake.error";

        //////////////////////////////////////////////////////////
        //  Constructor/Initializer
        //////////////////////////////////////////////////////////
        public PutSnowflakeResult() {
        }

        //////////////////////////////////////////////////////////
        // Private Member Variables
        //////////////////////////////////////////////////////////
        private String sourceFilename;
        private String targetFilename;
        private String sourceSize;
        private String targetSize;
        private String sourceCompression;
        private String targetCompression;
        private String status;
        private String encryption;
        private String message;

        //////////////////////////////////////////////////////////
        //  Getters/Setters
        //////////////////////////////////////////////////////////
        public String getSourceFilename() {
            return this.sourceFilename;
        }
        public void setSourceFilename(String value) {
            this.sourceFilename = value;
        }

        public String getTargetFilename() {
            return this.targetFilename;
        }
        public void setTargetFilename(String value) {
            this.targetFilename = value;
        }

        public String getSourceSize() {
            return this.sourceSize;
        }
        public void setSourceSize(String value) {
            this.sourceSize = value;
        }

        public String getTargetSize() {
            return this.targetSize;
        }
        public void setTargetSize(String value) {
            this.targetSize = value;
        }

        public String getSourceComnpression() {
            return this.sourceCompression;
        }
        public void setSourceCompression(String value) {
            this.sourceCompression = value;
        }

        public String getTargetCompression() {
            return this.targetCompression;
        }
        public void setTargetCompression(String value) {
            this.targetCompression = value;
        }

        public String getStatus() {
            return this.status;
        }
        public void setStatus(String value) {
            this.status = value;
        }

        public String getEncryption() {
            return this.encryption;
        }
        public void setEncryption(String value) {
            this.encryption = value;
        }

        public String getMessage() {
            return this.message;
        }
        public void setMessage(String value) {
            this.message = value;
        }
    }

    //////////////////////////////////////////////////////////////
    //  Put Snowflake Properties Class
    //////////////////////////////////////////////////////////////
    // This class exists to avoid class-level member variables on
    // AbstractProcessor implementations.  When running processors
    // Concurrently, NiFi may re-use a processor instance.  If
    // Class level variables are used, the processing can become
    // Tangled with other instances.  This class is instanciated
    // when the onTrigger is called which will tightly scope the
    // values to the single flow file.
    //////////////////////////////////////////////////////////////
    class PutSnowflakeProperties {
        //////////////////////////////////////////////////////////
        //  Constructor/Initializer
        //////////////////////////////////////////////////////////
        public PutSnowflakeProperties(PutSnowflake PutSnowflake, final ProcessContext context, final FlowFile flowFile) {
            // Populate this instance with processor configuration
            setFlowFileUID(flowFile.getAttribute("uuid"));
            setAttributes(flowFile.getAttributes());

            setInputDirectory(context.getProperty(PutSnowflake.INPUT_DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());
            setFileName(context.getProperty(PutSnowflake.FILE_NAME).evaluateAttributeExpressions(flowFile).getValue());
            setInternalStage(context.getProperty(PutSnowflake.INTERNAL_STAGE).evaluateAttributeExpressions(flowFile).getValue());
            setAutoCompress(context.getProperty(PutSnowflake.AUTO_COMPRESS).evaluateAttributeExpressions(flowFile).getValue());
            setSourceCompression(context.getProperty(PutSnowflake.SOURCE_COMPRESSION).evaluateAttributeExpressions(flowFile).getValue());
            setParallel(new Integer(context.getProperty(PutSnowflake.PARALLEL).evaluateAttributeExpressions(flowFile).getValue()));
        }

        // For unit test purpose
        public PutSnowflakeProperties() {
        }

        //////////////////////////////////////////////////////////
        //  Constant Declarations
        //////////////////////////////////////////////////////////
        // Static Template for PUT statement
        public static final String SNOWFLAKE_PUT_TEMPLATE = "PUT file://%s/%s @%s";
        public static final String SNOWFLAKE_ATTRIBUTE_PARALLEL = " PARALLEL = %d";
        public static final String SNOWFLAKE_ATTRIBUTE_AUTO_COMPRESS = " AUTO_COMPRESS = %s";
        public static final String SNOWFLAKE_ATTRIBUTE_SOURCE_COMPRESSION  = " SOURCE_COMPRESSION = %s";

        //////////////////////////////////////////////////////////
        // Private Member Variables
        //////////////////////////////////////////////////////////
        private String inputDirectory;
        private String fileName;
        private String internalStage;
        private String autoCompress = PutSnowflake.AUTO_COMPRESS_DEFAULT;
        private String sourceCompression = PutSnowflake.SOURCE_COMPRESSION_DEFAULT;
        private int parallel = PutSnowflake.PARALLEL_DEFAULT;
        private String _flow_file_uid;
        private Map<String, String> _attributes;

        //////////////////////////////////////////////////////////
        //  Getters/Setters
        //////////////////////////////////////////////////////////
        public String FlowFileUID() {
            return this._flow_file_uid;
        }
        public void setFlowFileUID(String value) {
            this._flow_file_uid = value;
        }

        public Map<String, String> Attributes() {
            return this._attributes;
        }
        public void setAttributes(Map<String, String> value) {
            this._attributes = value;
        }

        public String getInputDirectory() {
            return inputDirectory;
        }
        public void setInputDirectory(String value) { this.inputDirectory = value; }

        public String getFileName() { return fileName; }
        public void setFileName(String value) {
            this.fileName = value;
        }

        public String getInternalStage() {
            return internalStage;
        }
        public void setInternalStage(String value) {
            this.internalStage = value;
        }

        public String getAutoCompress() {
            return autoCompress;
        }
        public void setAutoCompress(String value) {
            this.autoCompress = value;
        }

        public String getSourceCompression() {
            return sourceCompression;
        }
        public void setSourceCompression(String value) {
            this.sourceCompression = value;
        }

        public int getParallel() {
            return parallel;
        }
        public void setParallel(int currentRowCount) {
            this.parallel = currentRowCount;
        }


        //////////////////////////////////////////////////////////
        //  Get PUT SQL
        //////////////////////////////////////////////////////////
        public String getPUTStatement() {
            String putStatement = String.format(SNOWFLAKE_PUT_TEMPLATE, this.getInputDirectory(), this.getFileName(), this.getInternalStage());

            if (!this.getAutoCompress().equals(PutSnowflake.AUTO_COMPRESS_DEFAULT))
            { putStatement += String.format(SNOWFLAKE_ATTRIBUTE_AUTO_COMPRESS, this.getAutoCompress()); }

            if (!this.getSourceCompression().equals(PutSnowflake.SOURCE_COMPRESSION_DEFAULT))
            { putStatement += String.format(SNOWFLAKE_ATTRIBUTE_SOURCE_COMPRESSION, this.getSourceCompression()); }

            if (this.getParallel() != PutSnowflake.PARALLEL_DEFAULT)
            { putStatement += String.format(SNOWFLAKE_ATTRIBUTE_PARALLEL, this.getParallel()); }

            return putStatement;
        }

    }


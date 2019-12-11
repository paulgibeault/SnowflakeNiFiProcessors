package com.snowflake.nifi;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Optional.empty;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPutSnowflake {
    @BeforeClass
    public static void setupBeforeClass() throws IOException {

    }

    @AfterClass
    public static void cleanUpAfterClass() throws Exception {

    }

    @Test
    public void testTranslateNonPrintableWhitespace() {
        String setParameterValue = "myValue\tMore Value\n d";
        setParameterValue = setParameterValue.replaceAll("\\s", " ");
        setParameterValue = setParameterValue.replaceAll("\\P{Print}", "?");
        assert (!setParameterValue.contains("?"));
    }

    @Test
    public void testBasicPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage";
        Assert.assertEquals(expected, actual);
    }


    @Test
    public void testAutoCompressTruePUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setAutoCompress(PutSnowflake.AUTO_COMPRESS_TRUE);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage AUTO_COMPRESS = true";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testAutoCompressFalsePUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setAutoCompress(PutSnowflake.AUTO_COMPRESS_FALSE);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage AUTO_COMPRESS = false";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testAutoCompressDefaultPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setAutoCompress(PutSnowflake.AUTO_COMPRESS_DEFAULT);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testParallel10PUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setParallel(10);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage PARALLEL = 10";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testParallelDefaultPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setParallel(PutSnowflake.PARALLEL_DEFAULT);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionAUTO_DETECTPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_AUTO_DETECT);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage SOURCE_COMPRESSION = AUTO_DETECT";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionRAW_DEFLATEPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_RAW_DEFLATE);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage SOURCE_COMPRESSION = RAW_DEFLATE";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionDEFLATEPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_DEFLATE);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage SOURCE_COMPRESSION = DEFLATE";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionZSTDPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_ZSTD);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage SOURCE_COMPRESSION = ZSTD";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionBROTLIPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_BROTLI);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage SOURCE_COMPRESSION = BROTLI";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionBZ2PUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_BZ2);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage SOURCE_COMPRESSION = BZ2";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionGZIPPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_GZIP);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage SOURCE_COMPRESSION = GZIP";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionDefaultPUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_DEFAULT);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSourceCompressionNonePUTCommand() {
        PutSnowflakeProperties putSnowflakeProperties = getFakePutSnowflakeProperties();

        putSnowflakeProperties.setSourceCompression(PutSnowflake.SOURCE_COMPRESSION_NONE);
        String actual = putSnowflakeProperties.getPUTStatement();
        String expected = "PUT file:///home/account/dir/some_file @database.schema.my_internal_stage SOURCE_COMPRESSION = NONE";
        Assert.assertEquals(expected, actual);
    }

    private PutSnowflakeProperties getFakePutSnowflakeProperties() {
        PutSnowflakeProperties putSnowflakeProperties = new PutSnowflakeProperties();
        putSnowflakeProperties.setInputDirectory("/home/account/dir");
        putSnowflakeProperties.setFileName("some_file");
        putSnowflakeProperties.setInternalStage("database.schema.my_internal_stage");
        //putSnowflakeProperties.setAutoCompress("true");
        //putSnowflakeProperties.setSourceCompression("None");
        //putSnowflakeProperties.setParallel(10);

        RecordSchema mockRecordSchema = mock(RecordSchema.class);
        when(mockRecordSchema.getField(anyString())).thenReturn(empty());

        return putSnowflakeProperties;
    }
}

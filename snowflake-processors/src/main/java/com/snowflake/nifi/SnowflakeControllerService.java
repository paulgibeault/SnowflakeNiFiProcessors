package com.snowflake.nifi;

import net.snowflake.ingest.SimpleIngestManager;
import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;

@Tags({"snowflake", "rest"})
@CapabilityDescription("Provides a controller service to manage connections to the Snowflake REST API.")
public class SnowflakeControllerService extends AbstractControllerService implements SnowflakeController{
    private static final Logger log = LoggerFactory.getLogger(SnowflakeControllerService.class);

    // Property Descriptors
    public static final PropertyDescriptor SNOWFLAKE_URL = new PropertyDescriptor
            .Builder().name("URL")
            .displayName("Snowflake URL")
            .description("Snowflake URL")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT_NUMBER = new PropertyDescriptor
            .Builder().name("Port")
            .displayName("Snowflake Port Number")
            .description("Snowflake Port")
            .required(true)
            .defaultValue("443")
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROTOCOL = new PropertyDescriptor
            .Builder().name("Protocol")
            .displayName("Snowflake Protocol")
            .description("Snowflake Protocol")
            .allowableValues(new AllowableValue("http", "HTTP"),
                    new AllowableValue("https", "HTTPS"))
            .required(true)
            .defaultValue("https")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACCOUNT = new PropertyDescriptor
            .Builder().name("Account")
            .displayName("Snowflake account name")
            .description("Snowflake account name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER = new PropertyDescriptor
            .Builder().name("User")
            .displayName("Snowflake user name")
            .description("Snowflake user name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRIVATEKEY = new PropertyDescriptor
            .Builder().name("PrivateKey")
            .displayName("Private key")
            .description("Private key used to sign JWT tokens.  It must be an unencrypted OpenSSL key that is Base64 encoded.  See: https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe-rest-gs.html#step-3-configure-security-per-user")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> serviceProperties;

    static{
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SNOWFLAKE_URL);
        props.add(PORT_NUMBER);
        props.add(PROTOCOL);
        props.add(ACCOUNT);
        props.add(USER);
        props.add(PRIVATEKEY);
        serviceProperties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return serviceProperties;
    }

    // Private member variables
    private Map<String,SimpleIngestManager> ingestManagerMap = new HashMap<>();
    private String host;
    private Integer portNum;
    private String protocol;
    private String account;
    private String user;
    private PrivateKey privateKey;

    public SimpleIngestManager getIngestManager(String inPipe)
    {
        SimpleIngestManager currentIngestMgr = ingestManagerMap.get(inPipe);
        if (null == currentIngestMgr)
        {
            try {
                currentIngestMgr = new SimpleIngestManager(account, user, inPipe, privateKey, protocol, host, portNum);
                ingestManagerMap.put(inPipe, currentIngestMgr);
            } catch (Exception e)
            {
                log.error("Unable to establish Snowflake connection for pipe: " + inPipe + ":" + e.toString());
            }
        }
        return currentIngestMgr;
    }

    // Implementation for Controller Service initialization
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException{
        log.info("Starting properties file service");
        host = context.getProperty(SNOWFLAKE_URL).getValue();
        portNum = Integer.parseInt(context.getProperty(PORT_NUMBER).getValue());
        protocol = context.getProperty(PROTOCOL).getValue();
        account = context.getProperty(ACCOUNT).getValue();
        user = context.getProperty(USER).toString();

        try {
        final String pk = context.getProperty(PRIVATEKEY).getValue();
        byte[] encoded = Base64.decodeBase64(pk);
        KeyFactory kf = KeyFactory.getInstance("RSA");

        EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        privateKey = kf.generatePrivate(keySpec);
        } catch(Exception e) {
            log.error("Unable to parse private key.  Ensure it is an unencrypted Base64 encoded OpenSSL key");
            throw new InitializationException(e);
        }
    }


}
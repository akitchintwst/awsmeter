package org.apache.jmeter.protocol.aws.lambda;

import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;

import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import org.apache.jmeter.config.Argument;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.aws.AWSClientSDK1;
import org.apache.jmeter.protocol.aws.AWSClientSDK2;
import org.apache.jmeter.protocol.aws.AWSSampler;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Lambda Invoke Sampler class to connect and invoke AWS Lambda functions.
 */
public class LambdaInvoke extends AWSSampler implements AWSClientSDK1 {

    /**
     * Lambda Function Name.
     */
    protected static final String LAMBDA_FUNCTION_NAME = "lambda_function_name";

    /**
     * Lambda Invocation Type.
     */
    protected static final String LAMBDA_INVOCATION_TYPE = "lambda_invocation_type";

    /**
     * Lambda Payload.
     */
    protected static final String LAMBDA_PAYLOAD = "lambda_payload";

    /**
     * List of Arguments to Lambda Invoke.
     */
    private static final List<Argument> LAMBDA_PARAMETERS;
    
    static {
        LAMBDA_PARAMETERS = Arrays.asList(
            new Argument(LAMBDA_FUNCTION_NAME, EMPTY),
            new Argument(LAMBDA_INVOCATION_TYPE, "RequestResponse"),
            new Argument(LAMBDA_PAYLOAD, EMPTY)
        );
    }

    /**
     * Initial values for test parameter. They are shown in Java Request test sampler.
     * AWS parameters and Lambda parameters.
     * @return Arguments to set as default on Java Request.
     */
    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.setArguments(Stream.of(AWS_PARAMETERS, LAMBDA_PARAMETERS)
                .flatMap(List::stream)
                .collect(Collectors.toList()));
        return defaultParameters;
    }
    
    /**
     * Create AWS Lambda Client.
     * @param credentials
     *        Represents the input of JMeter Java Request parameters.
     * @return AWSLambdaClientBuilder extends AwsSyncClientBuilder super class.
     */
	@Override
	public AwsSyncClientBuilder createAWSClient(Map<String, String> credentials) {
        return AWSLambdaClientBuilder.standard()
                .withCredentials(getAWSCredentialsProvider(credentials))
                .withRegion(Regions.fromName(getAWSRegion(credentials)));
    }      
    
    /**
     * Log attribute.
     */
    protected static Logger log = LoggerFactory.getLogger(LambdaInvoke.class);

    /**
     * AWS Lambda Client.
     */
    protected AWSLambda lambdaClient;

    /**
     * Read test parameters and initialize AWS Lambda client.
     * @param context to get the arguments values on Java Sampler.
     */
    @Override
    public void setupTest(JavaSamplerContext context) {
        log.info("Setup Lambda Invoke Sampler.");
        Map<String, String> credentials = new HashMap<>();

        context.getParameterNamesIterator().forEachRemaining(k -> {
            credentials.put(k, context.getParameter(k));
            log.info("Parameter: " + k + ", value: " + credentials.get(k));
        });

        log.info("Create Lambda Client.");
        lambdaClient = (AWSLambda) this.createAWSClient(credentials).build();
    }

    /**
     * Close AWS Lambda Client after run single thread.
     * @param context
     *        Arguments values on Java Sampler.
     */
    @Override
    public void teardownTest(JavaSamplerContext context) {
        log.info("Close Lambda Client.");
        Optional.ofNullable(lambdaClient)
                .ifPresent(client -> client.shutdown());
    }

    /**
     * Run the Lambda invocation.
     * @param context
     *        Arguments values on Java Sampler.
     * @return SampleResult with the result of the Lambda invocation.
     */
    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        result.sampleStart();

        try {
            String functionName = context.getParameter(LAMBDA_FUNCTION_NAME);
            String payload = context.getParameter(LAMBDA_PAYLOAD);
            String invocationType = context.getParameter(LAMBDA_INVOCATION_TYPE, "RequestResponse");

            InvokeRequest invokeRequest = new InvokeRequest()
                    .withFunctionName(functionName)
                    .withPayload(payload)
                    .withInvocationType(invocationType);

            InvokeResult invokeResult = lambdaClient.invoke(invokeRequest);

            String responsePayload = new String(invokeResult.getPayload().array(), StandardCharsets.UTF_8);
            result.setResponseData(responsePayload, StandardCharsets.UTF_8.name());
            result.setSuccessful(true);
        } catch (Exception e) {
            result.setSuccessful(false);
            result.setResponseMessage("Exception: " + e);
            log.error("Error invoking Lambda function", e);
        } finally {
            result.sampleEnd();
        }

        return result;
    }

}

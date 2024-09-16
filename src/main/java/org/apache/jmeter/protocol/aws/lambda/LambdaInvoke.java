package org.apache.jmeter.protocol.aws.lambda;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.nio.charset.StandardCharsets;

public class LambdaInvoke extends AbstractJavaSamplerClient {

    private AWSLambda lambdaClient;

    @Override
    public void setupTest(JavaSamplerContext context) {
        String accessKey = context.getParameter("AWS_ACCESS_KEY_ID");
        String secretKey = context.getParameter("AWS_SECRET_ACCESS_KEY");
        String region = context.getParameter("AWS_REGION");

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        lambdaClient = AWSLambdaClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        result.sampleStart();

        try {
            String functionName = context.getParameter("FUNCTION_NAME");
            String payload = context.getParameter("PAYLOAD");
            String invocationType = context.getParameter("INVOCATION_TYPE", "RequestResponse");

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
        } finally {
            result.sampleEnd();
        }

        return result;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        if (lambdaClient != null) {
            lambdaClient.shutdown();
        }
    }
}

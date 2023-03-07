package com.aws.ec2;
import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;

import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.springframework.boot.SpringApplication;


// STEP 1 : SETTING UP THE REQUIRED AWS SERVICES 

public class Carrecognition {
    public static void main(String[] args) throws IOException, JMSException, InterruptedException {

        SpringApplication.run(Carrecognition.class, args);
/*This line initializes the Spring application context for the Carrecognition class.*/
        Regions image_bucket_region = Regions.US_EAST_1;
        String image_bucket_name = "njit-cs-643";
/*Next we define the AWS region that the application will use for the Amazon S3 
and Amazon Rekognition services. Here we also set the name of s3 bucket which stores our images*/
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(image_bucket_region)
                    .build();
/*Then This block creates a new Amazon S3 client and sets the region to the US_EAST_1 */



// STEP 2 : CREATING A JMS CONNECTION 

            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(),
                    AmazonSQSClientBuilder.defaultClient());
/*The above line creates a new connection factory for the Amazon SQS service, with a default 
ProviderConfiguration and an AmazonSQSClientBuilder with default settings. */
            SQSConnection connection = connectionFactory.createConnection();
/*This line creates a new connection to the Amazon SQS service using the connection factory 
created in the previous line. */




// STEP 3 : CREATING A FIFO QUEUE 

            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
/*This line creates a new Amazon SQS messaging client wrapper for the connection.*/

/*Next the wrapped client object checks if a queue named "car_image_indices.fifo" exists on the Amazon SQS service, and 
creates it with some attributes if it doesn't exist. */

            if (!client.queueExists("car_image_indices.fifo")) {
                Map<String, String> attributes = new HashMap<String, String>();
                attributes.put("FifoQueue", "true");
                attributes.put("ContentBasedDeduplication", "true");
                client.createQueue(new CreateQueueRequest().withQueueName("car_image_indices.fifo").withAttributes(attributes));
            }



// STEP 4 : SETTING UP TO SEND MESSAGES SYNCHRONOUSLY

/*Once the Amazon SQS queue and the connection are established, we should create a JMS 
session that is nontransacted and set to AUTO_ACKNOWLEDGE mode. */
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
/* Next in order to send a text message to the queue, we need to first create a JMS queue 
identity and then a message producer.*/
            Queue queue = session.createQueue("car_image_indices.fifo");
/*Here we create a producer for car_image_indices.fifo queue*/            
            MessageProducer producer = session.createProducer(queue);



//STEP 5 : GETTING OBJECTS FROM S3 IMAGES BUCKET 

            System.out.println("Listening to images from njit-cs-643 bucket");
/* So,here we create a new request to list objects from the Amazon S3 bucket */            
            ListObjectsV2Request image_request = new ListObjectsV2Request().withBucketName(image_bucket_name);
            ListObjectsV2Result image_request_result;

            do {
                image_request_result = s3Client.listObjectsV2(image_request);
/*The above lines retrieves a list of objects from S3 Bucket using the specified request*/
                for (S3ObjectSummary objectSummary : image_request_result.getObjectSummaries()) {
/* Then we loop through each object summary returned by the S3 ListObjectsV2 request*/
/*Then we try to extract the key (name) of an image in an Amazon S3 bucket from the object summary  */
                    String s3_image = objectSummary.getKey();



// STEP 6 : DETECTING CAR LABELS FROM RETREIVED IMAGES USING AMAZON REKOGNITION ( CAR LABEL CONFIDENCE > 90%)

                    AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
/* Here we create a new Amazon Rekognition client using default configuration settings.*/
                    DetectLabelsRequest label_request = new DetectLabelsRequest()
                            .withImage(new Image().withS3Object(new S3Object().withName(s3_image).withBucket(image_bucket_name)))
                            .withMaxLabels(10).withMinConfidence(90F);
/*Next we create a new DetectLabelsRequest object to analyze the image for labels. 
Following which we set the image to be analyzed using the S3 bucket name and file key (i.e. the path).
Here we set the maximum number of labels to be returned by the API as 5 and set 90% as the minimum confidence
for the label to be considered valid */
                    try {
                        DetectLabelsResult label_request_result = rekognitionClient.detectLabels(label_request);
                        List<Label> detected_labels = label_request_result.getLabels();
/* Then we retrieve a list of labels detected in the image and then create a new  Hashtable object to
store the number of times each label is detected. */
 
/*Then we iterate through each label in the list and check if the label is a "Car"
 and if the confidence level is greater than 90. */
                        for (Label label : detected_labels) {
                            if (label.getName().equals("Car") & label.getConfidence() > 90) {
                                System.out.println("The labels which were detected for image " + s3_image);
                                System.out.print("Label Name: " + label.getName() + " ,");
                                System.out.print("Confidence Percentage: " + label.getConfidence().toString() + "\n");
                                System.out.println("The index for" + s3_image + "has been pushed to the queue for text recognition");
                                TextMessage message = session.createTextMessage(objectSummary.getKey());
/* The "JMSXGroupID" property is used to specify the message group that a message belongs to. Message groups are used 
in JMS to ensure that messages are processed in a specific order, and messages with the same group ID are processed sequentially.*/
                                message.setStringProperty("JMSXGroupID", "Default");
                                producer.send(message);
/* Then The producer.send(message) method call sends the message to the JMS destination. */
                            }
                        }
                    } catch (AmazonRekognitionException e) {
                        e.printStackTrace();
                    }
                }
                String token = image_request_result.getNextContinuationToken();
                image_request.setContinuationToken(token);
/* Now since the results are paginated, here we check for a continuation token using 
getNextContinuationToken() and sets the continuation token in a ListObjectsV2Request object using setContinuationToken(). 
This allows the code to retrieve the next page of results in a subsequent call to listObjectsV2().*/
            } while (image_request_result.isTruncated());
/*Here If an SdkClientException is thrown, the code in the catch block will be executed. The e.printStackTrace() method call prints a stack trace of the exception 
to the console, which can help identify the cause of the exception. */
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }
}

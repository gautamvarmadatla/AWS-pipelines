package com.aws.ec2;

import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;

import com.amazon.sqs.javamessaging.*;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;

import java.util.*;
import com.amazonaws.services.rekognition.model.*;
import javax.jms.*;
import javax.jms.Queue;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication

// SETUP TO RECEIVE MESSAGES ASYNCHRONOUSLY 

/*Here we implement the MessageListener interface and When a message is received, 
the onMessage method of the MessageListener interface will be invoked. */

class MyListener implements MessageListener {

    @Override
    public void onMessage(Message message) {


/*Next we define the AWS region that the application will use for the Amazon S3 
and Amazon Rekognition services. Here we also set the name of s3 bucket which stores our images*/
        try {
            Regions image_bucket_region  = Regions.US_EAST_1;
            String image_bucket_name = "njit-cs-643";


/* Next we create a new request to list images from the njit-cs-643 bucket */  
            ListObjectsV2Request image_request = new ListObjectsV2Request().withBucketName(image_bucket_name);
            ListObjectsV2Result image_request_result;
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(image_bucket_region )
                    .build();

/*Then This block creates a new Amazon S3 client and sets the region to the US_EAST_1 */
            AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
                    .withRegion(image_bucket_region )
                    .build();

/*We then try to retrieve a list of objects from S3 Bucket using the specified request*/ 
            image_request_result = s3Client.listObjectsV2(image_request);



/* MOST IMPORTANT PART OF THE CODE  */

/* Then we loop through each object summary returned by the S3 ListObjectsV2 request*/
            for (S3ObjectSummary objectSummary : image_request_result.getObjectSummaries()) {
                String validation = (String) ((TextMessage) message).getText().toString();
                if (objectSummary.getKey().contains(validation)) {
/*As we can see above we try to extract the text message received from the JMS queue and then search for an 
object in the Amazon S3 bucket whose key contains the same text message. */
                    String txt_detection_img = objectSummary.getKey();


/*Then the code in block below creates a new DetectTextRequest object and initializes it 
with a new Image object that represents the image to be processed. The Image object is 
created with a new S3Object object that specifies the name of the image file and the bucket
name where the image is stored. The withName method sets the name of the image file to be 
processed, and the withBucket method specifies the name of the S3 bucket that contains the
image file. Once the Image object is created, it is set as the image to be processed in the
DetectTextRequest object using the withImage method. This DetectTextRequest object is then 
passed to the rekognitionClient.detectText() method to detect text in the specified image. */
                    DetectTextRequest text_detection_request = new DetectTextRequest()
                            .withImage(new Image()
                                    .withS3Object(new S3Object()
                                            .withName(txt_detection_img)
                                            .withBucket(image_bucket_name)));
                    try {
                        DetectTextResult text_detection_result = rekognitionClient.detectText(text_detection_request);
                        List<TextDetection> detected_text = text_detection_result.getTextDetections();
/* We then send a request to Amazon Rekognition to detect text in the image specified in the 
request object. Followin which we extract the list of text detections from the result object 
and store it in the detected_text variable.*/
                        if (!detected_text.isEmpty()) {
                            System.out.println("Car Image :  " + txt_detection_img );
                            for (TextDetection text : detected_text) {
                                System.out.println("Detected Text : " + text.getDetectedText() + " Confidence Percentage : " + text.getConfidence().toString());
                                System.out.println("");
                            }
                        }
                    } catch (AmazonRekognitionException e) {
                        System.out.print("Error");
                        e.printStackTrace();
                    }
                }
            }

        } catch (JMSException e) {
            System.out.println("Please run the first application to detect Cars from images before running text recognition");
        }
    }
}

// STEP1 : SETTING UP THE REQUIRED SERVCES 

public class TextRekognition {
    public static void main(String[] args) throws Exception {
/*Here we run the Spring application with the TextRekognition class */
        SpringApplication.run(TextRekognition.class, args);
        Regions image_bucket_region  = Regions.US_EAST_1;
        try {
/*Next we Create a new instance of the AmazonSQSClientBuilder and sets the region to 
clientRegion. We then build the client to use the specified region. */
            AmazonSQSClientBuilder.standard()
                    .withRegion(image_bucket_region)
                    .build();



// STEP 2 : JMS CONNECTION 

            try {
                SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(),
                        AmazonSQSClientBuilder.defaultClient());
/*The above line creates a new connection factory for the Amazon SQS service, with a default 
ProviderConfiguration and an AmazonSQSClientBuilder with default settings. */
                SQSConnection connection = connectionFactory.createConnection();
/*This line creates a new connection to the Amazon SQS service using the connection factory 
created in the previous line. */ 





// STEP 3 : FIFO QUEUE 

                AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
/*This line creates a new Amazon SQS messaging client wrapper for the connection.*/

/*Next the wrapped client object checks if a queue named "car_image_indices.fifo" exists on the Amazon SQS service, and 
creates it with some attributes if it doesn't exist. */

                if (!client.queueExists("car_image_indices.fifo")) {
                    Map<String, String> attributes = new HashMap<String, String>();
                    attributes.put("FifoQueue", "true");
                    attributes.put("ContentBasedDeduplication", "true");
                    client.createQueue(
                            new CreateQueueRequest().withQueueName("car_image_indices.fifo").withAttributes(attributes));
                }




/* STEP 4 : SETTING UP TO RECEIVE MESSAGES ASYNCHRONOUSLY 
( MessageListener interface used here is implemented in later part of the code )*/



/*Once the Amazon SQS queue and the connection are established, we should create a JMS 
session that is nontransacted and set to AUTO_ACKNOWLEDGE mode. */
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
/* Next in order to send a text message to the queue, we need to first create a JMS queue 
identity and then a message consumer */
                Queue queue = session.createQueue("car_image_indices.fifo");
/* As a next step we create a new session for the connection, with a non-transactional mode and 
auto-acknowledgment mode, following which we create a new queue identify for the session */
                MessageConsumer consumer = session.createConsumer(queue);

/* We next create a new consumer from the same queue and invoke the start method*/
                consumer.setMessageListener(new MyListener());
                connection.start();        
/*Then we set up a message listener to receive messages from the specified queue, and then start 
the connection to receive incoming messages.*/
                Thread.sleep(10000);




// STEP 5 : HANDLING THE EXCEPTION WHEN OBJECT RECOGNITION WAS'NT RUN PRIOR TO TEXT DETECTION

/* In this situation we check if an SQS queue named "car_image_indices.fifo" exists. If it doesn't exist, it creates a 
new queue with the given name and sets two attributes, "FifoQueue" and "ContentBasedDeduplication", to "true". 
It then creates a new session and queue consumer for the "car_image_indices.fifo" queue, sets a message listener, and starts 
receiving incoming messages. Finally, it waits for 10 seconds before continuing.*/ 


            } catch (Exception e) {
                System.out.println("Please run the first application to detect Cars from images before running text recognition");
                SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(),
                        AmazonSQSClientBuilder.defaultClient());
                SQSConnection connection = connectionFactory.createConnection();
                AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
/* The above block of code first creates an Amazon SQS connection factory using the SQSConnectionFactory class with the default ProviderConfiguration
and AmazonSQSClientBuilder.defaultClient() settings. It then creates a connection using this factory and gets the wrapped Amazon SQS client using the 
getWrappedAmazonSQSClient() method of the SQSConnection object.*/

                if (!client.queueExists("car_image_indices.fifo")) {
                    Map<String, String> attributes = new HashMap<String, String>();
                    attributes.put("FifoQueue", "true");
                    attributes.put("ContentBasedDeduplication", "true");
                    client.createQueue(new CreateQueueRequest().withQueueName("car_image_indices.fifo").withAttributes(attributes));
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Queue queue = session.createQueue("car_image_indices.fifo");
                    MessageConsumer consumer = session.createConsumer(queue);
                    consumer.setMessageListener(new MyListener());
                    connection.start();
                    Thread.sleep(10000);
                }
            }
        } catch (AmazonServiceException e) {
            System.out.println("Please run the first application to detect Cars from images before running text recognition");
        }
    }
}





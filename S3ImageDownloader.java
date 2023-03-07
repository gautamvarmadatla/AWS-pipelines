package com.example;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3ImageDownloader {

    public static void main(String[] args) throws IOException {
        String bucketName = "nji-cs-643";
        String key = "image2.jpg";
        String filePath = "Images/image2.jpg";

        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3.getObject(getObjectRequest);

        Files.copy(s3ObjectResponse, Paths.get(filePath));
        System.out.println("Image downloaded successfully!");
    }
}

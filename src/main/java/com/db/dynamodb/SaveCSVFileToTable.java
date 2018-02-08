package com.db.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;

public class SaveCSVFileToTable implements RequestHandler<S3Event, String> {
    private final String CSV_TYPE = (String) "csv";

    public String handleRequest(S3Event s3event, Context context) {
        try {
            String line = "";
            String splitChar = ",";
            String tableName = "Table";

            S3EventNotificationRecord record = s3event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");

            String dstBucket = srcBucket + "processed";
            String successDstKey = "success-" + srcKey;
            String errorDstKey = "error-" + srcKey;

            if (!srcKey.endsWith(CSV_TYPE)) {
                System.out.println("Skipping non-csv " + srcKey);
                return "";
            }

            // Download the image from S3 into a stream
            AmazonS3 s3Client = new AmazonS3Client();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                    srcBucket, srcKey));
            InputStream objectData = s3Object.getObjectContent();

            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                    .withRegion(Regions.EU_WEST_1)
                    .withClientConfiguration(new ClientConfiguration())
                    .build();

            DynamoDB dynamoDB = new DynamoDB(client);

            Table table = dynamoDB.getTable(tableName);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(objectData))) {
                int columnNumber = -1;
                String [] headers = null;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split(splitChar,-1);
                    if (columnNumber==-1){
                        columnNumber = fields.length;
                        headers = fields;
                    }else{
                        Item item = new Item();
                        for (int i=0;i<columnNumber;i++){
                            if (!fields[i].equals("")){
                                item.withString(headers[i], fields[i]);
                            }
                        }
                        try{
                            table.putItem(item);
                        }catch(ProvisionedThroughputExceededException e){
                            System.err.println("Error inserting item: " + fields[0]);
                        }
                    }
                }

                System.out.println("Writing to: " + dstBucket + "/" + successDstKey);
                s3Client.putObject(dstBucket, successDstKey, objectData, new ObjectMetadata());

            } catch (Exception e) {
                System.out.println("Writing to: " + dstBucket + "/" + errorDstKey);
                s3Client.putObject(dstBucket, errorDstKey, objectData, new ObjectMetadata());
                System.out.println("Line: "+line);
                e.printStackTrace();
            }
            return "Ok";

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
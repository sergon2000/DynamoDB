package com.db.dynamodb;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

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
//import com.amazonaws.services.s3.model.S3Event;
import com.amazonaws.services.s3.model.S3Object;

public class SaveCSVFileToTable implements RequestHandler<S3Event, String> {
    /*private static final float MAX_WIDTH = 100;
    private static final float MAX_HEIGHT = 100;*/
    private final String CSV_TYPE = (String) "jpg";
    private final String JPG_TYPE = (String) "jpg";
    private final String JPG_MIME = (String) "image/jpeg";
    /*private final String PNG_TYPE = (String) "png";
    private final String PNG_MIME = (String) "image/png";*/

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

            // Sanity check: validate that source and destination are different
            // buckets.
            /*if (srcBucket.equals(dstBucket)) {
                System.out
                        .println("Destination bucket must not match source bucket.");
                return "";
            }*/

            // Infer the type.
            Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey);
            if (!matcher.matches()) {
                System.out.println("Unable to infer file type for key "
                        + srcKey);
                return "";
            }
            String type = matcher.group(1);
            if (!(CSV_TYPE.equals(type))) {
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
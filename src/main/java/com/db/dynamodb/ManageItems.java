package com.db.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;

import java.sql.Timestamp;

public class ManageItems {
    public static void main(String[] args) throws Exception {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .withClientConfiguration(new ClientConfiguration().withMaxErrorRetry(0))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("Music");

        String artist = "U2";

        int rowNumber = 1;
        int successCount = 0;
        int errorCount = 0;
        do{
            try {
                System.out.println("Adding a new item... "+rowNumber);
                PutItemOutcome outcome = table
                        .putItem(new Item().withString("Artist", artist).withString("SongTitle", "Where the streets have no name"+(rowNumber++)).withString("Album","The Joshua Tree"));

                System.out.println(new Timestamp(System.currentTimeMillis()) +" PutItem succeeded:\n" + outcome.getPutItemResult());
                successCount++;


            }catch (ProvisionedThroughputExceededException e) {
                System.err.println("Unable to add item "+rowNumber);
                errorCount++;
            }catch (Exception e){
                System.err.println(e.getMessage());
                errorCount++;
            }
        }while (rowNumber<=5000);

        System.out.println("Number of inserted rows: "+successCount);
        System.out.println("Number of failed rows: "+errorCount);
    }
}

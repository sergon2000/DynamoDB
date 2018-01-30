package com.db.dynamodb;

import java.util.Arrays;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class MusicCreateTable {
    public static void main(String[] args) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Music";
        try {
            System.out.println("Creating the table, wait...");
            Table table = dynamoDB.createTable (tableName,
                    Arrays.asList (
                            new KeySchemaElement("Artist", KeyType.HASH), // the partition key
                            // the sort key
                            new KeySchemaElement("SongTitle", KeyType.RANGE)
                    ),
                    Arrays.asList (
                            new AttributeDefinition("Artist", ScalarAttributeType.S),
                            new AttributeDefinition("SongTitle", ScalarAttributeType.S)
                    ),
                    new ProvisionedThroughput(5L, 5L)
            );
            table.waitForActive();
            System.out.println("Table created successfully.  Status: " +
                    table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Cannot create the table: ");
            System.err.println(e.getMessage());
        }
    }
}
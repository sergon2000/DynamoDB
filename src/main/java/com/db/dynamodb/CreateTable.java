package com.db.dynamodb;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.Arrays;

public class CreateTable {
    public static void main(String[] args) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Table";
        String partitionKey = "State";
        String sortKey = "CensusTract";

        try {
            System.out.println("Creating the table, wait...");

            Table table = dynamoDB.createTable (tableName,
                    Arrays.asList (
                            new KeySchemaElement(partitionKey, KeyType.HASH),
                            new KeySchemaElement(sortKey, KeyType.RANGE)),
                    Arrays.asList (
                            new AttributeDefinition(partitionKey, ScalarAttributeType.S),
                            new AttributeDefinition(sortKey, ScalarAttributeType.S)
                    ),
                    new ProvisionedThroughput(5L, 25L)
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
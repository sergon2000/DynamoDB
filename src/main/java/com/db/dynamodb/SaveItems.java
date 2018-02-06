package com.db.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class SaveItems {

    public static void main(String[] args) {

        String splitChar = ",";
        String tableName = "Table";

        if (args.length!=1){
            System.out.println("Please specify the csv file: SaveItems <csvFile>");
            return;
        }

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.EU_WEST_1)
                .withClientConfiguration(new ClientConfiguration().withMaxErrorRetry(0))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable(tableName);

        String line = "";

        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(splitChar);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

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
                .withClientConfiguration(new ClientConfiguration())
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable(tableName);

        String line = "";

        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
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

        } catch (Exception e) {
            System.out.println("Line: "+line);
            e.printStackTrace();
        }

    }
}

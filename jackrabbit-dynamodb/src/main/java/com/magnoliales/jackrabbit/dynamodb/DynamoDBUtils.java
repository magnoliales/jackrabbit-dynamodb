package com.magnoliales.jackrabbit.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

final class DynamoDBUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBUtils.class);
    private static final long THROUGHPUT = 25L;
    private static final int MAX_ATTEMPTS = 12;
    private static final int PAUSE = 5000;

    private DynamoDBUtils() {
    }

    static Table getOrCreateTable(AmazonDynamoDB client, String tableName, String attributeName,
                                  boolean createOnMissing) {
        try {
            TableDescription tableDescription = client.describeTable(tableName).getTable();
            if (!tableDescription.getTableStatus().equals(TableStatus.ACTIVE.toString())) {
                String message = "Table " + tableName + " exists but not active. Cannot proceed.";
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
            boolean hashKeyFound = false;
            for (KeySchemaElement keySchemaElement : tableDescription.getKeySchema()) {
                if (keySchemaElement.getAttributeName().equals(attributeName)
                        && keySchemaElement.getKeyType().equals(KeyType.HASH.toString())) {
                    hashKeyFound = true;
                    break;
                }
            }
            if (!hashKeyFound) {
                String message = "A hash key '" + attributeName + "' is required for table " + tableName;
                LOGGER.error(message);
                throw new IllegalStateException(message);
            }
        } catch (ResourceNotFoundException e) {
            LOGGER.info("Table " + tableName + " does not exist");
            if (!createOnMissing) {
                String message = "Cannot proceed without a table. "
                        + "Either create a table manually or set createOnMissing flag";
                LOGGER.error(message, e);
                throw new IllegalStateException(message, e);
            } else {
                LOGGER.info("Creating table " + tableName);
                ArrayList<KeySchemaElement> keySchemaElements = new ArrayList<>();
                ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
                keySchemaElements.add(new KeySchemaElement()
                        .withAttributeName(attributeName)
                        .withKeyType(KeyType.HASH));
                attributeDefinitions.add(new AttributeDefinition()
                        .withAttributeName(attributeName)
                        .withAttributeType(ScalarAttributeType.S));
                ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                        .withReadCapacityUnits(THROUGHPUT)
                        .withWriteCapacityUnits(THROUGHPUT);
                CreateTableRequest createTableRequest = new CreateTableRequest()
                        .withTableName(tableName)
                        .withKeySchema(keySchemaElements)
                        .withAttributeDefinitions(attributeDefinitions)
                        .withProvisionedThroughput(provisionedThroughput);
                try {
                    client.createTable(createTableRequest);
                    waitForTableToBecomeAvailable(client, tableName);
                } catch (AmazonClientException | InterruptedException e1) {
                    String message = "Cannot create table " + tableName;
                    LOGGER.error(message, e1);
                    throw e;
                }
            }
        } catch (AmazonClientException e) {
            String message = "Cannot fetch information for table " + tableName;
            LOGGER.error(message, e);
            throw e;
        }
        return new DynamoDB(client).getTable(tableName);
    }

    static void waitForTableToBecomeAvailable(AmazonDynamoDB client, String tableName) throws InterruptedException {
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            Thread.sleep(PAUSE);
            TableDescription tableDescription = client.describeTable(tableName).getTable();
            if (tableDescription == null) {
                continue;
            }
            String tableStatus = tableDescription.getTableStatus();
            if (tableStatus.equals(TableStatus.ACTIVE.toString())) {
                return;
            }
        }
    }
}

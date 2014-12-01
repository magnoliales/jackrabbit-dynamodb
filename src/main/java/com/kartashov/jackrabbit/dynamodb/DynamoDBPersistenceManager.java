package com.kartashov.jackrabbit.dynamodb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.PMContext;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.util.*;

public class DynamoDBPersistenceManager implements PersistenceManager {

    private static final Logger log = LoggerFactory.getLogger(DynamoDBPersistenceManager.class);

    private boolean initialized;
    private DynamoDB database;
    private Table nodesTable;
    private Table propertiesTable;
    private ObjectMapper mapper;

    @Override
    public void init(PMContext context) throws Exception {
        if (initialized) {
            throw new IllegalStateException("Already initialized");
        }
        log.info("Initializing persistence manager");

        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        AWSCredentials credentials = null; // @todo fix this
        Region region = Region.getRegion(Regions.EU_WEST_1);

        AmazonDynamoDB amazonDynamoDBClient = new AmazonDynamoDBClient(credentials);
        amazonDynamoDBClient.setRegion(region);
        database = new DynamoDB(amazonDynamoDBClient);

        nodesTable = database.getTable("Nodes2");
        propertiesTable = database.getTable("Properties");

        initialized = true;
        log.info("Persistence manager is initialized");
    }

    @Override
    public void close() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
    }

    @Override
    public NodeState createNew(NodeId nodeId) {
        return new NodeState(nodeId, null, null, NodeState.STATUS_NEW, false);
    }

    @Override
    public PropertyState createNew(PropertyId propertyId) {
        return new PropertyState(propertyId, PropertyState.STATUS_NEW, false);
    }

    @Override
    public NodeState load(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        PrimaryKey primaryKey = new PrimaryKey("NodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item = nodesTable.getItem(spec);
        if (item == null) {
            throw new NoSuchItemStateException("Cannot find node " + nodeId.toString());
        }
        String data = item.getJSON("Data");
        if (data == null) {
            throw new ItemStateException("Empty data for node " + nodeId.toString());
        }
        try {
            return mapper.readValue(data, NodeStateData.class).toNodeState(this, nodeId);
        } catch (IOException e) {
            throw new ItemStateException("Cannot read data for node " + nodeId.toString(), e);
        }
    }

    @Override
    public PropertyState load(PropertyId propertyId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        PrimaryKey primaryKey = new PrimaryKey("PropertyId", propertyId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item = propertiesTable.getItem(spec);
        if (item == null) {
            throw new NoSuchItemStateException("Cannot find property " + propertyId.toString());
        }
        String data = item.getJSON("Data");
        if (data == null) {
            throw new ItemStateException("Empty data for property " + propertyId.toString());
        }
        try {
            return mapper.readValue(data, PropertyStateData.class).toPropertyState(this, propertyId);
        } catch (IOException e) {
            throw new ItemStateException("Cannot read data for property " + propertyId.toString(), e);
        }
    }

    @Override
    public NodeReferences loadReferencesTo(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        PrimaryKey primaryKey = new PrimaryKey("NodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("References");
        Item item = nodesTable.getItem(spec);
        if (item == null) {
            throw new NoSuchItemStateException("Cannot find node " + nodeId.toString());
        }
        NodeReferences nodeReferences = new NodeReferences(nodeId);
        Set<String> references = item.getStringSet("References");
        if (references != null) {
            for (String reference : references) {
                try {
                    nodeReferences.addReference(PropertyId.valueOf(reference));
                } catch (IllegalArgumentException e) {
                    throw new ItemStateException("Wrong reference format " + reference, e);
                }
            }
        }
        return nodeReferences;
    }

    @Override
    public boolean exists(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        PrimaryKey primaryKey = new PrimaryKey("NodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("NodeId");
        return nodesTable.getItem(spec) != null;
    }

    @Override
    public boolean exists(PropertyId propertyId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        PrimaryKey primaryKey = new PrimaryKey("PropertyId", propertyId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("PropertyId");
        return propertiesTable.getItem(spec) != null;
    }

    @Override
    public boolean existsReferencesTo(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        PrimaryKey primaryKey = new PrimaryKey("NodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("References");
        Item item = nodesTable.getItem(spec);
        if (item == null) {
            throw new NoSuchItemStateException("Cannot find node " + nodeId.toString());
        }
        Set<String> references = item.getStringSet("References");
        return references != null && references.size() > 0;
    }

    @Override
    public void store(ChangeLog changeLog) throws ItemStateException {

        BatchWriteItemSpec batchWriteItemSpec = new BatchWriteItemSpec();

        TableWriteItems nodesTableWriteItems = new TableWriteItems(nodesTable.getTableName());
        TableWriteItems propertiesTableWriteItems = new TableWriteItems(propertiesTable.getTableName());

        Map<NodeId, Set<String>> modifiedReferences = new HashMap<>();
        for (NodeReferences nodeReferences : changeLog.modifiedRefs()) {
            Set<String> properties = new HashSet<>();
            for (PropertyId propertyId : nodeReferences.getReferences()) {
                properties.add(propertyId.toString());
            }
            modifiedReferences.put(nodeReferences.getTargetId(), properties);
        }

        for (ItemState itemState : changeLog.deletedStates()) {
            if (itemState.isNode()) {
                PrimaryKey primaryKey = new PrimaryKey("NodeId", itemState.getId().toString());
                nodesTableWriteItems.withPrimaryKeysToDelete(primaryKey);
            } else {
                PrimaryKey primaryKey = new PrimaryKey("PropertyId", itemState.getId().toString());
                propertiesTableWriteItems.withPrimaryKeysToDelete(primaryKey);
            }
        }

        List<ItemState> itemStates = new ArrayList<>();
        for (ItemState addedState : changeLog.addedStates()) {
            itemStates.add(addedState);
        }
        for (ItemState modifiedState : changeLog.modifiedStates()) {
            itemStates.add(modifiedState);
        }

        for (ItemState itemState : itemStates) {
            if (itemState.isNode()) {
                NodeState nodeState = (NodeState) itemState;
                NodeId nodeId = nodeState.getNodeId();
                PrimaryKey primaryKey = new PrimaryKey("NodeId", nodeId.toString());
                NodeStateData nodeStateData = new NodeStateData(nodeState);
                String data;
                try {
                    data = mapper.writeValueAsString(nodeStateData);
                } catch (IOException e) {
                    throw new ItemStateException("Cannot serialize node data", e);
                }
                Item item = new Item().withPrimaryKey(primaryKey).withJSON("Data", data);
                if (modifiedReferences.containsKey(nodeId)) {
                    item.withStringSet("References", modifiedReferences.get(nodeId));
                }
                nodesTableWriteItems.withItemsToPut(item);
            } else {
                PropertyState propertyState = (PropertyState) itemState;
                PrimaryKey primaryKey = new PrimaryKey("PropertyId", propertyState.getPropertyId().toString());
                String data;
                try {
                    PropertyStateData propertyStateData = new PropertyStateData(propertyState);
                    data = mapper.writeValueAsString(propertyStateData);
                } catch (RepositoryException | IOException e) {
                    throw new ItemStateException("Cannot read property state", e);
                }
                Item item = new Item().withPrimaryKey(primaryKey).withJSON("Data", data);
                propertiesTableWriteItems.withItemsToPut(item);
            }
        }

        batchWriteItemSpec.withTableWriteItems(nodesTableWriteItems, propertiesTableWriteItems);
        BatchWriteItemOutcome batchWriteItemOutcome = database.batchWriteItem(batchWriteItemSpec);
        log.trace("Items processed " + batchWriteItemOutcome.getBatchWriteItemResult().toString());
    }

    @Override
    public void checkConsistency(String[] strings, boolean b, boolean b1) {
        throw new AssertionError("Not implemented");
    }
}

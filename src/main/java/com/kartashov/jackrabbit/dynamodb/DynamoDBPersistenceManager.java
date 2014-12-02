package com.kartashov.jackrabbit.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
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
        log.trace("Initializing persistence manager");

        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        Region region = Region.getRegion(Regions.EU_WEST_1);

        AmazonDynamoDB amazonDynamoDBClient = new AmazonDynamoDBClient();
        amazonDynamoDBClient.setRegion(region);
        database = new DynamoDB(amazonDynamoDBClient);

        nodesTable = database.getTable("Nodes");
        propertiesTable = database.getTable("Properties");

        initialized = true;
        log.trace("Persistence manager is initialized");
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
        log.trace("Loading node " + nodeId.toString());
        PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item = nodesTable.getItem(spec);
        if (item == null) {
            throw new NoSuchItemStateException("Cannot find node " + nodeId.toString());
        }
        String data = item.getJSONPretty("data");
        log.trace("Data loaded " + data);
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
        log.trace("Loading property " + propertyId.toString());
        PrimaryKey primaryKey = new PrimaryKey("propertyId", propertyId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item = propertiesTable.getItem(spec);
        if (item == null) {
            throw new NoSuchItemStateException("Cannot find property " + propertyId.toString());
        }
        String data = item.getJSON("data");
        log.trace("Data loaded " + data);
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
        log.trace("Loading references to " + nodeId);
        PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("references");
        Item item = nodesTable.getItem(spec);
        if (item == null) {
            throw new NoSuchItemStateException("Cannot find node " + nodeId.toString());
        }
        NodeReferences nodeReferences = new NodeReferences(nodeId);
        Set<String> references = item.getStringSet("references");
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
        log.trace("Check if node " + nodeId.toString() + " exists");
        PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item;
        try {
            item = nodesTable.getItem(spec);
        } catch (AmazonClientException e) {
            throw new ItemStateException("Cannot load node " + nodeId.toString(), e);
        }
        return item != null;
    }

    @Override
    public boolean exists(PropertyId propertyId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        log.trace("Check if property " + propertyId.toString() + " exists");
        PrimaryKey primaryKey = new PrimaryKey("propertyId", propertyId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item;
        try {
            item = propertiesTable.getItem(spec);
        } catch (AmazonClientException e) {
            throw new ItemStateException("Cannot load node " + propertyId.toString(), e);
        }
        return item != null;
    }

    @Override
    public boolean existsReferencesTo(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        log.trace("Check if there are references to node " + nodeId.toString());
        PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("references");
        Item item;
        try {
            item = nodesTable.getItem(spec);
        } catch (AmazonClientException e) {
            throw new ItemStateException("Cannot load references to  " + nodeId.toString(), e);
        }
        if (item == null) {
            throw new NoSuchItemStateException("Cannot find node " + nodeId.toString());
        }
        Set<String> references = item.getStringSet("references");
        return references != null && references.size() > 0;
    }

    @Override
    public void store(ChangeLog changeLog) throws ItemStateException {

        BatchWriteItemSpec batchWriteItemSpec = new BatchWriteItemSpec();

        log.trace("Storing change log");
        log.trace("Items to delete: " + changeLog.deletedStates());
        log.trace("Items to add: " + changeLog.addedStates());
        log.trace("Items to modify: " + changeLog.modifiedStates());

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
                PrimaryKey primaryKey = new PrimaryKey("nodeId", itemState.getId().toString());
                nodesTableWriteItems.withPrimaryKeysToDelete(primaryKey);
                log.trace("Deleting node " + itemState.getId().toString());
            } else {
                PrimaryKey primaryKey = new PrimaryKey("propertyId", itemState.getId().toString());
                propertiesTableWriteItems.withPrimaryKeysToDelete(primaryKey);
                log.trace("Deleting property " + itemState.getId().toString());
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
                PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
                NodeStateData nodeStateData = new NodeStateData(nodeState);
                String data;
                try {
                    data = mapper.writeValueAsString(nodeStateData);
                } catch (IOException e) {
                    throw new ItemStateException("Cannot serialize node data", e);
                }
                Item item = new Item().withPrimaryKey(primaryKey).withJSON("data", data);
                if (modifiedReferences.containsKey(nodeId)) {
                    item.withStringSet("references", modifiedReferences.get(nodeId));
                }
                nodesTableWriteItems.withItemsToPut(item);
                log.trace("Adding new node " + nodeId.toString());
            } else {
                PropertyState propertyState = (PropertyState) itemState;
                PrimaryKey primaryKey = new PrimaryKey("propertyId", propertyState.getPropertyId().toString());
                String data;
                try {
                    PropertyStateData propertyStateData = new PropertyStateData(propertyState);
                    data = mapper.writeValueAsString(propertyStateData);
                } catch (RepositoryException | IOException e) {
                    throw new ItemStateException("Cannot read property state", e);
                }
                Item item = new Item().withPrimaryKey(primaryKey).withJSON("data", data);
                propertiesTableWriteItems.withItemsToPut(item);
                log.trace("Adding new property " + propertyState.getPropertyId().toString());
            }
        }

        batchWriteItemSpec.withTableWriteItems(nodesTableWriteItems, propertiesTableWriteItems);
        try {
            // @todo this does not work!
            BatchWriteItemOutcome batchWriteItemOutcome = database.batchWriteItem(batchWriteItemSpec);
            Map<String, List<WriteRequest>> unprocessedItems = batchWriteItemOutcome.getUnprocessedItems();
            if (unprocessedItems.size() > 0) {
                throw new ItemStateException("could not store everything");
            }
        } catch (AmazonClientException e) {
            throw new ItemStateException("Cannot update database", e);
        }
    }

    @Override
    public void checkConsistency(String[] strings, boolean b, boolean b1) {
        throw new AssertionError("Not implemented");
    }
}

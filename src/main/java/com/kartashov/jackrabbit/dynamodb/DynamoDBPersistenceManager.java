package com.kartashov.jackrabbit.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.CachingPersistenceManager;
import org.apache.jackrabbit.core.persistence.PMContext;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DynamoDBPersistenceManager implements PersistenceManager, CachingPersistenceManager {

    private static final Logger log = LoggerFactory.getLogger(DynamoDBPersistenceManager.class);

    private boolean initialized;
    private Table nodesTable;
    private Table propertiesTable;
    private ObjectMapper mapper;
    private ConcurrentMap<NodeId, NodeState> nodesCache;
    private NodeState missingNodeState;

    @Override
    public void init(PMContext context) throws Exception {

        if (initialized) {
            String message = "Persistence manager is already initialized";
            log.warn(message);
            throw new IllegalStateException(message);
        }

        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        Region region = Region.getRegion(Regions.EU_WEST_1);

        AmazonDynamoDB amazonDynamoDBClient = new AmazonDynamoDBClient();
        amazonDynamoDBClient.setRegion(region);
        DynamoDB database = new DynamoDB(amazonDynamoDBClient);

        nodesTable = database.getTable("Nodes");
        propertiesTable = database.getTable("Properties");

        nodesCache = new ConcurrentHashMap<>();
        missingNodeState = createNew(NodeId.randomId());

        initialized = true;
    }

    @Override
    public void close() throws Exception {
        if (!initialized) {
            String message = "Persistence manager is not initialized";
            log.warn(message);
            throw new IllegalStateException(message);
        }

        nodesCache.clear();
    }

    @Override
    public NodeState createNew(NodeId nodeId) {
        nodesCache.remove(nodeId);
        return new NodeState(nodeId, null, null, NodeState.STATUS_NEW, false);
    }

    @Override
    public PropertyState createNew(PropertyId propertyId) {
        return new PropertyState(propertyId, PropertyState.STATUS_NEW, false);
    }

    @Override
    public NodeState load(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            String message = "Persistence manager is not initialized";
            log.warn(message);
            throw new IllegalStateException(message);
        }
        if (nodesCache.containsKey(nodeId)) {
            NodeState cachedNodeState = nodesCache.get(nodeId);
            if (cachedNodeState == missingNodeState) {
                String message = "Cannot find node " + nodeId.toString();
                log.warn(message);
                throw new NoSuchItemStateException(message);
            } else {
                return cachedNodeState;
            }
        }
        PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item;
        try {
            item = nodesTable.getItem(spec);
        } catch (AmazonClientException e) {
            String message = "Cannot load node " + nodeId.toString();
            log.warn(message, e);
            throw new ItemStateException(message, e);
        }
        if (item == null) {
            nodesCache.putIfAbsent(nodeId, missingNodeState);
            String message = "Cannot find node " + nodeId.toString();
            log.warn(message);
            throw new NoSuchItemStateException(message);
        }
        String data = item.getJSONPretty("data");
        if (data == null) {
            String message = "Empty data for node " + nodeId.toString();
            log.warn(message);
            throw new ItemStateException(message);
        }
        NodeState nodeState;
        try {
            nodeState = mapper.readValue(data, NodeStateData.class).toNodeState(this, nodeId);
        } catch (IOException e) {
            String message = "Cannot read data for node " + nodeId.toString();
            log.warn(message, e);
            throw new ItemStateException(message, e);
        }
        nodesCache.putIfAbsent(nodeId, nodeState);
        return nodeState;
    }

    @Override
    public PropertyState load(PropertyId propertyId) throws ItemStateException {
        if (!initialized) {
            String message = "Persistence manager is not initialized";
            log.warn(message);
            throw new IllegalStateException(message);
        }
        if (nodesCache.get(propertyId.getParentId()) == missingNodeState) {
            String message = "Cannot find property " + propertyId.toString();
            log.warn(message);
            throw new NoSuchItemStateException(message);
        }
        PrimaryKey primaryKey = new PrimaryKey("propertyId", propertyId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item = propertiesTable.getItem(spec);
        if (item == null) {
            String message = "Cannot find property " + propertyId.toString();
            log.warn(message);
            throw new NoSuchItemStateException(message);
        }
        String data = item.getJSON("data");
        if (data == null) {
            String message = "Empty data for property " + propertyId.toString();
            log.warn(message);
            throw new ItemStateException(message);
        }
        try {
            return mapper.readValue(data, PropertyStateData.class).toPropertyState(this, propertyId);
        } catch (IOException e) {
            String message = "Cannot read data for property " + propertyId.toString();
            log.warn(message, e);
            throw new ItemStateException(message, e);
        }
    }

    @Override
    public NodeReferences loadReferencesTo(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            String message = "Persistence manager is not initialized";
            log.warn(message);
            throw new IllegalStateException(message);
        }
        if (nodesCache.get(nodeId) == missingNodeState) {
            String message = "Cannot find node " + nodeId.toString();
            log.warn(message);
            throw new NoSuchItemStateException(message);
        }
        PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("references");
        Item item = nodesTable.getItem(spec);
        if (item == null) {
            String message = "Cannot find node " + nodeId.toString();
            log.warn(message);
            throw new NoSuchItemStateException(message);
        }
        NodeReferences nodeReferences = new NodeReferences(nodeId);
        Set<String> references = item.getStringSet("references");
        if (references != null) {
            for (String reference : references) {
                try {
                    nodeReferences.addReference(PropertyId.valueOf(reference));
                } catch (IllegalArgumentException e) {
                    String message = "Wrong reference format " + reference;
                    log.warn(message, e);
                    throw new ItemStateException(message, e);
                }
            }
        }
        return nodeReferences;
    }

    @Override
    public boolean exists(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            String message = "Persistence manager is not initialized";
            log.warn(message);
            throw new IllegalStateException(message);
        }
        if (nodesCache.get(nodeId) == missingNodeState) {
            return false;
        }
        PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item;
        try {
            item = nodesTable.getItem(spec);
        } catch (AmazonClientException e) {
            String message = "Cannot load node " + nodeId.toString();
            log.warn(message, e);
            throw new ItemStateException(message, e);
        }
        if (item == null) {
            nodesCache.putIfAbsent(nodeId, missingNodeState);
            return false;
        } else {
            return true;
        }
    }

    @Override
    public boolean exists(PropertyId propertyId) throws ItemStateException {
        if (!initialized) {
            String message = "Persistence manager is not initialized";
            log.warn(message);
            throw new IllegalStateException(message);
        }
        if (nodesCache.get(propertyId.getParentId()) == missingNodeState) {
            return false;
        }
        PrimaryKey primaryKey = new PrimaryKey("propertyId", propertyId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item;
        try {
            item = propertiesTable.getItem(spec);
        } catch (AmazonClientException e) {
            String message = "Cannot load node " + propertyId.toString();
            log.warn(message, e);
            throw new ItemStateException(message, e);
        }
        return item != null;
    }

    @Override
    public boolean existsReferencesTo(NodeId nodeId) throws ItemStateException {
        if (!initialized) {
            String message = "Persistence manager is not initialized";
            log.warn(message);
            throw new IllegalStateException(message);
        }
        if (nodesCache.get(nodeId) == missingNodeState) {
            String message = "Cannot find node " + nodeId.toString();
            log.warn(message);
            throw new NoSuchItemStateException(message);
        }
        PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("references");
        Item item;
        try {
            item = nodesTable.getItem(spec);
        } catch (AmazonClientException e) {
            String message = "Cannot load references to  " + nodeId.toString();
            log.warn(message, e);
            throw new ItemStateException(message, e);
        }
        if (item == null) {
            String message = "Cannot find node " + nodeId.toString();
            log.warn(message);
            throw new NoSuchItemStateException(message);
        }
        Set<String> references = item.getStringSet("references");
        return references != null && references.size() > 0;
    }

    @Override
    public synchronized void store(ChangeLog changeLog) throws ItemStateException {

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
                NodeState nodeState = (NodeState) itemState;
                NodeId nodeId = nodeState.getNodeId();
                PrimaryKey primaryKey = new PrimaryKey("nodeId", nodeId.toString());
                DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(primaryKey);
                try {
                    nodesTable.deleteItem(deleteItemSpec);
                } catch (AmazonClientException e) {
                    String message = "Cannot delete node " + nodeId.toString();
                    log.warn(message, e);
                    throw new ItemStateException(message, e);
                }
                nodesCache.putIfAbsent(nodeId, missingNodeState);
            } else {
                PrimaryKey primaryKey = new PrimaryKey("propertyId", itemState.getId().toString());
                DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(primaryKey);
                try {
                    propertiesTable.deleteItem(deleteItemSpec);
                } catch (AmazonClientException e) {
                    String message = "Cannot delete property " + itemState.getId().toString();
                    log.warn(message, e);
                    throw new ItemStateException(message, e);
                }
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
                    String message = "Cannot serialize node data " + nodeId.toString();
                    log.warn(message, e);
                    throw new ItemStateException(message, e);
                }
                Item item = new Item().withPrimaryKey(primaryKey).withJSON("data", data);
                if (modifiedReferences.containsKey(nodeId)) {
                    item.withStringSet("references", modifiedReferences.get(nodeId));
                }
                PutItemSpec putItemSpec = new PutItemSpec().withItem(item);
                try {
                    nodesTable.putItem(putItemSpec);
                } catch (AmazonClientException e) {
                    String message = "Cannot store node " + nodeId.toString();
                    log.warn(message, e);
                    throw new ItemStateException(message, e);
                }
                nodesCache.remove(nodeId);
            } else {
                PropertyState propertyState = (PropertyState) itemState;
                PrimaryKey primaryKey = new PrimaryKey("propertyId", propertyState.getPropertyId().toString());
                String data;
                try {
                    PropertyStateData propertyStateData = new PropertyStateData(propertyState);
                    data = mapper.writeValueAsString(propertyStateData);
                } catch (RepositoryException | IOException e) {
                    String message = "Cannot read property state";
                    log.warn(message, e);
                    throw new ItemStateException(message, e);
                }
                Item item = new Item().withPrimaryKey(primaryKey).withJSON("data", data);
                PutItemSpec putItemSpec = new PutItemSpec().withItem(item);
                try {
                    propertiesTable.putItem(putItemSpec);
                } catch (AmazonClientException e) {
                    String message = "Cannot write property " + itemState.getId().toString();
                    log.warn(message, e);
                    throw new ItemStateException(message, e);
                }
            }
        }
    }

    @Override
    public void checkConsistency(String[] strings, boolean b, boolean b1) {
        throw new AssertionError("Not implemented");
    }

    @Override
    public void onExternalUpdate(ChangeLog changes) {
        for (ItemState itemState : changes.addedStates()) {
            if (itemState.isNode()) {
                NodeId nodeId = (NodeId) itemState.getId();
                nodesCache.remove(nodeId);
            }
        }
        for (ItemState itemState : changes.modifiedStates()) {
            if (itemState.isNode()) {
                NodeId nodeId = (NodeId) itemState.getId();
                nodesCache.remove(nodeId);
            }
        }
        for (ItemState itemState : changes.deletedStates()) {
            if (itemState.isNode()) {
                NodeId nodeId = (NodeId) itemState.getId();
                nodesCache.putIfAbsent(nodeId, missingNodeState);
            }
        }
    }
}

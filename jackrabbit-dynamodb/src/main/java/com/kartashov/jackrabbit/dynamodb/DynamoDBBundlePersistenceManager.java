package com.kartashov.jackrabbit.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.PMContext;
import org.apache.jackrabbit.core.persistence.bundle.AbstractBundlePersistenceManager;
import org.apache.jackrabbit.core.persistence.util.BLOBStore;
import org.apache.jackrabbit.core.persistence.util.NodePropBundle;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NoSuchItemStateException;
import org.apache.jackrabbit.core.state.NodeReferences;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is a persistence manager that stores the {@link NodePropBundle}s in a DynamoDB table.
 *
 * <p>
 * Configuration:<br>
 * <ul>
 * <li>&lt;param name="{@link #setTable(String) table}" value=""/>
 * <li>&lt;param name="{@link #setRegion(String) region}" value=""/>
 * <li>&lt;param name="{@link #setConsistencyCheck(String) consistencyCheck}" value="false"/>
 * <li>&lt;param name="{@link #setConsistencyFix(String) consistencyFix}" value="false"/>
 * <li>&lt;param name="{@link #setCreateOnMissing(String) createOnMissing}" value="false"/>
 * </ul>
 */
public class DynamoDBBundlePersistenceManager extends AbstractBundlePersistenceManager {

    private static final Logger log = LoggerFactory.getLogger(DynamoDBBundlePersistenceManager.class);
    private static final String ID_ATTRIBUTE = "id";

    private String tableName;
    private ObjectMapper mapper;
    private Table table;
    private Region region;
    private boolean initialized;
    private boolean consistencyCheck;
    private boolean consistencyFix;
    private boolean createOnMissing;

    /**
     * Set DynamoDB table name where the bundles need to be stored.
     * @param tableName the case sensitive name of the table
     */
    public void setTable(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Set AWS Region name.
     * @param regionName the name of the region as specified in {@link com.amazonaws.regions.Region}.
     */
    public void setRegion(String regionName) {
        region = RegionUtils.getRegion(regionName);
        if (region == null) {
            String message = "Cannot get region with name " + regionName;
            log.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Defines if a consistency check is to be performed on initialization.
     * @param consistencyCheck the consistency check flag.
     */
    public void setConsistencyCheck(String consistencyCheck) {
        this.consistencyCheck = Boolean.valueOf(consistencyCheck);
    }

    /**
     * Defines if the consistency check should attempt to fix issues that it finds.
     * @param consistencyFix the consistency fix flag.
     */
    public void setConsistencyFix(String consistencyFix) {
        this.consistencyFix = Boolean.valueOf(consistencyFix);
    }

    /**
     * Defines if a new table is created if not found in the database.
     * @param createOnMissing the missing table creation flag.
     */
    public void setCreateOnMissing(String createOnMissing) {
        this.createOnMissing = Boolean.valueOf(createOnMissing);
    }

    @Override
    public void init(PMContext context) throws Exception {

        if (initialized) {
            String message = "Persistence manager is already initialized";
            log.error(message);
            throw new IllegalStateException(message);
        }

        if (tableName == null) {
            String message = "Table name is not set";
            log.warn(message);
            throw new IllegalStateException(message);
        }

        super.init(context);

        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        AmazonDynamoDB client = new AmazonDynamoDBClient();
        client.setRegion(region);

        table = DynamoDBUtils.getOrCreateTable(client, tableName, ID_ATTRIBUTE, createOnMissing);

        if (consistencyCheck) {
            log.info("Consistency check requested");
            checkConsistency(null, true, consistencyFix);
        }

        initialized = true;
    }

    @Override
    protected NodePropBundle loadBundle(NodeId nodeId) throws ItemStateException {
        PrimaryKey primaryKey = new PrimaryKey(ID_ATTRIBUTE, nodeId.toString());
        GetItemSpec getItemSpec = new GetItemSpec().withPrimaryKey(primaryKey);
        Item item;
        try {
            item = table.getItem(getItemSpec);
        } catch (AmazonClientException e) {
            String message = "Cannot load bundle " + nodeId.toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
        if (item == null) {
            return null;
        }
        String data = item.getJSON("data");
        if (data == null) {
            String message = "Bundle data is missing " + nodeId.toString();
            log.error(message);
            throw new IllegalStateException(message);
        }
        try {
            return mapper.readValue(data, NodePropBundleData.class).toNodePropBundle(this, nodeId);
        } catch (IOException | URISyntaxException | ParseException e) {
            String message = "Cannot deserialize bundle data " + nodeId.toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
    }

    @Override
    protected void storeBundle(NodePropBundle bundle) throws ItemStateException {
        PrimaryKey primaryKey = new PrimaryKey(ID_ATTRIBUTE, bundle.getId().toString());
        String data;
        try {
            data = mapper.writeValueAsString(new NodePropBundleData(bundle));
        } catch (IOException | RepositoryException e) {
            String message = "Cannot serialize bundle data " + bundle.getId().toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
        Item item = new Item().withPrimaryKey(primaryKey).withJSON("data", data);
        PutItemSpec putItemSpec = new PutItemSpec().withItem(item);
        try {
            table.putItem(putItemSpec);
        } catch (AmazonClientException e) {
            String message = "Cannot store bundle " + bundle.getId().toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
    }

    @Override
    protected void destroyBundle(NodePropBundle bundle) throws ItemStateException {
        PrimaryKey primaryKey = new PrimaryKey(ID_ATTRIBUTE, bundle.getId().toString());
        DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(primaryKey);
        try {
            table.deleteItem(deleteItemSpec);
        } catch (AmazonClientException e) {
            String message = "Cannot delete bundle " + bundle.getId().toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
    }

    @Override
    protected void destroy(NodeReferences refs) throws ItemStateException {
        PrimaryKey primaryKey = new PrimaryKey(ID_ATTRIBUTE, refs.getTargetId().toString());
        AttributeUpdate attributeUpdate = new AttributeUpdate("references").delete();
        try {
            table.updateItem(primaryKey, attributeUpdate);
        } catch (AmazonClientException e) {
            String message = "Cannot remove references to " + refs.getTargetId().toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
    }

    @Override
    protected void store(NodeReferences refs) throws ItemStateException {
        Set<String> references = new HashSet<>();
        for (PropertyId propertyId : refs.getReferences()) {
            references.add(propertyId.toString());
        }
        PrimaryKey primaryKey = new PrimaryKey(ID_ATTRIBUTE, refs.getTargetId().toString());
        AttributeUpdate attributeUpdate = new AttributeUpdate("references").put(references);
        try {
            table.updateItem(primaryKey, attributeUpdate);
        } catch (AmazonClientException e) {
            String message = "Cannot update references for " + refs.getTargetId().toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
    }

    @Override
    protected BLOBStore getBlobStore() {
        return null;
    }

    @Override
    public List<NodeId> getAllNodeIds(NodeId after, int maxCount) throws ItemStateException, RepositoryException {
        List<NodeId> nodeIds = new ArrayList<>();
        ScanSpec scanSpec = new ScanSpec().withAttributesToGet(ID_ATTRIBUTE);
        if (after != null) {
            PrimaryKey primaryKey = new PrimaryKey(ID_ATTRIBUTE, nodeIds.toString());
            scanSpec.withExclusiveStartKey(primaryKey);
        }
        if (maxCount != 0) {
            scanSpec.withMaxPageSize(maxCount);
        }
        for (Item item : table.scan(scanSpec)) {
            NodeId nodeId = NodeId.valueOf(item.getString(ID_ATTRIBUTE));
            nodeIds.add(nodeId);
        }
        return nodeIds;
    }

    @Override
    public NodeReferences loadReferencesTo(NodeId targetId) throws ItemStateException {
        PrimaryKey primaryKey = new PrimaryKey(ID_ATTRIBUTE, targetId.toString());
        GetItemSpec getItemSpec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("references");
        Item item;
        try {
            item = table.getItem(getItemSpec);
        } catch (AmazonClientException e) {
            String message = "Cannot load references to " + targetId.toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
        if (item == null) {
            String message = "Cannot find node " + targetId.toString();
            log.error(message);
            throw new NoSuchItemStateException(message);
        }
        NodeReferences nodeReferences = new NodeReferences(targetId);
        Set<String> references = item.getStringSet("references");
        if (references != null) {
            for (String reference : references) {
                nodeReferences.addReference(PropertyId.valueOf(reference));
            }
        }
        return nodeReferences;
    }

    @Override
    public boolean existsReferencesTo(NodeId targetId) throws ItemStateException {
        PrimaryKey primaryKey = new PrimaryKey(ID_ATTRIBUTE, targetId.toString());
        GetItemSpec getItemSpec = new GetItemSpec().withPrimaryKey(primaryKey).withAttributesToGet("references");
        Item item;
        try {
            item = table.getItem(getItemSpec);
        } catch (AmazonClientException e) {
            String message = "Cannot load references to " + targetId.toString();
            log.error(message, e);
            throw new ItemStateException(message, e);
        }
        if (item == null) {
            String message = "Cannot find node " + targetId.toString();
            log.error(message);
            throw new NoSuchItemStateException(message);
        }
        Set<String> references = item.getStringSet("references");
        return references != null && references.size() > 0;
    }
}

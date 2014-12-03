package com.kartashov.jackrabbit.dynamodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.persistence.util.NodePropBundle;
import org.apache.jackrabbit.core.state.PropertyState;
import org.apache.jackrabbit.core.value.InternalValue;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.NameFactory;
import org.apache.jackrabbit.spi.Path;
import org.apache.jackrabbit.spi.commons.name.NameFactoryImpl;
import org.apache.jackrabbit.spi.commons.name.PathFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.*;

import static org.apache.jackrabbit.core.persistence.util.NodePropBundle.ChildNodeEntry;
import static org.apache.jackrabbit.core.persistence.util.NodePropBundle.PropertyEntry;

public class NodePropBundleData {

    private String nodeType;
    private String parentId;
    private Set<String> mixinTypes;
    private short modCount;
    private Set<Property> properties;
    private List<Child> children;

    @JsonCreator
    public NodePropBundleData(@JsonProperty("nodeType") String nodeType,
                              @JsonProperty("parentId") String parentId,
                              @JsonProperty("mixinTypes") Set<String> mixinTypes,
                              @JsonProperty("modCount") short modCount,
                              @JsonProperty("properties") Set<Property> properties,
                              @JsonProperty("children") List<Child> children) {
        this.nodeType = nodeType;
        this.parentId = parentId;
        this.mixinTypes = mixinTypes;
        this.modCount = modCount;
        this.properties = properties;
        this.children = children;
    }

    public NodePropBundleData(NodePropBundle nodePropBundle) throws RepositoryException {
        nodeType = nodePropBundle.getNodeTypeName().toString();
        NodeId pid = nodePropBundle.getParentId();
        if (pid != null) {
            parentId = pid.toString();
        }
        mixinTypes = new HashSet<>();
        for (Name mixinTypeName : nodePropBundle.getMixinTypeNames()) {
            mixinTypes.add(mixinTypeName.toString());
        }
        modCount = nodePropBundle.getModCount();
        properties = new HashSet<>();
        for (PropertyEntry propertyEntry : nodePropBundle.getPropertyEntries()) {
            properties.add(new Property(propertyEntry));
        }
        children = new ArrayList<>();
        for (ChildNodeEntry childNodeEntry : nodePropBundle.getChildNodeEntries()) {
            children.add(new Child(childNodeEntry));
        }
    }

    public NodePropBundle toNodePropBundle(PersistenceManager pm, NodeId nodeId) {
        NameFactory nameFactory = NameFactoryImpl.getInstance();
        NodePropBundle nodePropBundle = new NodePropBundle(pm.createNew(nodeId));

        nodePropBundle.setNodeTypeName(nameFactory.create(nodeType));
        if (parentId != null) {
            nodePropBundle.setParentId(NodeId.valueOf(parentId));
        }
        Set<Name> mixinTypeNames = new HashSet<>();
        if (mixinTypes != null) {
            for (String mixinType : mixinTypes) {
                mixinTypeNames.add(nameFactory.create(mixinType));
            }
        }
        nodePropBundle.setMixinTypeNames(mixinTypeNames);
        nodePropBundle.setModCount(modCount);
        if (properties != null) {
            for (Property property : properties) {
                Name propertyName = nameFactory.create(property.getName());
                PropertyId propertyId = new PropertyId(nodeId, propertyName);
                nodePropBundle.addProperty(property.toPropertyEntry(pm, propertyId));
            }
        }
        if (children != null) {
            for (Child child : children) {
                Name childName = nameFactory.create(child.getName());
                NodeId childNodeId = NodeId.valueOf(child.getNodeId());
                nodePropBundle.addChildNodeEntry(childName, childNodeId);
            }
        }
        return nodePropBundle;
    }

    @JsonProperty
    public String getNodeType() {
        return nodeType;
    }

    @JsonProperty
    public String getParentId() {
        return parentId;
    }

    @JsonProperty
    public Set<String> getMixinTypes() {
        return mixinTypes;
    }

    @JsonProperty
    public short getModCount() {
        return modCount;
    }

    @JsonProperty
    public Set<Property> getProperties() {
        return properties;
    }

    @JsonProperty
    public List<Child> getChildren() {
        return children;
    }

    public static class Property {

        private static final Logger log = LoggerFactory.getLogger(Property.class);

        private String name;
        private String type;
        private boolean multiValued;
        private short modCount;
        private List<Object> values;

        @JsonCreator
        public Property(@JsonProperty("name") String name,
                        @JsonProperty("type") String type,
                        @JsonProperty("multiValued") boolean multiValued,
                        @JsonProperty("modCount") short modCount,
                        @JsonProperty("values") List<Object> values) {
            this.name = name;
            this.type = type;
            this.multiValued = multiValued;
            this.modCount = modCount;
            this.values = values;
        }

        public Property(PropertyEntry propertyEntry) throws RepositoryException {
            name = propertyEntry.getName().toString();
            type = PropertyType.nameFromValue(propertyEntry.getType());
            multiValued = propertyEntry.isMultiValued();
            this.modCount = propertyEntry.getModCount();
            values = new ArrayList<>();
            for (InternalValue internalValue : propertyEntry.getValues()) {
                switch (propertyEntry.getType()) {
                    case PropertyType.BOOLEAN:
                        values.add(internalValue.getBoolean());
                        break;
                    case PropertyType.DATE:
                        values.add(internalValue.getDate().getTimeInMillis());
                        break;
                    case PropertyType.DECIMAL:
                        values.add(internalValue.getDecimal().toPlainString());
                        break;
                    case PropertyType.DOUBLE:
                        values.add(Double.toString(internalValue.getDouble()));
                        break;
                    case PropertyType.LONG:
                        values.add(internalValue.getLong());
                        break;
                    case PropertyType.NAME:
                        values.add(internalValue.getName().toString());
                        break;
                    case PropertyType.PATH:
                        values.add(internalValue.getPath().toString());
                        break;
                    case PropertyType.STRING:
                        String s = internalValue.getString();
                        if (s.equals("")) {
                            values.add(Boolean.FALSE);
                        } else {
                            values.add(s);
                        }
                        break;
                    default:
                        log.error("Serializer is not implemented for type " + type);
                        throw new AssertionError("Not implemented for " + type);
                }
            }
        }

        public PropertyEntry toPropertyEntry(PersistenceManager pm, PropertyId propertyId) {
            PropertyState propertyState = pm.createNew(propertyId);
            propertyState.setType(PropertyType.valueFromName(type));
            propertyState.setMultiValued(multiValued);
            propertyState.setModCount(modCount);
            List<InternalValue> internalValues = new ArrayList<>();
            for (Object value : values) {
                switch (propertyState.getType()) {
                    case PropertyType.BOOLEAN:
                        internalValues.add(InternalValue.create((Boolean) value));
                        break;
                    case PropertyType.DATE:
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTimeInMillis((Long) value);
                        internalValues.add(InternalValue.create(calendar));
                        break;
                    case PropertyType.DECIMAL:
                        DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getInstance(Locale.US);
                        decimalFormat.setParseBigDecimal(true);
                        BigDecimal decimal = (BigDecimal) decimalFormat.parse((String) value, new ParsePosition(0));
                        internalValues.add(InternalValue.create(decimal));
                        break;
                    case PropertyType.DOUBLE:
                        internalValues.add(InternalValue.create(Double.valueOf((String) value)));
                        break;
                    case PropertyType.LONG:
                        internalValues.add(InternalValue.create(Long.valueOf(value.toString())));
                        break;
                    case PropertyType.NAME:
                        Name name = NameFactoryImpl.getInstance().create((String) value);
                        internalValues.add(InternalValue.create(name));
                        break;
                    case PropertyType.PATH:
                        Path path = PathFactoryImpl.getInstance().create((String) value);
                        internalValues.add(InternalValue.create(path));
                        break;
                    case PropertyType.STRING:
                        if (value instanceof Boolean) {
                            internalValues.add(InternalValue.create(""));
                        } else {
                            internalValues.add(InternalValue.create((String) value));
                        }
                        break;
                    default:
                        log.error("Deserializer is not implemented for type " + type);
                        throw new AssertionError("Not implemented for " + type);
                }
            }
            propertyState.setValues(internalValues.toArray(new InternalValue[internalValues.size()]));
            return new PropertyEntry(propertyState);
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @JsonProperty
        public String getType() {
            return type;
        }

        @JsonProperty
        public boolean isMultiValued() {
            return multiValued;
        }

        @JsonProperty
        public short getModCount() {
            return modCount;
        }

        @JsonProperty
        public List<Object> getValues() {
            return values;
        }
    }

    public static class Child {

        private String name;
        private String nodeId;

        @JsonCreator
        public Child(@JsonProperty("name") String name,
                     @JsonProperty("nodeId") String nodeId) {
            this.name = name;
            this.nodeId = nodeId;
        }

        public Child(ChildNodeEntry childNodeEntry) {
            name = childNodeEntry.getName().toString();
            nodeId = childNodeEntry.getId().toString();
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @JsonProperty
        public String getNodeId() {
            return nodeId;
        }
    }
}



package com.kartashov.jackrabbit.dynamodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.ChildNodeEntry;
import org.apache.jackrabbit.core.state.NodeState;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.NameFactory;
import org.apache.jackrabbit.spi.commons.name.NameFactoryImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NodeStateData {

    private String nodeType;
    private String parentId;
    private Set<String> mixinTypes;
    private short modCount;
    private Set<String> properties;
    private List<Child> children;

    @JsonCreator
    public NodeStateData(@JsonProperty("nodeType") String nodeType,
                         @JsonProperty("parentId") String parentId,
                         @JsonProperty("mixinTypes") Set<String> mixinTypes,
                         @JsonProperty("modCount") short modCount,
                         @JsonProperty("properties") Set<String> properties,
                         @JsonProperty("children") List<Child> children) {
        this.nodeType = nodeType;
        this.parentId = parentId;
        this.mixinTypes = mixinTypes;
        this.modCount = modCount;
        this.properties = properties;
        this.children = children;
    }

    public NodeStateData(NodeState nodeState) {
        nodeType = nodeState.getNodeTypeName().toString();
        NodeId pid = nodeState.getParentId();
        if (pid != null) {
            parentId = pid.toString();
        }
        mixinTypes = new HashSet<>();
        for (Name mixinTypeName : nodeState.getMixinTypeNames()) {
            mixinTypes.add(mixinTypeName.toString());
        }
        modCount = nodeState.getModCount();
        properties = new HashSet<>();
        for (Name property : nodeState.getPropertyNames()) {
            properties.add(property.toString());
        }
        children = new ArrayList<>();
        for (ChildNodeEntry childNodeEntry : nodeState.getChildNodeEntries()) {
            children.add(new Child(childNodeEntry));
        }
    }

    public NodeState toNodeState(PersistenceManager pm, NodeId nodeId) {
        NameFactory nameFactory = NameFactoryImpl.getInstance();
        NodeState nodeState = pm.createNew(nodeId);
        nodeState.setNodeTypeName(nameFactory.create(nodeType));
        if (parentId != null) {
            nodeState.setParentId(NodeId.valueOf(parentId));
        }
        Set<Name> mixinTypeNames = new HashSet<>();
        if (mixinTypes != null) {
            for (String mixinType : mixinTypes) {
                mixinTypeNames.add(nameFactory.create(mixinType));
            }
        }
        nodeState.setMixinTypeNames(mixinTypeNames);
        nodeState.setModCount(modCount);
        Set<Name> propertyNames = new HashSet<>();
        for (String property : properties) {
            propertyNames.add(nameFactory.create(property));
        }
        nodeState.setPropertyNames(propertyNames);
        if (children != null) {
            for (Child child : children) {
                Name childName = nameFactory.create(child.getName());
                NodeId childNodeId = NodeId.valueOf(child.getNodeId());
                nodeState.addChildNodeEntry(childName, childNodeId);
            }
        }
        return nodeState;
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
    public Set<String> getProperties() {
        return properties;
    }

    @JsonProperty
    public List<Child> getChildren() {
        return children;
    }

    public static class Child {

        private Name name;
        private NodeId nodeId;

        @JsonCreator
        public Child(@JsonProperty("name") String name,
                     @JsonProperty("nodeId") String nodeId) {
            this.name = NameFactoryImpl.getInstance().create(name);
            this.nodeId = NodeId.valueOf(nodeId);
        }

        public Child(ChildNodeEntry childNodeEntry) {
            name = childNodeEntry.getName();
            nodeId = childNodeEntry.getId();
        }

        @JsonProperty
        public String getName() {
            return name.toString();
        }

        @JsonProperty
        public String getNodeId() {
            return nodeId.toString();
        }
    }
}

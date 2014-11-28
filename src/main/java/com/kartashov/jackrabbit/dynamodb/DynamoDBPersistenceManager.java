package com.kartashov.jackrabbit.dynamodb;

import org.apache.jackrabbit.core.cache.Cache;
import org.apache.jackrabbit.core.cache.CacheAccessListener;
import org.apache.jackrabbit.core.cluster.UpdateEventChannel;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.CachingPersistenceManager;
import org.apache.jackrabbit.core.persistence.IterablePersistenceManager;
import org.apache.jackrabbit.core.persistence.PMContext;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.persistence.check.ConsistencyCheckListener;
import org.apache.jackrabbit.core.persistence.check.ConsistencyChecker;
import org.apache.jackrabbit.core.persistence.check.ConsistencyReport;
import org.apache.jackrabbit.core.persistence.util.NodeInfo;
import org.apache.jackrabbit.core.state.*;

import javax.jcr.RepositoryException;
import java.util.List;
import java.util.Map;

public class DynamoDBPersistenceManager implements PersistenceManager, CachingPersistenceManager,
                                                   IterablePersistenceManager, CacheAccessListener,
                                                   ConsistencyChecker {

    @Override
    public void init(PMContext pmContext) throws Exception {
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public NodeState createNew(NodeId nodeId) {
        return null;
    }

    @Override
    public PropertyState createNew(PropertyId propertyId) {
        return null;
    }

    @Override
    public NodeState load(NodeId nodeId) throws ItemStateException {
        return null;
    }

    @Override
    public PropertyState load(PropertyId propertyId) throws ItemStateException {
        return null;
    }

    @Override
    public NodeReferences loadReferencesTo(NodeId nodeId) throws ItemStateException {
        return null;
    }

    @Override
    public boolean exists(NodeId nodeId) throws ItemStateException {
        return false;
    }

    @Override
    public boolean exists(PropertyId propertyId) throws ItemStateException {
        return false;
    }

    @Override
    public boolean existsReferencesTo(NodeId nodeId) throws ItemStateException {
        return false;
    }

    @Override
    public void store(ChangeLog changeLog) throws ItemStateException {

    }

    @Override
    public void checkConsistency(String[] strings, boolean b, boolean b1) {

    }

    @Override
    public void cacheAccessed(long accessCount) {

    }

    @Override
    public void disposeCache(Cache cache) {

    }

    @Override
    public void onExternalUpdate(ChangeLog changes) {

    }

    @Override
    public void setEventChannel(UpdateEventChannel eventChannel) {

    }

    @Override
    public ConsistencyReport check(String[] uuids, boolean recursive, boolean fix, String lostNFoundId, ConsistencyCheckListener listener) throws RepositoryException {
        return null;
    }

    @Override
    public List<NodeId> getAllNodeIds(NodeId after, int maxCount) throws ItemStateException, RepositoryException {
        return null;
    }

    @Override
    public Map<NodeId, NodeInfo> getAllNodeInfos(NodeId after, int maxCount) throws ItemStateException, RepositoryException {
        return null;
    }
}

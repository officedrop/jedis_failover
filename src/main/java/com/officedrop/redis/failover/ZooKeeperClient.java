package com.officedrop.redis.failover;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * User: Maurício Linhares
 * Date: 12/18/12
 * Time: 4:19 PM
 */

public interface ZooKeeperClient extends Closeable {

    public HostConfiguration getMaster();

    public Collection<HostConfiguration> getSlaves();

    public void setNodeData( String hostName, Map<HostConfiguration,NodeState> nodeStates );

    public void setClusterData( ClusterStatus clusterStatus );

    public ClusterStatus getClusterData();

    public void addEventListeners( ZooKeeperEventListener ... listener );

    public void close();

    public void waitUntilLeader( long timeout, TimeUnit unit) throws InterruptedException;

    public boolean hasLeadership();

    public Map<String,Map<HostConfiguration,NodeState>> getNodeDatas();

}
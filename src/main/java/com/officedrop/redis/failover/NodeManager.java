package com.officedrop.redis.failover;

import com.officedrop.redis.failover.jedis.JedisClientFactory;
import com.officedrop.redis.failover.strategy.FailoverSelectionStrategy;
import com.officedrop.redis.failover.strategy.FailureDetectionStrategy;
import com.officedrop.redis.failover.utils.SleepUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 9:02 AM
 */
public class NodeManager implements NodeListener {

    private static final Logger log = LoggerFactory.getLogger(NodeManager.class);

    private final ZooKeeperClient zooKeeperClient;
    private final Collection<HostConfiguration> redisServers;
    private final JedisClientFactory factory;
    private final ExecutorService threadPool;
    private final List<Node> nodes = new CopyOnWriteArrayList<Node>();
    private final FailoverSelectionStrategy failoverStrategy;
    private final FailureDetectionStrategy failureDetectionStatery;
    private final long nodeSleepTimeout;
    private final int nodeRetries;
    private final String nodeName;
    private volatile boolean running;
    private volatile ClusterStatus lastClusterStatus;
    private volatile Map<String, Map<HostConfiguration, NodeState>> lastNodesData;
    private final Object mutex = new Object();
    private final List<NodeManagerListener> listeners = new CopyOnWriteArrayList<NodeManagerListener>();

    public NodeManager(
            ZooKeeperClient zooKeeperClient,
            Collection<HostConfiguration> redisServers,
            JedisClientFactory factory,
            ExecutorService threadPool,
            FailoverSelectionStrategy failoverStrategy,
            FailureDetectionStrategy failureDetectionStrategy,
            long nodeSleepTimeout,
            int nodeRetries
    ) {
        this.zooKeeperClient = zooKeeperClient;
        this.redisServers = redisServers;
        this.factory = factory;
        this.threadPool = threadPool;
        this.nodeSleepTimeout = nodeSleepTimeout;
        this.nodeRetries = nodeRetries;
        this.failoverStrategy = failoverStrategy;
        this.failureDetectionStatery = failureDetectionStrategy;

        String uniqueName = UUID.randomUUID().toString();

        try {
            String hostName = Inet4Address.getLocalHost().getHostName();
            uniqueName = hostName + "-" + uniqueName;
        } catch (Exception e) {
            log.error("Failed to get host name", e);
        }

        this.nodeName = uniqueName;

        this.zooKeeperClient.addEventListeners(new ZooKeeperEventListener() {
            @Override
            public void nodesDataChanged(final ZooKeeperClient client, final Map<String, Map<HostConfiguration, NodeState>> nodesData) {

                if (client.hasLeadership() ) {
                    NodeManager.this.threadPool.submit( new Runnable() {
                        @Override
                        public void run() {
                            nodeStatusesChanged(nodesData);
                        }
                    } );
                }

            }

            @Override
            public void clusterDataChanged(final ZooKeeperClient client, final ClusterStatus clusterStatus) {
                NodeManager.this.threadPool.submit( new Runnable() {
                    @Override
                    public void run() {
                        clusterStatusChanged(clusterStatus);
                    }
                } );
            }
        });

    }

    public void addListeners(NodeManagerListener... listeners) {
        this.listeners.addAll(Arrays.asList(listeners));
    }

    private void nodeStatusesChanged( Map<String, Map<HostConfiguration, NodeState>> nodesData ) {

        synchronized (this.mutex) {

            Collection<HostConfiguration> available = new ArrayList<HostConfiguration>();
            Collection<HostConfiguration> unavailable = new ArrayList<HostConfiguration>();

            Map<HostConfiguration,Collection<NodeState>> statusByNode = new HashMap<HostConfiguration, Collection<NodeState>>();

            for ( Map<HostConfiguration,NodeState> entry : nodesData.values() ) {
                for ( Map.Entry<HostConfiguration,NodeState> state : entry.entrySet() ) {
                    Collection<NodeState> nodeStates = statusByNode.get( state.getKey() );

                    if ( nodeStates == null ) {
                        nodeStates = new ArrayList<NodeState>();
                        statusByNode.put(state.getKey(), nodeStates);
                    }

                    nodeStates.add(state.getValue());
                }
            }

            for ( Map.Entry<HostConfiguration,Collection<NodeState>> nodeStates : statusByNode.entrySet() ) {
                boolean isAvailable = this.failureDetectionStatery.isAvailable( nodeStates.getKey(), nodeStates.getValue() );

                if ( !isAvailable ) {
                   unavailable.add(nodeStates.getKey());
                } else {
                    available.add(nodeStates.getKey());
                }

            }

            HostConfiguration newMaster = null;
            boolean slavesChanged = false;

            if ( this.lastClusterStatus.hasMaster() && unavailable.contains(this.lastClusterStatus.getMaster()) ) {
                newMaster = this.failoverStrategy.selectMaster( new HashSet<HostConfiguration>(available), nodesData );
                available.remove( newMaster );
            }

            if ( !this.lastClusterStatus.getSlaves().equals( available ) ) {
                slavesChanged = true;
            }

            if ( newMaster != null || slavesChanged ) {
                ClusterStatus status = new ClusterStatus( newMaster != null ? newMaster : this.lastClusterStatus.getMaster(), available, unavailable );
                this.fireClusterStatusChanged(status);
            }

        }

    }

    private void clusterStatusChanged( ClusterStatus clusterStatus ) {
        synchronized (this.mutex) {
            if (!this.zooKeeperClient.hasLeadership()) {
                this.fireClusterStatusChanged(clusterStatus);
            }
        }
    }

    private void fireClusterStatusChanged( ClusterStatus clusterStatus ) {
        if (NodeManager.this.lastClusterStatus != null) {
            switch (NodeManager.this.lastClusterStatus.difference(clusterStatus)) {
                case BOTH:
                    fireMasterChangedEvent(clusterStatus);
                    fireSlaveChangedEvent(clusterStatus);
                    break;
                case MASTER:
                    fireMasterChangedEvent(clusterStatus);
                    break;
                case SLAVES:
                    fireSlaveChangedEvent(clusterStatus);
                    break;
            }
        }

        NodeManager.this.lastClusterStatus = clusterStatus;
    }

    private void fireMasterChangedEvent(ClusterStatus status) {
        for (NodeManagerListener listener : this.listeners) {
            try {
                listener.masterChanged(this, status);
            } catch (Exception e) {
                log.error("Failed to send event to listener", e);
            }
        }
    }

    private void fireSlaveChangedEvent(ClusterStatus status) {
        for (NodeManagerListener listener : this.listeners) {
            try {
                listener.masterChanged(this, status);
            } catch (Exception e) {
                log.error("Failed to send event to listener", e);
            }
        }
    }

    public void start() {
        for (final HostConfiguration configuration : this.redisServers) {
            final Node node = new Node(configuration, this.factory, this.nodeSleepTimeout, this.nodeRetries);
            node.addNodeListeners(this);
            this.nodes.add(node);

            this.threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    node.start();
                }
            });
        }

        this.lastClusterStatus = this.zooKeeperClient.getClusterData();

        while (this.running) {
            try {

                if (!this.zooKeeperClient.hasLeadership()) {
                    this.zooKeeperClient.waitUntilLeader(5, TimeUnit.SECONDS);
                } else {
                    if (this.lastClusterStatus.isEmpty()) {
                        electMaster();
                    } else {
                        reconcile();
                    }
                }
            } catch ( NodeManagerException e ) {
                log.error("Failed to boot the NodeManager, giving up execution", e);
                throw new IllegalStateException(e);
            } catch (Exception e) {
                log.error("Wait for leader wait call raised an error", e);
            } finally {
                SleepUtils.safeSleep(5, TimeUnit.SECONDS);
            }
        }

    }

    private void electMaster() {
        Node master = null;

        for (Node node : this.nodes) {
            try {
                if (node.isMaster()) {
                    master = node;
                    break;
                }
            } catch ( Exception e ) {
                log.error("Failed to check if node is master", e);
            }
        }

        if ( master == null ) {
            throw new NodeManagerException( "No node is configured as master and I have no information on who was the previous master" );
        }

        for (Node node : this.nodes) {
            if (!node.equals(master)) {
                try {
                    node.makeSlaveOf(master.getHostConfiguration().getHost(), master.getHostConfiguration().getPort());
                } catch ( Exception e ) {
                    log.error("Failed to set node to be slave of current master");
                }
            }
        }

    }

    private void reconcile() {

        HostConfiguration configuration = this.lastClusterStatus.getMaster();

    }

    public void stop() {
        this.running = false;

        for (Node node : this.nodes) {
            node.stop();
        }

        this.zooKeeperClient.close();
    }

    @Override
    public void nodeIsOnline(final Node node, final long latency) {
        this.publishNodeState();
    }

    @Override
    public void nodeIsOffline(final Node node, final Exception e) {
        this.publishNodeState();
    }

    private void publishNodeState() {
        this.zooKeeperClient.setNodeData(this.nodeName, this.toNodeStates());
    }

    private Map<HostConfiguration, NodeState> toNodeStates() {

        Map<HostConfiguration, NodeState> states = new HashMap<HostConfiguration, NodeState>();

        for (Node node : this.nodes) {
            states.put(node.getHostConfiguration(), node.getCurrentState());
        }

        return states;
    }

}

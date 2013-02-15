package com.officedrop.redis.failover;

import com.officedrop.redis.failover.jedis.GenericJedisClientFactory;
import com.officedrop.redis.failover.jedis.JedisClientFactory;
import com.officedrop.redis.failover.strategy.FailoverSelectionStrategy;
import com.officedrop.redis.failover.strategy.FailureDetectionStrategy;
import com.officedrop.redis.failover.strategy.LatencyFailoverSelectionStrategy;
import com.officedrop.redis.failover.strategy.SimpleMajorityStrategy;
import com.officedrop.redis.failover.utils.DaemonThreadPoolFactory;
import com.officedrop.redis.failover.utils.Function;
import com.officedrop.redis.failover.utils.SleepUtils;
import com.officedrop.redis.failover.utils.TransformationUtils;
import com.officedrop.redis.failover.zookeeper.ZooKeeperNetworkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 9:02 AM
 */
public class NodeManager implements NodeListener, ClusterChangeEventSource {

    private static final Logger log = LoggerFactory.getLogger(NodeManager.class);

    private final ZooKeeperClient zooKeeperClient;
    private final Collection<HostConfiguration> redisServers;
    private final JedisClientFactory factory;
    private final ExecutorService threadPool;
    private final Set<Node> nodes = new HashSet<Node>();
    private final FailoverSelectionStrategy failoverStrategy;
    private final FailureDetectionStrategy failureDetectionStatery;
    private final long nodeSleepTimeout;
    private final int nodeRetries;
    private final String nodeName;
    private volatile boolean running;
    private volatile ClusterStatus lastClusterStatus;
    private volatile Map<HostConfiguration, NodeState> currentNodesState;
    private volatile Map<String, Map<HostConfiguration, NodeState>> lastNodesData;
    private final Object mutex = new Object();
    private final List<NodeManagerListener> listeners = new CopyOnWriteArrayList<NodeManagerListener>();
    private final Set<HostConfiguration> reportedNodes = Collections.synchronizedSet(new HashSet<HostConfiguration>());
    private final boolean closeZookeeper;

    public NodeManager(
            ZooKeeperClient zooKeeperClient,
            Collection<HostConfiguration> redisServers,
            JedisClientFactory factory,
            ExecutorService threadPool,
            FailoverSelectionStrategy failoverStrategy,
            FailureDetectionStrategy failureDetectionStrategy,
            long nodeSleepTimeout,
            int nodeRetries,
            boolean closeZooKeeper
    ) {
        this.zooKeeperClient = zooKeeperClient;
        this.redisServers = new HashSet<HostConfiguration>(redisServers);
        this.factory = factory;
        this.threadPool = threadPool;
        this.nodeSleepTimeout = nodeSleepTimeout;
        this.nodeRetries = nodeRetries;
        this.failoverStrategy = failoverStrategy;
        this.failureDetectionStatery = failureDetectionStrategy;
        this.closeZookeeper = closeZooKeeper;

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
            public void clusterDataChanged(final ZooKeeperClient client, final ClusterStatus clusterStatus) {
                if (NodeManager.this.running) {
                    NodeManager.this.threadPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            log.info("Cluster status has changed - {}", clusterStatus);
                            clusterStatusChanged(clusterStatus);
                        }
                    });
                }
            }
        });
    }

    public NodeManager(String zooKeeperUrl, Collection<HostConfiguration> redisServers) {
        this(
                new ZooKeeperNetworkClient(zooKeeperUrl),
                redisServers,
                GenericJedisClientFactory.INSTANCE,
                DaemonThreadPoolFactory.newCachedPool(),
                LatencyFailoverSelectionStrategy.INSTANCE,
                SimpleMajorityStrategy.INSTANCE,
                5000,
                3,
                true
        );
    }

    public ClusterStatus getLastClusterStatus() {
        return this.lastClusterStatus;
    }

    public void addListeners(NodeManagerListener... listeners) {
        this.listeners.addAll(Arrays.asList(listeners));
    }

    public void removeListeners(NodeManagerListener... listeners) {
        this.listeners.removeAll(Arrays.asList(listeners));
    }

    private void nodeStatusesChanged(Map<String, Map<HostConfiguration, NodeState>> nodesData) {

        synchronized (this.mutex) {

            if (this.lastNodesData == null || !nodesData.equals(this.lastNodesData)) {

                log.info("Nodes data has changed, checking if we have differences in the cluster \n{}\n{}", this.lastNodesData, nodesData);

                this.lastNodesData = nodesData;

                Collection<HostConfiguration> available = new ArrayList<HostConfiguration>();
                Collection<HostConfiguration> unavailable = new ArrayList<HostConfiguration>();

                Map<HostConfiguration, Collection<NodeState>> statusByNode = TransformationUtils.toStatusByNode(nodesData);

                for (Map.Entry<HostConfiguration, Collection<NodeState>> nodeStates : statusByNode.entrySet()) {
                    boolean isAvailable = this.failureDetectionStatery.isAvailable(nodeStates.getKey(), nodeStates.getValue());

                    if (!isAvailable) {
                        log.info("{} is not available", nodeStates.getKey());
                        unavailable.add(nodeStates.getKey());
                    } else {
                        log.info("{} is available", nodeStates.getKey());
                        available.add(nodeStates.getKey());
                    }
                }

                HostConfiguration newMaster = null;
                boolean slavesChanged = false;

                if (this.lastClusterStatus.hasMaster() && unavailable.contains(this.lastClusterStatus.getMaster())) {
                    newMaster = this.failoverStrategy.selectMaster(new HashSet<HostConfiguration>(available), nodesData);
                    available.remove(newMaster);
                } else {
                    available.remove(this.lastClusterStatus.getMaster());
                }

                if (!this.lastClusterStatus.getSlaves().equals(available)) {
                    slavesChanged = true;
                }

                if (newMaster != null || slavesChanged) {
                    ClusterStatus status = new ClusterStatus(newMaster != null ? newMaster : this.lastClusterStatus.getMaster(), available, unavailable);
                    this.fireClusterStatusChanged(status);
                }

            }


        }

    }

    private void clusterStatusChanged(ClusterStatus clusterStatus) {
        synchronized (this.mutex) {
            if (!this.zooKeeperClient.hasLeadership()) {
                this.fireClusterStatusChanged(clusterStatus);
            }
        }
    }

    private void fireClusterStatusChanged(ClusterStatus clusterStatus) {

        ClusterStatus previousClusterStatus = this.lastClusterStatus;
        this.lastClusterStatus = clusterStatus;

        if (previousClusterStatus != null) {
            switch (previousClusterStatus.difference(this.lastClusterStatus)) {
                case BOTH:
                    log.info("Both master and slaves changed");
                    fireMasterChangedEvent(clusterStatus);
                    fireSlaveChangedEvent(clusterStatus);
                    break;
                case MASTER:
                    log.info("Master changed");
                    fireMasterChangedEvent(clusterStatus);
                    break;
                case SLAVES:
                    log.info("Slaves changed");
                    fireSlaveChangedEvent(clusterStatus);
                    break;
            }
        } else {
            fireMasterChangedEvent(clusterStatus);
            fireSlaveChangedEvent(clusterStatus);
        }

        if (this.zooKeeperClient.hasLeadership()) {
            if (!clusterStatus.hasMaster()) {
                throw new IllegalArgumentException("There has to be a master before setting the cluster configuration");
            }

            this.zooKeeperClient.setClusterData(clusterStatus);
        }
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
                listener.slavesChanged(this, status);
            } catch (Exception e) {
                log.error("Failed to send event to listener", e);
            }
        }
    }

    public void start() {

        synchronized (this.mutex) {
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

            this.running = true;

            while (this.running && this.reportedNodes.size() != this.nodes.size()) {
                log.info("Waiting for all nodes to report {} ({}) - {} - {}", this.reportedNodes.size(), this.nodes.size(), this.nodes, this.reportedNodes);
                SleepUtils.safeSleep(this.nodeSleepTimeout, TimeUnit.MILLISECONDS);
            }

            log.info("All nodes reported, starting leader election loop - {}", TransformationUtils.toNodeStates(this.nodes));
        }

        this.threadPool.submit(new Runnable() {
            @Override
            public void run() {
                masterLoop();
            }
        });
    }

    private void masterLoop() {
        while (this.running) {
            try {

                if (!this.zooKeeperClient.hasLeadership()) {
                    log.info("Trying to acquire redis failover cluster leadership");
                    this.zooKeeperClient.waitUntilLeader(5, TimeUnit.SECONDS);
                } else {
                    log.info("Last cluster status is {}", this.lastClusterStatus);
                    synchronized (this.mutex) {
                        if (this.lastClusterStatus.isEmpty() || !this.lastClusterStatus.hasMaster()) {
                            electMaster();
                        } else {
                            reconcile();
                        }
                    }
                }
            } catch (NodeManagerException e) {
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

        log.info("Electing master on empty cluster configuration");

        Node master = null;

        for (Node node : this.nodes) {
            try {
                if (node.isMaster()) {
                    master = node;
                    break;
                }
            } catch (Exception e) {
                log.error("Failed to check if node is master", e);
            }
        }

        if (master == null) {
            throw new NodeManagerException("No node is configured as master and I have no information on who was the previous master");
        }

        log.info("Node {} is the master of the current cluster configuration", master.getHostConfiguration());

        List<HostConfiguration> slaves = new ArrayList<HostConfiguration>();
        List<HostConfiguration> unavailable = new ArrayList<HostConfiguration>();

        for (Node node : this.nodes) {
            if (!node.equals(master)) {
                try {
                    node.makeSlaveOf(master.getHostConfiguration().getHost(), master.getHostConfiguration().getPort());
                    slaves.add(node.getHostConfiguration());
                } catch (Exception e) {
                    unavailable.add(node.getHostConfiguration());
                    log.error("Failed to set node to be slave of current master");
                }
            }
        }

        ClusterStatus status = new ClusterStatus(master.getHostConfiguration(), slaves, unavailable);

        this.fireClusterStatusChanged(status);
    }

    private void reconcile() {
        log.info("Checking if we have to do a manual failover");
        handleManualFailover();

        log.info("Checking if node data has changed");
        nodeStatusesChanged(this.zooKeeperClient.getNodeDatas());

        log.info("Reconciling cluster configuration");
        this.assertMasterIsConfigured();
    }

    private void handleManualFailover() {
        HostConfiguration config = this.zooKeeperClient.getManualFailoverConfiguration();

        if ( config != null ) {
            log.info("Received new master from manual failover configuration {} - {} - {}",
                    config,
                    config.equals(this.lastClusterStatus.getMaster()),
                    this.reportedNodes.contains(config)
                    );
            if ( !config.equals(this.lastClusterStatus.getMaster()) && this.reportedNodes.contains(config) ) {
                Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
                slaves.add(this.lastClusterStatus.getMaster());
                slaves.addAll( this.lastClusterStatus.getSlaves() );
                slaves.remove(config);

                ClusterStatus status =  new ClusterStatus( config, slaves, this.lastClusterStatus.getUnavailables() );
                this.fireClusterStatusChanged(status);
            }
            this.zooKeeperClient.deleteManualFailoverConfiguration();
        } else {
            log.info("No manual failover configuration was available");
        }
    }

    public void assertMasterIsConfigured() {
        List<Node> availableNodes = new ArrayList<Node>();

        for (Node node : this.nodes) {
            if (!node.getCurrentState().isOffline()) {
                availableNodes.add(node);
            }
        }

        for (Node node : availableNodes) {
            if (this.lastClusterStatus.getMaster().equals(node.getHostConfiguration())) {
                try {
                    if (!node.isMaster()) {
                        node.becomeMaster();
                    }
                } catch (Exception e) {
                    log.error("Failed to mark current master as master", e);
                }
                break;
            }
        }

        for (Node node : availableNodes) {
            try {
                log.info("Node {} master is {}", node.getHostConfiguration(), node.getMasterConfiguration());
                if (this.lastClusterStatus.getSlaves().contains(node.getHostConfiguration())) {
                    if (node.getMasterConfiguration() == null || !node.getMasterConfiguration().equals(this.lastClusterStatus.getMaster())) {
                        node.makeSlaveOf(this.lastClusterStatus.getMaster().getHost(), this.lastClusterStatus.getMaster().getPort());
                    }
                }
            } catch (Exception e) {
                log.error("Failed to configure slave to be slave of current correct master", e);
            }

        }
    }

    public void stop() {

        log.warn("Stopping node manager {}", this);

        this.running = false;

        for (Node node : this.nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                log.error("Failed to stop this node", e);
            }
        }

        if ( this.closeZookeeper ) {
            this.zooKeeperClient.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        log.warn("Finalizing node manager {}", this);
        if (this.running) {
            this.stop();
        }
    }

    @Override
    public void nodeIsOnline(final Node node, final long latency) {
        this.publishNodeState();
        if (this.reportedNodes.size() != this.nodes.size()) {
            this.reportedNodes.add(node.getHostConfiguration());
        }
    }

    @Override
    public void nodeIsOffline(final Node node, final Exception e) {
        this.publishNodeState();
        if (this.reportedNodes.size() != this.nodes.size()) {
            this.reportedNodes.add(node.getHostConfiguration());
        }
    }

    private void publishNodeState() {

        Map<HostConfiguration, NodeState> states = TransformationUtils.toNodeStates(this.nodes);

        if (this.currentNodesState == null || !this.currentNodesState.equals( states )) {
            this.currentNodesState = states;
            this.zooKeeperClient.setNodeData(this.nodeName, this.currentNodesState);
        }
    }

    public void waitUntilMasterIsAvailable(long millis) {
        SleepUtils.waitUntil(millis, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                return NodeManager.this.getLastClusterStatus().hasMaster();
            }
        });
    }

}

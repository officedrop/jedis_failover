package com.officedrop.redis.failover.zookeeper;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.BackgroundPathable;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.recipes.leader.LeaderLatch;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.utils.EnsurePath;
import com.officedrop.redis.failover.*;
import com.officedrop.redis.failover.utils.JacksonJsonBinder;
import com.officedrop.redis.failover.utils.JsonBinder;
import com.officedrop.redis.failover.utils.PathUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * User: Maurício Linhares
 * Date: 12/31/12
 * Time: 5:53 PM
 */
public class ZooKeeperNetworkClient implements ZooKeeperClient {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperNetworkClient.class);

    public static final String BASE_PATH = "/redis_failover";
    public static final String NODE_STATES_PATH = PathUtils.toPath(BASE_PATH, "manager_node_state");
    public static final String LEADER_MUTEX = PathUtils.toPath(BASE_PATH, "leader");
    public static final String CLUSTER_PATH = PathUtils.toPath(BASE_PATH, "nodes");
    public static final String MANUAL_FAILOVER_PATH = PathUtils.toPath(BASE_PATH, "manual_failover");

    private final CuratorFramework curator;
    private final List<ZooKeeperEventListener> listeners = new CopyOnWriteArrayList<ZooKeeperEventListener>();
    private final JsonBinder jsonBinder = JacksonJsonBinder.BINDER;
    private final LeaderLatch leaderLatch;
    private volatile boolean closed = false;
    private final Timer timer;
    private volatile ClusterStatus lastClusterStatus;

    public ZooKeeperNetworkClient(String hosts) {

        try {
            int slashIndex;

            if ((slashIndex = hosts.indexOf('/')) != -1) {
                String namespace = hosts.substring(hosts.indexOf('/'), hosts.length());
                CuratorFramework namespaceCurator = CuratorFrameworkFactory
                        .builder()
                        .connectString(hosts.substring(0, slashIndex))
                        .retryPolicy(new RetryOneTime(1))
                        .build();
                namespaceCurator.start();

                EnsurePath namespaceEnsurePath = new EnsurePath(namespace);
                namespaceEnsurePath.ensure(namespaceCurator.getZookeeperClient());
                namespaceCurator.close();
            }

            this.curator = CuratorFrameworkFactory
                    .builder()
                    .connectString(hosts)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                    .build();
            this.curator.start();

            this.lastClusterStatus = new ClusterStatus(null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);

            EnsurePath ensurePath = new EnsurePath(NODE_STATES_PATH);
            ensurePath.ensure(this.curator.getZookeeperClient());

            ensurePath = new EnsurePath(CLUSTER_PATH);
            ensurePath.ensure(this.curator.getZookeeperClient());

            this.timer = new Timer(true);
            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    ClusterStatus clusterStatus = getClusterData();

                    if (clusterStatus != null && !clusterStatus.equals(ZooKeeperNetworkClient.this.lastClusterStatus)) {
                        ZooKeeperNetworkClient.this.lastClusterStatus = clusterStatus;
                        clusterStatusChanged();
                    }

                }
            }, 0, 5000);

            this.leaderLatch = new LeaderLatch(this.curator, LEADER_MUTEX, UUID.randomUUID().toString());
            this.leaderLatch.start();
        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }
    }

    public CuratorFramework getCurator() {
        return this.curator;
    }

    @Override
    public void waitUntilLeader(long timeout, TimeUnit unit) throws InterruptedException {
        this.leaderLatch.await(timeout, unit);
    }

    @Override
    public boolean hasLeadership() {
        return this.leaderLatch.hasLeadership();
    }

    @Override
    public void setNodeData(final String hostName, final Map<HostConfiguration, NodeState> nodeStates) {
        String path = PathUtils.toPath(NODE_STATES_PATH, hostName);
        this.createOrSet(path, this.jsonBinder.toBytes(nodeStates), CreateMode.EPHEMERAL);
    }

    @Override
    public void setClusterData(final ClusterStatus clusterStatus) {

        if (!clusterStatus.hasMaster()) {
            throw new IllegalArgumentException("You can't set a cluster status without a master");
        }

        this.createOrSet(CLUSTER_PATH, this.jsonBinder.toBytes(clusterStatus), CreateMode.PERSISTENT);
    }

    @Override
    public ClusterStatus getClusterData() {
        try {
            byte[] data = this.curator.getData().forPath(CLUSTER_PATH);

            if (data != null && data.length != 0) {
                return this.jsonBinder.toClusterStatus(data);
            } else {
                return new ClusterStatus(null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
            }
        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }
    }

    @Override
    public void addEventListeners(final ZooKeeperEventListener... listeners) {
        this.listeners.addAll(Arrays.asList(listeners));
    }

    public void close() {
        log.info("Closing ZookeeperNetworkClient");
        if (!this.closed) {
            this.closed = true;
            this.timer.cancel();
            this.close(this.leaderLatch, this.curator);
        }
    }

    private void close(Closeable... closables) {

        for (Closeable closeable : closables) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.error(String.format("Failed to close %s", closeable), e);
                throw new ZooKeeperException(e);
            }
        }

    }

    private void createOrSet(String path, byte[] data, CreateMode mode) {
        try {
            synchronized (this.curator) {
                if (this.curator.checkExists().forPath(path) != null) {
                    this.curator.setData().forPath(path, data);
                } else {
                    this.curator.create()
                            .creatingParentsIfNeeded()
                            .withMode(mode)
                            .forPath(path, data);
                }
            }

        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }
    }

    private void clusterStatusChanged() {
        for (ZooKeeperEventListener listener : this.listeners) {
            listener.clusterDataChanged(this, this.lastClusterStatus);
        }
    }

    @Override
    public HostConfiguration getManualFailoverConfiguration() {
        try {
            if (this.curator.checkExists().forPath(MANUAL_FAILOVER_PATH) != null) {
                byte[] data = this.curator.getData().forPath(MANUAL_FAILOVER_PATH);
                String host = new String(data, Charset.forName("UTF-8"));

                if (host.contains(":")) {
                    String[] hostData = host.split(":");
                    return new HostConfiguration(hostData[0], Integer.valueOf(hostData[1]));
                } else {
                    this.deleteManualFailoverConfiguration();
                }
            }
        } catch (NumberFormatException e) {
            this.deleteManualFailoverConfiguration();
        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }

        return null;
    }

    public void deleteManualFailoverConfiguration() {
        try {
            this.curator.delete().guaranteed().inBackground().forPath(MANUAL_FAILOVER_PATH);
        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }
    }

    @Override
    public Map<String, Map<HostConfiguration, NodeState>> getNodeDatas() {
        Map<String, Map<HostConfiguration, NodeState>> states = new HashMap<String, Map<HostConfiguration, NodeState>>();

        try {
            List<String> children = this.curator.getChildren().forPath(NODE_STATES_PATH);

            for (String path : children) {
                byte[] nodeData = this.curator.getData().forPath(PathUtils.toPath(NODE_STATES_PATH, path));

                if (nodeData != null && nodeData.length > 0) {
                    states.put(path, this.jsonBinder.toNodeState(nodeData));
                }
            }
        } catch (Exception e) {
            log.error("Error trying to access node datas", e);
            throw new ZooKeeperException(e);
        }

        return states;
    }

}

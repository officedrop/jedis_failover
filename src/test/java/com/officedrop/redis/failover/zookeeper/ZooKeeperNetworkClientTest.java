package com.officedrop.redis.failover.zookeeper;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingServer;
import com.officedrop.redis.failover.*;
import com.officedrop.redis.failover.utils.JacksonJsonBinder;
import com.officedrop.redis.failover.utils.JsonBinderTest;
import com.officedrop.redis.failover.utils.PathUtils;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Exchanger;

/**
 * User: Maurício Linhares
 * Date: 1/1/13
 * Time: 11:35 AM
 */
public class ZooKeeperNetworkClientTest {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperNetworkClientTest.class);

    @Test
    public void testClientCreation() throws Exception {

        TestingServer server = new TestingServer();

        String namespace = "/sample";

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString() + namespace);

        CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        curator.start();

        Assert.assertNotNull(curator.checkExists().forPath(PathUtils.toPath(namespace, ZooKeeperNetworkClient.BASE_PATH)));
        Assert.assertNotNull(curator.checkExists().forPath(PathUtils.toPath(namespace, ZooKeeperNetworkClient.NODE_STATES_PATH)));
        Assert.assertNotNull(curator.checkExists().forPath(PathUtils.toPath(namespace, ZooKeeperNetworkClient.CLUSTER_PATH)));

        curator.close();
        client.close();
        server.close();
    }

    @Test
    public void testSetNodeData() throws Exception {

        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString());

        Map<HostConfiguration, NodeState> states = new HashMap<HostConfiguration, NodeState>();

        states.put(JsonBinderTest.configuration7000, new NodeState(500));

        client.setNodeData("my-test-node", states);

        byte[] originalData = client.getCurator().getData().forPath(PathUtils.toPath(ZooKeeperNetworkClient.NODE_STATES_PATH, "my-test-node"));

        Map<HostConfiguration, NodeState> originalNodes = JacksonJsonBinder.BINDER.toNodeState(originalData);

        Assert.assertTrue(originalNodes.containsKey(JsonBinderTest.configuration7000));

        states.put(JsonBinderTest.configuration7001, NodeState.OFFLINE_STATE);

        client.setNodeData("my-test-node", states);

        byte[] currentData = client.getCurator().getData().forPath(PathUtils.toPath(ZooKeeperNetworkClient.NODE_STATES_PATH, "my-test-node"));

        Map<HostConfiguration, NodeState> currentNodes = JacksonJsonBinder.BINDER.toNodeState(currentData);

        Assert.assertTrue(currentNodes.containsKey(JsonBinderTest.configuration7000));
        Assert.assertTrue(currentNodes.containsKey(JsonBinderTest.configuration7001));

        client.close();

        server.close();
    }

    @Test
    public void testGetManualFailoverWhenThereIsNoConfig() throws Exception {

        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString());

        Assert.assertNull(client.getManualFailoverConfiguration());

        client.close();

        server.close();
    }

    @Test
    public void testManualFailioverWhenThereIsAConfig() throws Exception {

        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString());

        client.getCurator().create().forPath(ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH, "localhost:7000".getBytes("UTF-8"));

        Thread.sleep(1000);

        Assert.assertEquals(new HostConfiguration("localhost", 7000), client.getManualFailoverConfiguration());

        client.close();

        server.close();
    }

    @Test
    public void testDeleteManualFailoverConfigWhenThereIsOne() throws Exception {

        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString());

        client.getCurator().create().forPath(ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH, "localhost:7000".getBytes("UTF-8"));

        client.deleteManualFailoverConfiguration();

        Thread.sleep(1000);

        Assert.assertNull(client.getCurator().checkExists().forPath(ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH));

        client.close();

        server.close();
    }

    @Test
    public void testDeleteManualFailoverConfigWhenThereIsNotOne() throws Exception {

        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString());

        client.deleteManualFailoverConfiguration();

        client.close();

        server.close();
    }

    @Test
    public void testLoadFailoverConfigurationWithBadData() throws Exception {
        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString());

        client.getCurator().create().forPath(ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH, "localhost".getBytes("UTF-8"));

        Thread.sleep(1000);

        Assert.assertNull(client.getManualFailoverConfiguration());

        Thread.sleep(1000);

        Assert.assertNull(client.getCurator().checkExists().forPath( ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH ));

        client.close();

        server.close();
    }

    @Test
    public void testLoadFailoverConfigurationWithBadPort() throws Exception {
        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString());

        client.getCurator().create().forPath(ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH, "localhost:port_here".getBytes("UTF-8"));

        Thread.sleep(1000);

        Assert.assertNull(client.getManualFailoverConfiguration());

        Thread.sleep(1000);

        Assert.assertNull(client.getCurator().checkExists().forPath( ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH ));

        client.close();

        server.close();
    }

}

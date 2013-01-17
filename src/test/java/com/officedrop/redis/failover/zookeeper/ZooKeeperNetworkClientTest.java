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

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient( server.getConnectString() + namespace );

        CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString( server.getConnectString() )
                .retryPolicy(new RetryOneTime(1))
                .build();
        curator.start();

        Assert.assertNotNull( curator.checkExists().forPath( PathUtils.toPath( namespace, ZooKeeperNetworkClient.BASE_PATH ) ) );
        Assert.assertNotNull( curator.checkExists().forPath( PathUtils.toPath( namespace, ZooKeeperNetworkClient.NODE_STATES_PATH ) ) );
        Assert.assertNotNull( curator.checkExists().forPath( PathUtils.toPath( namespace, ZooKeeperNetworkClient.CLUSTER_PATH ) ) );

        curator.close();
        client.close();
        server.close();
    }

    @Test
    public void testSetNodeData() throws Exception {

        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient( server.getConnectString() );

        Map<HostConfiguration,NodeState> states = new HashMap<HostConfiguration, NodeState>();

        states.put(JsonBinderTest.configuration7000, new NodeState(500, false));

        client.setNodeData("my-test-node", states);

        byte[] originalData = client.getCurator().getData().forPath(PathUtils.toPath( ZooKeeperNetworkClient.BASE_PATH, ZooKeeperNetworkClient.NODE_STATES, "my-test-node" ));

        Map<HostConfiguration,NodeState> originalNodes = JacksonJsonBinder.BINDER.toNodeState(originalData);

        Assert.assertTrue(originalNodes.containsKey(JsonBinderTest.configuration7000));

        states.put(JsonBinderTest.configuration7001, new NodeState());

        client.setNodeData("my-test-node", states);

        byte[] currentData = client.getCurator().getData().forPath(PathUtils.toPath( ZooKeeperNetworkClient.BASE_PATH, ZooKeeperNetworkClient.NODE_STATES, "my-test-node" ));

        Map<HostConfiguration,NodeState> currentNodes = JacksonJsonBinder.BINDER.toNodeState(currentData);

        Assert.assertTrue(currentNodes.containsKey(JsonBinderTest.configuration7000));
        Assert.assertTrue(currentNodes.containsKey(JsonBinderTest.configuration7001));

        client.close();

        server.close();
    }

}

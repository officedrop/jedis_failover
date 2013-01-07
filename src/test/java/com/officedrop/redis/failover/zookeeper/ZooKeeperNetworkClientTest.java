package com.officedrop.redis.failover.zookeeper;

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

        server.close();
    }

    @Test
    public void testRaiseEventWhenNodeDataIsChanged() throws Exception {

        TestingServer server = new TestingServer();

        ZooKeeperNetworkClient client = new ZooKeeperNetworkClient(server.getConnectString());

        final Exchanger<Boolean> exchanger = new Exchanger<Boolean>();

        client.addEventListeners(new ZooKeeperEventListener() {
            @Override
            public void nodesDataChanged(final ZooKeeperClient client, final Map<String, Map<HostConfiguration, NodeState>> nodesData) {
                try {

                    log.debug("Received event with data {}", nodesData);

                    exchanger.exchange(true);
                } catch ( Exception e ) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public void clusterDataChanged(final ZooKeeperClient client, final ClusterStatus clusterStatus) {
            }
        });

        Map<HostConfiguration,NodeState> states = new HashMap<HostConfiguration, NodeState>();
        states.put( JsonBinderTest.configuration7000, new NodeState(500) );
        states.put( JsonBinderTest.configuration7001, new NodeState(900) );
        states.put( JsonBinderTest.configuration7002, new NodeState() );

        client.setNodeData("some-node", states);

        Assert.assertTrue(exchanger.exchange(true));

        Assert.assertEquals(states, client.getNodeDatas().get("some-node"));

        states.put(JsonBinderTest.configuration7003, new NodeState());

        client.setNodeData("some-other-node", states);

        Assert.assertTrue(exchanger.exchange(true));

        Assert.assertEquals( states, client.getNodeDatas().get( "some-other-node" ) );

        server.stop();
    }

}

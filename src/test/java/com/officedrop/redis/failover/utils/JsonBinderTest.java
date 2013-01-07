package com.officedrop.redis.failover.utils;

import com.officedrop.redis.failover.ClusterStatus;
import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 4:28 PM
 */
public class JsonBinderTest {

    public static final HostConfiguration configuration7000 = new HostConfiguration("localhost", 7000);
    public static final HostConfiguration configuration7001 = new HostConfiguration("localhost", 7001);
    public static final HostConfiguration configuration7002 = new HostConfiguration("localhost", 7002);
    public static final HostConfiguration configuration7003 = new HostConfiguration("localhost", 7003);

    private static final String MASTER_DATA = "{\"master\":\"localhost:7000\",\"slaves\":[\"localhost:7001\",\"localhost:7002\"],\"unavailable\":[ \"localhost:7003\" ]}";
    private static final String WITHOUT_MASTER_DATA = "{\"slaves\":[\"localhost:7001\",\"localhost:7002\"],\"unavailable\":[ \"localhost:7003\" ]}";


    JsonBinder binder = new JacksonJsonBinder();

    @Test
    public void testParseJson() throws Exception {

        byte[] data = IOUtils.toByteArray(this.getClass().getClassLoader().getResource("sample_node_data.json"));

        Map<HostConfiguration,NodeState> nodes = binder.toNodeState(data);

        Assert.assertEquals(500, nodes.get(configuration7000).getLatency());
        Assert.assertFalse(nodes.get(configuration7000).isOffline());

        Assert.assertEquals(600, nodes.get(configuration7001).getLatency());
        Assert.assertFalse(nodes.get(configuration7001).isOffline());

        Assert.assertEquals(-1, nodes.get(configuration7002).getLatency());
        Assert.assertTrue(nodes.get(configuration7002).isOffline());
    }

    @Test
    public void testGenerateJson() throws Exception {

        Map<HostConfiguration,NodeState> nodes = new HashMap<HostConfiguration, NodeState>();

        nodes.put(configuration7000, new NodeState(500, false));
        nodes.put(configuration7001, new NodeState(600, false));
        nodes.put(configuration7002, new NodeState());

        byte[] data = binder.toBytes(nodes);

        String file = IOUtils.toString(this.getClass().getClassLoader().getResource("sample_node_data.json"));

        Assert.assertEquals( file, new String(data) );
    }

    @Test
    public void testParseMasterData() {

        ClusterStatus status =  binder.toClusterStatus(MASTER_DATA.getBytes());

        Assert.assertEquals( configuration7000, status.getMaster() );

        Assert.assertEquals( 2, status.getSlaves().size() );
        Assert.assertTrue( status.getSlaves().contains(configuration7001) );
        Assert.assertTrue( status.getSlaves().contains(configuration7002) );

        Assert.assertEquals( 1, status.getUnavailables().size() );
        Assert.assertTrue( status.getUnavailables().contains(configuration7003) );
    }

    @Test
    public void testWithoutMaster() {
        ClusterStatus status = binder.toClusterStatus(WITHOUT_MASTER_DATA.getBytes());

        Assert.assertNull( status.getMaster() );

        Assert.assertEquals( 2, status.getSlaves().size() );
        Assert.assertTrue( status.getSlaves().contains(configuration7001) );
        Assert.assertTrue( status.getSlaves().contains(configuration7002) );

        Assert.assertEquals( 1, status.getUnavailables().size() );
        Assert.assertTrue( status.getUnavailables().contains(configuration7003) );
    }

    @Test
    public void testGenerateJsonData() {
        ClusterStatus  status = new ClusterStatus(
                configuration7000,
                Arrays.asList( configuration7001, configuration7002 ),
                Arrays.asList(configuration7003));

        Assert.assertEquals(
                "{\"unavailable\":[\"localhost:7003\"],\"slaves\":[\"localhost:7001\",\"localhost:7002\"],\"master\":\"localhost:7000\"}",
                new String( binder.toBytes(status) ) );
    }

}

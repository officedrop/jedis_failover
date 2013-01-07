package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;

import static com.officedrop.redis.failover.utils.JsonBinderTest.*;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 5:19 PM
 */
public class LatencyFailoverSelectionStrategyTest {

    LatencyFailoverSelectionStrategy strategy = new LatencyFailoverSelectionStrategy();

    @Test
    public void testSelectLowestLatencyServer() {

        Set<HostConfiguration> hosts = new HashSet<HostConfiguration>(
                Arrays.asList(
                        configuration7000,
                        configuration7001,
                        configuration7002
                ) );

        Map<String, Map<HostConfiguration, NodeState>> nodeReports = new HashMap<String, Map<HostConfiguration, NodeState>>();

        Map<HostConfiguration,NodeState> state1 = new HashMap<HostConfiguration, NodeState>();
        state1.put( configuration7000, new NodeState(400) );
        state1.put( configuration7001, new NodeState(600) );
        state1.put( configuration7002, new NodeState(200) );

        Map<HostConfiguration,NodeState> state2 = new HashMap<HostConfiguration, NodeState>();
        state2.put( configuration7000, new NodeState(600) );
        state2.put( configuration7001, new NodeState(400) );
        state2.put( configuration7002, new NodeState(300) );

        Map<HostConfiguration,NodeState> state3 = new HashMap<HostConfiguration, NodeState>();
        state3.put( configuration7000, new NodeState(500) );
        state3.put( configuration7001, new NodeState(500) );
        state3.put( configuration7002, new NodeState(600) );


        nodeReports.put("state-1", state1);
        nodeReports.put("state-2", state2);
        nodeReports.put("state-3", state3);

        HostConfiguration result = strategy.selectMaster(hosts, nodeReports);

        Assert.assertEquals( configuration7002, result );

    }

}

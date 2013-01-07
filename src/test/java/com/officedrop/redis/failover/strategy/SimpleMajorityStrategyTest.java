package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 5:03 PM
 */
public class SimpleMajorityStrategyTest {

    SimpleMajorityStrategy strategy = new SimpleMajorityStrategy();
    HostConfiguration configuration = new HostConfiguration("localhost", 1);

    @Test
    public void testSimpleMajorityUnavailableDecision() {

        boolean result = strategy.isAvailable(configuration,
                Arrays.asList(
                        new NodeState(400),
                        new NodeState(500),
                        new NodeState(),
                        new NodeState(),
                        new NodeState()
                )
        );

        Assert.assertFalse(result);

    }

    @Test
    public void testSimpleMajorityAvailableDecision() {

        boolean result = strategy.isAvailable(configuration,
                Arrays.asList(
                        new NodeState(400),
                        new NodeState(500),
                        new NodeState(200),
                        new NodeState(),
                        new NodeState()
                )
        );

        Assert.assertTrue(result);
    }

    @Test
    public void testSimpleMajorityAvailableDecisionWithEvenItems() {

        boolean result = strategy.isAvailable(configuration,
                Arrays.asList(
                        new NodeState(400),
                        new NodeState(500),
                        new NodeState(500),
                        new NodeState()
                )
        );

        Assert.assertTrue(result);
    }

    @Test
    public void testSimpleMajorityUnavailableDecisionWithEvenItems() {

        boolean result = strategy.isAvailable(configuration,
                Arrays.asList(
                        new NodeState(400),
                        new NodeState(500),
                        new NodeState(),
                        new NodeState()
                )
        );

        Assert.assertFalse(result);
    }

    @Test
    public void testSimpleMajorityUnavailableDecisionWithTwo() {
        boolean result = strategy.isAvailable(configuration,
                Arrays.asList(
                        new NodeState(400),
                        new NodeState()
                )
        );

        Assert.assertFalse(result);
    }

}

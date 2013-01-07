package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;

import java.util.Collection;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 5:00 PM
 */
public interface FailureDetectionStrategy {

    public boolean isAvailable(HostConfiguration configuration, Collection<NodeState> states);

}

package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;

import java.util.Map;
import java.util.Set;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 5:16 PM
 */
public interface FailoverSelectionStrategy {

    public HostConfiguration selectMaster( Set<HostConfiguration> hosts, final Map<String, Map<HostConfiguration, NodeState>> nodeReports);

}

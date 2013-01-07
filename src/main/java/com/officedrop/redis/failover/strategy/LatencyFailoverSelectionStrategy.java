package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;

import java.util.*;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 5:18 PM
 */
public class LatencyFailoverSelectionStrategy implements FailoverSelectionStrategy {

    @Override
    public HostConfiguration selectMaster(Set<HostConfiguration> hosts, final Map<String, Map<HostConfiguration, NodeState>> nodeReports) {

        Map<HostConfiguration, HostLatency> measurements = new HashMap<HostConfiguration, HostLatency>();

        for (HostConfiguration host : hosts) {

            HostLatency latency = new HostLatency(host);

            measurements.put(host, latency);

            for (Map.Entry<String, Map<HostConfiguration, NodeState>> entry : nodeReports.entrySet()) {

                NodeState state = entry.getValue().get( host );

                if ( !state.isOffline() ) {
                    latency.addLatency( state.getLatency() );
                }

            }
        }

        SortedSet<HostLatency> sortedLatencies = new TreeSet<HostLatency>(measurements.values());

        return sortedLatencies.first().getHostConfiguration();
    }

}
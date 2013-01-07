package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
* User: Maurício Linhares
* Date: 1/5/13
* Time: 5:33 PM
*/
class HostLatency implements Comparable<HostLatency> {

    private final HostConfiguration host;
    private final List<Long> measuredLatencies = new ArrayList<Long>();

    public HostLatency(HostConfiguration host) {
        this.host = host;
    }

    @Override
    public int compareTo(final HostLatency o) {
        return this.getAverageLatency().compareTo(o.getAverageLatency());
    }

    public void addLatency(long latency) {
        this.measuredLatencies.add(latency);
    }

    public HostConfiguration getHostConfiguration() {
        return this.host;
    }

    public Long getAverageLatency() {

        long sum = 0;

        for (long latency : this.measuredLatencies) {
            sum += latency;
        }

        return sum / this.measuredLatencies.size();
    }

}

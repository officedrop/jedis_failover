package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;

import java.util.Collection;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 5:02 PM
 */
public class SimpleMajorityStrategy implements FailureDetectionStrategy {

    public static final SimpleMajorityStrategy INSTANCE = new SimpleMajorityStrategy();

    @Override
    public boolean isAvailable(final HostConfiguration configuration, final Collection<NodeState> states) {

        int count = 0;

        for ( NodeState state : states ) {
            if ( state.isOffline() ) {
                count++;
            }
        }

        return !(count >= decideMajority(states.size()));
    }

    private int decideMajority( int size ) {
        if ( (size % 2) == 0 ) {
            return size / 2;
        } else {
            return (size / 2) + 1;
        }
    }

}

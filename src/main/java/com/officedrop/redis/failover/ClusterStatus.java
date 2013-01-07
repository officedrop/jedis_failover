package com.officedrop.redis.failover;

import java.util.*;

/**
 * User: Maurício Linhares
 * Date: 1/4/13
 * Time: 8:46 AM
 */
public class ClusterStatus {

    private final HostConfiguration master;
    private final Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
    private final Set<HostConfiguration> unavailables = new HashSet<HostConfiguration>();

    public ClusterStatus( HostConfiguration master, Collection<HostConfiguration> slaves, Collection<HostConfiguration> unavailables) {
        this.master = master;
        this.slaves.addAll(slaves);
        this.unavailables.addAll(unavailables);
    }

    public HostConfiguration getMaster() {
        return master;
    }

    public Set<HostConfiguration> getSlaves() {
        return slaves;
    }

    public Set<HostConfiguration> getUnavailables() {
        return unavailables;
    }

    public ClusterStatusDifference difference( ClusterStatus status ) {

        boolean masterChanged = false;
        boolean slavesChanged = !this.slaves.equals( status.slaves );

        if ( this.master != null && status.master != null ) {
            masterChanged = !this.master.equals(status.master);
        }

        if ( masterChanged && slavesChanged ) {
            return ClusterStatusDifference.BOTH;
        } else {
            if ( masterChanged ) {
                return ClusterStatusDifference.MASTER;
            } else {
                if ( slavesChanged ) {
                    return ClusterStatusDifference.SLAVES;
                } else {
                    return ClusterStatusDifference.NO_DIFFERENCE;
                }
            }
        }
    }

    public boolean isEmpty() {
        return this.master == null && this.slaves.isEmpty() && this.unavailables.isEmpty();
    }

    public boolean hasMaster() {
        return this.master != null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterStatus)) return false;

        ClusterStatus that = (ClusterStatus) o;

        if (master != null ? !master.equals(that.master) : that.master != null) return false;
        if (!slaves.equals(that.slaves)) return false;
        if (!unavailables.equals(that.unavailables)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = master != null ? master.hashCode() : 0;
        result = 31 * result + slaves.hashCode();
        result = 31 * result + unavailables.hashCode();
        return result;
    }
}

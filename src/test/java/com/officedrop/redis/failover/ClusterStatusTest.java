package com.officedrop.redis.failover;

import com.officedrop.redis.failover.utils.JsonBinderTest;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 7:08 PM
 */
public class ClusterStatusTest {

    @Test
    public void testClusterStatusNoDifference() {
        HostConfiguration master = JsonBinderTest.configuration7000;
        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>(Arrays.asList( JsonBinderTest.configuration7001, JsonBinderTest.configuration7002 ));

        ClusterStatus status = new ClusterStatus(master, slaves, Collections.EMPTY_LIST);

        Assert.assertEquals( ClusterStatusDifference.NO_DIFFERENCE, status.difference(status) );
    }

    @Test
    public void testClusterStatusSlavesDifference() {
        HostConfiguration master = JsonBinderTest.configuration7000;
        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>(Arrays.asList( JsonBinderTest.configuration7001, JsonBinderTest.configuration7002 ));

        ClusterStatus status = new ClusterStatus(master, slaves, Collections.EMPTY_LIST);

        slaves.remove( JsonBinderTest.configuration7002 );

        ClusterStatus otherStatus = new ClusterStatus(master, slaves, Collections.EMPTY_LIST);

        Assert.assertEquals( ClusterStatusDifference.SLAVES, status.difference(otherStatus) );
    }

    @Test
    public void testClusterStatusMasterDifference() {
        HostConfiguration master = JsonBinderTest.configuration7000;
        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>(Arrays.asList( JsonBinderTest.configuration7001, JsonBinderTest.configuration7002 ));

        ClusterStatus status = new ClusterStatus(master, slaves, Collections.EMPTY_LIST);

        ClusterStatus otherStatus = new ClusterStatus(JsonBinderTest.configuration7003, slaves, Collections.EMPTY_LIST);

        Assert.assertEquals( ClusterStatusDifference.MASTER, status.difference(otherStatus) );
    }

    @Test
    public void testClusterStatusBothDifference() {
        HostConfiguration master = JsonBinderTest.configuration7000;
        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>(Arrays.asList( JsonBinderTest.configuration7001, JsonBinderTest.configuration7002 ));

        ClusterStatus status = new ClusterStatus(master, slaves, Collections.EMPTY_LIST);

        slaves.add( JsonBinderTest.configuration7000 );

        ClusterStatus otherStatus = new ClusterStatus(JsonBinderTest.configuration7003, slaves, Collections.EMPTY_LIST);

        Assert.assertEquals( ClusterStatusDifference.BOTH, status.difference(otherStatus) );
    }

}

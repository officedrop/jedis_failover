package com.officedrop.redis.failover;

import com.officedrop.redis.failover.jedis.JedisClient;
import com.officedrop.redis.failover.jedis.JedisClientFactory;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * User: Maurício Linhares
 * Date: 12/21/12
 * Time: 11:26 AM
 */
public class ClientTest {

    @Test
    public void testCreateClient() throws Exception {

        ClusterChangeEventSource nodeManager = mock(ClusterChangeEventSource.class );

        JedisClient masterClient = mock(JedisClient.class);
        JedisClient slaveClient1 = mock(JedisClient.class);
        JedisClient slaveClient2 = mock(JedisClient.class);

        JedisClientFactory factory = mock(JedisClientFactory.class);

        HostConfiguration master = new HostConfiguration("localhost", 6000);
        List<HostConfiguration> slaves = Arrays.asList( new HostConfiguration("localhost", 6001), new HostConfiguration("localhost", 6002) );

        ClusterStatus status = new ClusterStatus( master, slaves, Collections.EMPTY_LIST );

        when(nodeManager.getLastClusterStatus()).thenReturn(status);

        when( factory.create(master) ).thenReturn(masterClient);
        when( factory.create(slaves.get(0)) ).thenReturn(slaveClient1);
        when( factory.create(slaves.get(1)) ).thenReturn(slaveClient2);

        when( masterClient.set("some-key", "some-value") ).thenReturn("some-value");
        when( slaveClient1.get("some-key") ).thenReturn("some-value");
        when( slaveClient2.get("some-other-key")).thenReturn("some-other-value");

        Client client = new Client(nodeManager, factory);

        client.set("some-key", "some-value");

        String value = client.get("some-key");

        String otherValue = client.get("some-other-key");

        verify( masterClient ).set("some-key", "some-value");
        verify(slaveClient1).get("some-key");
        verify(slaveClient2).get("some-other-key");

        Assert.assertEquals( "some-value", value );
        Assert.assertEquals( "some-other-value", otherValue );

    }

}

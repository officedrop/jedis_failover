package com.officedrop.redis.failover.jedis;

import com.officedrop.redis.failover.HostConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: mauricio
 * Date: 1/15/13
 * Time: 6:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class JedisPoolBuilderTest {

    @Test
    @Ignore
    public void testsrem() {

        JedisPool pool = new JedisPoolBuilder()
                .withFailoverConfiguration("localhost:2181", Arrays.asList(new HostConfiguration("localhost", 7000), new HostConfiguration("localhost", 7001)))
                .build();

        pool.withJedis(new JedisFunction() {
            @Override
            public void execute(JedisActions jedis) throws Exception {
                Long result = jedis.sadd("key", "otherkey");

                System.out.println("result is " + result);

                result = jedis.srem("key", "otherkey");

                System.out.println("result is " + result);
            }
        });

    }

}

package com.officedrop.redis.failover;

import com.officedrop.redis.failover.jedis.GenericJedisClientFactory;
import com.officedrop.redis.failover.redis.RedisServer;
import com.officedrop.redis.failover.utils.Action1;
import com.officedrop.redis.failover.utils.ThreadPool;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 9:23 AM
 */
public class NodeTest {

    private static final Logger log = LoggerFactory.getLogger(NodeTest.class);

    @Test
    public void testPingNode() {

        RedisServer.withServer( new Action1<RedisServer>() {
            @Override
            public void apply( RedisServer server) {

                final Node node = new Node(server.getHostConfiguration(), GenericJedisClientFactory.INSTANCE, 500, 3);

                final Exchanger<Boolean> exchanger = new Exchanger<Boolean>();

                node.addNodeListeners(new NodeListener() {
                    @Override
                    public void nodeIsOnline(final Node node, final long latency) {
                        try {
                            exchanger.exchange(Boolean.TRUE);
                        } catch ( Exception e ) {
                            log.error("Failed to exchange object", e);
                        }
                    }

                    @Override
                    public void nodeIsOffline(final Node node, final Exception e) {
                        log.error("Failed to connect to node", e);
                        try {
                            exchanger.exchange(Boolean.FALSE);
                        } catch ( Exception exception ) {
                            log.error("Failed to exchange object", e);
                        }
                    }
                });

                ThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        node.start();
                    }
                });

                try {
                    Assert.assertTrue( exchanger.exchange(Boolean.TRUE, 5, TimeUnit.SECONDS) );
                } catch ( Exception e ) {
                    throw new IllegalStateException(e);
                }

                Assert.assertTrue(node.isMaster());

                node.stop();

            }
        });

    }

    @Test
    public void testFailedNode() {

        RedisServer.withServer(new Action1<RedisServer>() {
            @Override
            public void apply(RedisServer server) {

                server.setAlwaysTimeout(true);

                final Node node = new Node(server.getHostConfiguration(), GenericJedisClientFactory.INSTANCE, 500, 3);

                final Exchanger<Boolean> exchanger = new Exchanger<Boolean>();

                node.addNodeListeners(new NodeListener() {
                    @Override
                    public void nodeIsOnline(final Node node, final long latency) {

                        log.debug("Node is online");

                        try {
                            exchanger.exchange(Boolean.FALSE);
                        } catch ( Exception e ) {
                            log.error("Failed to exchange object", e);
                        }
                    }

                    @Override
                    public void nodeIsOffline(final Node node, final Exception e) {
                        log.error("Failed to connect to node", e);
                        try {
                            exchanger.exchange(Boolean.TRUE);
                        } catch ( Exception exception ) {
                            log.error("Failed to exchange object", e);
                        }
                    }
                });

                ThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        node.start();
                    }
                });

                try {
                    Assert.assertTrue( exchanger.exchange(Boolean.TRUE, 10, TimeUnit.SECONDS) );
                } catch ( Exception e ) {
                    throw new IllegalStateException(e);
                } finally {
                    node.stop();
                }

            }
        });

    }

}

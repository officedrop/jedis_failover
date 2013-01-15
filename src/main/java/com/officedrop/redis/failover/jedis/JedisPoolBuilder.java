package com.officedrop.redis.failover.jedis;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeManager;
import com.officedrop.redis.failover.utils.Action1;
import org.apache.commons.pool.impl.GenericObjectPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collection;

/**
 * User: Maurício Linhares
 * Date: 1/14/13
 * Time: 9:11 AM
 */
public class JedisPoolBuilder {

    private JedisFactory jedisFactory;
    private JedisPoolConfig poolConfig = new JedisPoolConfig();
    private Action1<CommonsJedisPool> onCloseAction;

    public JedisPoolBuilder() {
        this.poolConfig.setMaxIdle(1);
        this.poolConfig.setTestWhileIdle(true);
        this.poolConfig.setTestOnBorrow(true);
        this.poolConfig.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_GROW);
    }

    public JedisPoolBuilder withFailoverConfiguration( String zooKeeperHosts, Collection<HostConfiguration> redisServers) {

        final NodeManager nodeManager = new NodeManager(zooKeeperHosts, redisServers);
        nodeManager.start();

        try {
            nodeManager.waitUntilMasterIsAvailable(10000);
        } catch ( Exception e ) {
            nodeManager.stop();
            throw new IllegalStateException("Node manager could not be started", e);
        }

        this.onCloseAction = new Action1<CommonsJedisPool>() {
            @Override
            public void apply( CommonsJedisPool pool ) {
                nodeManager.stop();
            }
        };

        this.jedisFactory = new FailoverJedisFactory(nodeManager);

        return this;
    }

    public JedisPoolBuilder withHost( HostConfiguration configuration ) {

        this.jedisFactory = new CommonJedisFactory(configuration);

        return this;
    }

    public JedisPoolBuilder withHost( String host, int port ) {
        return this.withHost(new HostConfiguration(host, port));
    }

    public JedisPoolBuilder withPoolConfiguration( JedisPoolConfig config ) {
        this.poolConfig = config;

        return this;
    }

    public JedisPoolBuilder withNamespace( String namespace ) {

        if ( this.jedisFactory == null ) {
            throw new NullPointerException("You must set a valid jedis factory before setting the namespace");
        }

        this.jedisFactory = new NamespacedJedisFactory( this.jedisFactory, namespace );

        return this;
    }

    public JedisPool build() {

        CommonsJedisPool pool = new CommonsJedisPool(this.jedisFactory, this.poolConfig);

        if ( this.onCloseAction != null ) {
            pool.addListeners(this.onCloseAction);
        }

        return pool;
    }

}

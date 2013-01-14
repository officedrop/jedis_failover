package com.officedrop.redis.failover.jedis;

/**
 * User: Maurício Linhares
 * Date: 1/14/13
 * Time: 8:28 AM
 */
public class NamespacedJedisFactory implements JedisFactory {

    private final JedisFactory factory;
    private final String namespace;

    public NamespacedJedisFactory( JedisFactory factory, String namespace ) {
        this.factory = factory;
        this.namespace = namespace;
    }

    @Override
    public JedisActions create() {
        return new NamespacedJedisActions( this.namespace, this.factory.create() );
    }
}

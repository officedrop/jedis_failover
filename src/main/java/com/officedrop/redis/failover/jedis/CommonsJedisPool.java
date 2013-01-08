package com.officedrop.redis.failover.jedis;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:37 PM
 */
public class CommonsJedisPool implements PoolableObjectFactory, JedisPool {

    private static final Logger log = LoggerFactory.getLogger(CommonsJedisPool.class);

    private final JedisFactory factory;
    private final GenericObjectPool pool = new GenericObjectPool(this);

    public CommonsJedisPool(JedisFactory factory) {
        this.factory = factory;
    }

    @Override
    public void withJedis(final JedisFunction action) {
        this.withJedis(new JedisResultFunction<Object>() {
            @Override
            public Object execute(final JedisActions jedis) throws Exception {
                action.execute(jedis);
                return null;
            }
        });
    }

    @Override
    public <T> T withJedis(JedisResultFunction<T> action) {

        JedisActions jedis = null;

        try {
            jedis = (JedisActions) this.pool.borrowObject();
            return action.execute( jedis );
        } catch ( Exception e ) {
            throw new RuntimeException(e);
        } finally {
            try {
                this.pool.returnObject(jedis);
            } catch ( Exception e ) {
                log.error("Failed to return object to pool", e);
            }
        }
    }

    @Override
    public Object makeObject() throws Exception {
        return this.factory.create();
    }

    @Override
    public void destroyObject(final Object obj) throws Exception {
        JedisActions actions = (JedisActions) obj;
        actions.quit();
    }

    @Override
    public boolean validateObject(final Object obj) {
        try {
            JedisActions actions = (JedisActions) obj;
            actions.ping();
            return true;
        } catch ( Exception e ) {
            log.error("Failed to create validate pooled object", e);
            return false;
        }
    }

    @Override
    public void activateObject(final Object obj) throws Exception {
    }

    @Override
    public void passivateObject(final Object obj) throws Exception {
    }
}

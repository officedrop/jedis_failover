package com.officedrop.redis.failover.jedis;

import com.officedrop.redis.failover.utils.Action1;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:37 PM
 */
public class CommonsJedisPool implements PoolableObjectFactory, JedisPool {

    private static final Logger log = LoggerFactory.getLogger(CommonsJedisPool.class);

    private final JedisFactory factory;
    private final GenericObjectPool pool;
    private final List<Action1<CommonsJedisPool>> listeners = new CopyOnWriteArrayList<Action1<CommonsJedisPool>>();

    public CommonsJedisPool(JedisFactory factory, JedisPoolConfig config) {
        this.factory = factory;
        this.pool = new GenericObjectPool(this, config);
    }

    public void addListeners( Action1<CommonsJedisPool> ... listeners ) {
        this.listeners.addAll(Arrays.asList(listeners));
    }

    public CommonsJedisPool(JedisFactory factory) {
        this.factory = factory;
        this.pool = new GenericObjectPool(this);
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
            if ( jedis != null ) {
                try {
                    this.pool.returnObject(jedis);
                } catch ( Exception e ) {
                    log.error("Failed to return object to pool", e);
                }
            }
        }
    }

    @Override
    public Object makeObject() throws Exception {
        return this.factory.create();
    }

    @Override
    public void destroyObject(final Object obj) throws Exception {
        try {

            if ( obj instanceof JedisActions ) {
                JedisActions actions = (JedisActions) obj;
                actions.quit();
            }

        } catch ( Exception e ) {
            log.error("Failed to destroy jedis object", e);
        }

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

    public void close() {
        try {
            this.pool.close();
        } catch ( Exception e ) {
            throw new ConnectionException(e);
        } finally {
            for ( Action1<CommonsJedisPool> listener : this.listeners ) {
                try {
                    listener.apply(this);
                } catch ( Exception e ) {
                    log.error("Failed to send even to listener", e);
                }
            }
        }
    }
}

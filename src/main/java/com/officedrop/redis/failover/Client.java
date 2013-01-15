package com.officedrop.redis.failover;

import com.officedrop.redis.failover.jedis.ClientFunction;
import com.officedrop.redis.failover.jedis.JedisClient;
import com.officedrop.redis.failover.jedis.JedisClientFactory;
import com.officedrop.redis.failover.utils.CircularList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

import java.util.*;

/**
 * User: Maurício Linhares
 * Date: 12/17/12
 * Time: 7:04 PM
 */
public class Client implements JedisClient, NodeManagerListener {

    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private ClusterChangeEventSource nodeManager;
    private JedisClientFactory factory;
    private JedisClient master;
    private CircularList<JedisClient> slaves;

    public Client(ClusterChangeEventSource nodeManager, JedisClientFactory factory) {
        this.factory = factory;
        this.nodeManager = nodeManager;

        this.nodeManager.addListeners(this);

        this.updateMaster();
        this.updateSlaves();
    }

    @Override
    public void masterChanged(final NodeManager manager, final ClusterStatus status) {
        log.info("Master has changed -> {}", status.getMaster());
        this.updateMaster();
    }

    @Override
    public void slavesChanged(final NodeManager manager, final ClusterStatus status) {
        log.info("Slaves have changed -> {}", status.getSlaves());
        this.updateSlaves();
    }

    private void updateMaster() {
        this.quitMaster();
        this.master = this.factory.create(this.nodeManager.getLastClusterStatus().getMaster());
    }

    private void updateSlaves() {
        this.quitSlaves();

        List<JedisClient> slaveClients = new ArrayList<JedisClient>();

        for ( HostConfiguration configuration : this.nodeManager.getLastClusterStatus().getSlaves() ) {
            slaveClients.add(this.factory.create(configuration));
        }

        this.slaves = new CircularList<JedisClient>(slaveClients);
    }

    private void quitMaster() {
        if (this.master != null) {
            try {
                this.master.quit();
            } catch (Exception e) {
                log.error("Failed while closing the connection to master", e);
            }
        }
    }

    @Override
    public Long del(final String... keys) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.del(keys);
            }
        });
    }

    private void quitSlaves() {
        if (this.slaves != null) {
            for (JedisClient slave : this.slaves) {
                try {
                    slave.quit();
                } catch (Exception e) {
                    log.error("Failed while closing the connection to the slave", e);
                }
            }
        }
    }

    public String quit() {
        this.quitMaster();
        this.quitSlaves();

        this.nodeManager.removeListeners( this );

        return "OK";
    }

    @Override
    public String ping() {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(final JedisClient client) {
                return client.ping();
            }
        });
    }

    @Override
    public String slaveof(final String host, final int port) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(final JedisClient client) {
                return client.slaveof(host, port);
            }
        });
    }

    @Override
    public String slaveofNoOne() {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(final JedisClient client) {
                return client.slaveofNoOne();
            }
        });
    }

    JedisClient getSlave() {
        if ( this.slaves.isEmpty() ) {
            return this.master;
        } else {
            return this.slaves.next();
        }
    }

    <R> R doAction( ClientType type, ClientFunction<R> function ) {
        JedisClient client;

        switch( type ) {
            case SLAVE:
                client = this.getSlave();
                break;
            default:
                client = master;
                break;
        }

        return function.apply(client);
    }

    @Override
    public String set(final String key, final String value) {
        return this.doAction( ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.set(key, value);
            }
        });
    }

    @Override
    public String get(final String key) {

        return this.doAction(ClientType.SLAVE, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                client.ping();
                return client.get(key);
            }
        });

    }

    @Override
    public Boolean exists(final String key) {
        return this.doAction( ClientType.SLAVE, new ClientFunction<Boolean>() {
            @Override
            public Boolean apply(JedisClient client) {
                return client.exists(key);
            }
        } );
    }

    @Override
    public String type(final String key) {

        return this.doAction( ClientType.SLAVE, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.type(key);
            }
        }  );
    }

    @Override
    public Long expire(final String key, final int seconds) {

        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.expire(key, seconds);
            }
        });

    }

    @Override
    public Long expireAt(final String key, final long unixTime) {

        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.expireAt(key, unixTime);
            }
        });

    }

    @Override
    public Long ttl(final String key) {

        return this.doAction( ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.ttl(key);
            }
        } );

    }

    @Override
    public Boolean setbit(final String key, final long offset, final boolean value) {
        return this.doAction( ClientType.MASTER, new ClientFunction<Boolean>() {
            @Override
            public Boolean apply(JedisClient client) {
                return client.setbit(key, offset, value);
            }
        } );
    }

    @Override
    public Boolean getbit( final String key, final long offset) {

        return this.doAction(ClientType.SLAVE, new ClientFunction<Boolean>() {
            @Override
            public Boolean apply(JedisClient client) {
                return client.getbit(key, offset);
            }
        });

    }

    @Override
    public Long setrange(final String key, final long offset, final String value) {

        return this.doAction( ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.setrange(key, offset, value);
            }
        } );

    }

    @Override
    public String getrange(final String key, final long startOffset, final long endOffset) {

        return this.doAction(ClientType.SLAVE, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.getrange(key, startOffset, endOffset);
            }
        });

    }

    @Override
    public String getSet(final String key, final String value) {

        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.getSet(key, value);
            }
        });

    }

    @Override
    public Long setnx(final String key, final String value) {

        return this.doAction( ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.setnx(key, value);
            }
        } );

    }

    @Override
    public String setex(final String key, final int seconds, final String value) {

        return this.doAction( ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.setex(key, seconds, value);
            }
        } );

    }

    @Override
    public Long decrBy(final String key, final long integer) {

        return this.doAction( ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.decrBy(key, integer);
            }
        } );

    }

    @Override
    public Long decr(final String key) {

        return this.doAction( ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.decr(key);
            }
        } );

    }

    @Override
    public Long incrBy(final String key, final long integer) {

        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.incrBy(key, integer);
            }
        });

    }

    @Override
    public Long incr(final String key) {

        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.incr(key);
            }
        });

    }

    @Override
    public Long append(final String key, final String value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.append(key, value);
            }
        });
    }

    @Override
    public String substr(final String key, final int start, final int end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.substr(key, start, end);
            }
        });
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hset(key, field, value);
            }
        });
    }

    @Override
    public String hget( final String key, final String field) {
        return this.doAction( ClientType.SLAVE, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.hget(key, field);
            }
        } );
    }

    @Override
    public Long hsetnx( final String key, final String field, final String value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hsetnx(key, field, value);
            }
        });
    }

    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.hmset(key, hash);
            }
        });
    }

    @Override
    public List<String> hmget(final String key, final String... fields) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<List<String>>() {
            @Override
            public List<String> apply(JedisClient client) {
                return client.hmget(key, fields);
            }
        });
    }

    @Override
    public Long hincrBy(final String key, final String field, final long value) {
        return this.doAction( ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hincrBy(key, field, value);
            }
        } );
    }

    @Override
    public Boolean hexists(final String key, final String field) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Boolean>() {
            @Override
            public Boolean apply(JedisClient client) {
                return client.hexists(key, field);
            }
        });
    }

    @Override
    public Long hdel(final String key, final String... field) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hdel(key, field);
            }
        });
    }

    @Override
    public Long hlen(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hlen(key);
            }
        });
    }

    @Override
    public Set<String> hkeys(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.hkeys(key);
            }
        });
    }

    @Override
    public List<String> hvals(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<List<String>>() {
            @Override
            public List<String> apply(JedisClient client) {
                return client.hvals(key);
            }
        });
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Map<String, String>>() {
            @Override
            public Map<String, String> apply(JedisClient client) {
                return client.hgetAll(key);
            }
        });
    }

    @Override
    public Long rpush(final String key, final String... string) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.rpush(key, string);
            }
        });
    }

    @Override
    public Long lpush(final String key, final String... string) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.lpush(key, string);
            }
        });
    }

    @Override
    public Long llen(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.llen(key);
            }
        });
    }

    @Override
    public List<String> lrange(final String key, final long start, final long end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<List<String>>() {
            @Override
            public List<String> apply(JedisClient client) {
                return client.lrange(key, start, end);
            }
        });
    }

    @Override
    public String ltrim( final String key, final long start, final long end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.ltrim(key, start, end);
            }
        });
    }

    @Override
    public String lindex(final String key, final long index) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.lindex(key, index);
            }
        });
    }

    @Override
    public String lset( final String key, final long index, final String value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.lset(key, index, value);
            }
        });
    }

    @Override
    public Long lrem(final String key, final long count, final String value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.lrem(key, count, value);
            }
        });
    }

    @Override
    public String lpop(final String key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.lpop(key);
            }
        });
    }

    @Override
    public String rpop(final String key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.rpop(key);
            }
        });
    }

    @Override
    public Long sadd(final String key, final String... member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.sadd(key, member);
            }
        });
    }

    @Override
    public Set<String> smembers(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.smembers(key);
            }
        });
    }

    @Override
    public Long srem(final String key, final String... member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.srem(key, member);
            }
        });
    }

    @Override
    public String spop(final String key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.spop(key);
            }
        });
    }

    @Override
    public Long scard(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.scard(key);
            }
        });
    }

    @Override
    public Boolean sismember(final String key, final String member) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Boolean>() {
            @Override
            public Boolean apply(JedisClient client) {
                return client.sismember(key, member);
            }
        });
    }

    @Override
    public String srandmember(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.srandmember(key);
            }
        });
    }

    @Override
    public Long zadd(final String key, final double score, final String member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zadd(key, score, member);
            }
        });
    }

    @Override
    public Long zadd(final String key, final Map<Double, String> scoreMembers) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zadd(key, scoreMembers);
            }
        });
    }

    @Override
    public Set<String> zrange(final String key, final long start, final long end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrange(key, start, end);
            }
        });
    }

    @Override
    public Long zrem(final String key, final String... member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zrem(key, member);
            }
        });
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Double>() {
            @Override
            public Double apply(JedisClient client) {
                return client.zincrby(key, score, member);
            }
        });
    }

    @Override
    public Long zrank(final String key, final String member) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zrank(key, member);
            }
        });
    }

    @Override
    public Long zrevrank(final String key, final String member) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zrevrank(key, member);
            }
        });
    }

    @Override
    public Set<String> zrevrange(final String key, final long start, final long end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrevrange(key, start, end);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeWithScores(key, start, end);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeWithScores(key, start, end);
            }
        });
    }

    @Override
    public Long zcard(final String key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zcard(key);
            }
        });
    }

    @Override
    public Double zscore(final String key, final String member) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Double>() {
            @Override
            public Double apply(JedisClient client) {
                return client.zscore(key, member);
            }
        });
    }

    @Override
    public List<String> sort(final String key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<List<String>>() {
            @Override
            public List<String> apply(JedisClient client) {
                return client.sort(key);
            }
        });
    }

    @Override
    public List<String> sort(final String key, final SortingParams sortingParameters) {
        return this.doAction(ClientType.MASTER, new ClientFunction<List<String>>() {
            @Override
            public List<String> apply(JedisClient client) {
                return client.sort(key, sortingParameters);
            }
        });
    }

    @Override
    public Long zcount(final String key, final double min, final double max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zcount(key, min, max);
            }
        });
    }

    @Override
    public Long zcount(final String key, final String min, final String max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zcount(key, min, max);
            }
        });
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrangeByScore(key, min, max);
            }
        });
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrangeByScore(key, min, max);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrevrangeByScore(key, max, min);
            }
        });
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrangeByScore(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrevrangeByScore(key, max, min);
            }
        });
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrangeByScore(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrevrangeByScore(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<String>>() {
            @Override
            public Set<String> apply(JedisClient client) {
                return client.zrevrangeByScore(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Long zremrangeByRank(final String key, final long start, final long end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zremrangeByRank(key, start, end);
            }
        });
    }

    @Override
    public Long zremrangeByScore(final String key, final double start, final double end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zremrangeByScore(key, start, end);
            }
        });
    }

    @Override
    public Long zremrangeByScore(final String key, final String start, final String end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zremrangeByScore(key, start, end);
            }
        });
    }

    @Override
    public Long linsert(final String key, final BinaryClient.LIST_POSITION where, final String pivot, final String value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.linsert(key, where, pivot, value);
            }
        });
    }

    @Override
    public String info() {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(final JedisClient client) {
                return client.info();
            }
        });
    }

    @Override
    public Long lpushx(final String key, final String string) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.lpushx(key, string);
            }
        });
    }

    @Override
    public Long rpushx(final String key, final String string) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.rpushx(key, string);
            }
        });
    }

    @Override
    public String set(final byte[] key, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.set(key, value);
            }
        });
    }

    @Override
    public byte[] get(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.get(key);
            }
        });
    }

    @Override
    public Boolean exists(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Boolean>() {
            @Override
            public Boolean apply(JedisClient client) {
                return client.exists(key);
            }
        });
    }

    @Override
    public String type(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.type(key);
            }
        });
    }

    @Override
    public Long expire(final byte[] key, final int seconds) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.expire(key, seconds);
            }
        });
    }

    @Override
    public Long expireAt(final byte[] key, final long unixTime) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.expireAt(key, unixTime);
            }
        });
    }

    @Override
    public Long ttl(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.ttl(key);
            }
        });
    }

    @Override
    public byte[] getSet(final byte[] key, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.getSet(key, value);
            }
        });
    }

    @Override
    public Long setnx(final byte[] key, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.setnx(key, value);
            }
        });
    }

    @Override
    public String setex(final byte[] key, final int seconds, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.setex(key, seconds, value);
            }
        });
    }

    @Override
    public Long decrBy( final byte[] key, final long integer) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.decrBy(key, integer);
            }
        });
    }

    @Override
    public Long decr( final byte[] key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.decr(key);
            }
        });
    }

    @Override
    public Long incrBy( final byte[] key, final long integer) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.incrBy(key, integer);
            }
        });
    }

    @Override
    public Long incr(final byte[] key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.incr(key);
            }
        });
    }

    @Override
    public Long append(final byte[] key, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.append(key, value);
            }
        });
    }

    @Override
    public byte[] substr(final byte[] key, final int start, final int end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.substr(key, start, end);
            }
        });
    }

    @Override
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hset(key, field, value);
            }
        });
    }

    @Override
    public byte[] hget(final byte[] key, final byte[] field) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.hget(key, field);
            }
        });
    }

    @Override
    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hsetnx(key, field, value);
            }
        });
    }

    @Override
    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.hmset(key, hash);
            }
        });
    }

    @Override
    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<List<byte[]>>() {
            @Override
            public List<byte[]> apply(JedisClient client) {
                return client.hmget(key, fields);
            }
        });
    }

    @Override
    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hincrBy(key, field, value);
            }
        });
    }

    @Override
    public Boolean hexists(final byte[] key, final byte[] field) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Boolean>() {
            @Override
            public Boolean apply(JedisClient client) {
                return client.hexists(key, field);
            }
        });
    }

    @Override
    public Long hdel(final byte[] key, final byte[]... field) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hdel(key, field);
            }
        });
    }

    @Override
    public Long hlen(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.hlen(key);
            }
        });
    }

    @Override
    public Set<byte[]> hkeys(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.hkeys(key);
            }
        });
    }

    @Override
    public Collection<byte[]> hvals(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Collection<byte[]>>() {
            @Override
            public Collection<byte[]> apply(JedisClient client) {
                return client.hvals(key);
            }
        }) ;
    }

    @Override
    public Map<byte[], byte[]> hgetAll( final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Map<byte[], byte[]>>() {
            @Override
            public Map<byte[], byte[]> apply(JedisClient client) {
                return client.hgetAll(key);
            }
        });
    }

    @Override
    public Long rpush( final byte[] key, final byte[]... string) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.rpush(key, string);
            }
        });
    }

    @Override
    public Long lpush(final byte[] key, final byte[]... string) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.lpush(key, string);
            }
        });
    }

    @Override
    public Long llen(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.llen(key);
            }
        });
    }

    @Override
    public List<byte[]> lrange(final byte[] key, final int start, final int end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<List<byte[]>>() {
            @Override
            public List<byte[]> apply(JedisClient client) {
                return client.lrange(key, start, end);
            }
        });
    }

    @Override
    public String ltrim(final byte[] key, final int start, final int end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.ltrim(key, start, end);
            }
        });
    }

    @Override
    public byte[] lindex(final byte[] key, final int index) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.lindex(key, index);
            }
        });
    }

    @Override
    public String lset(final byte[] key, final int index, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<String>() {
            @Override
            public String apply(JedisClient client) {
                return client.lset(key, index, value);
            }
        });
    }

    @Override
    public Long lrem(final byte[] key, final int count, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.lrem(key, count, value);
            }
        });
    }

    @Override
    public byte[] lpop(final byte[] key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.lpop(key);
            }
        });
    }

    @Override
    public byte[] rpop(final byte[] key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.rpop(key);
            }
        });
    }

    @Override
    public Long sadd(final byte[] key, final byte[]... member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.sadd(key, member);
            }
        });
    }

    @Override
    public Set<byte[]> smembers(final byte[] key) {
        return this.doAction( ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.smembers(key);
            }
        } );
    }

    @Override
    public Long srem(final byte[] key, final byte[]... member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.srem(key, member);
            }
        });
    }

    @Override
    public byte[] spop(final byte[] key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.spop(key);
            }
        });
    }

    @Override
    public Long scard(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.scard(key);
            }
        } );
    }

    @Override
    public Boolean sismember(final byte[] key, final byte[] member) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Boolean>() {
            @Override
            public Boolean apply(JedisClient client) {
                return client.sismember(key, member);
            }
        });
    }

    @Override
    public byte[] srandmember(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.srandmember(key);
            }
        });
    }

    @Override
    public Long zadd(final byte[] key, final double score, final byte[] member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zadd(key, score, member);
            }
        });
    }

    @Override
    public Long zadd(final byte[] key, final Map<Double, byte[]> scoreMembers) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zadd(key, scoreMembers);
            }
        });
    }

    @Override
    public Set<byte[]> zrange(final byte[] key, final int start, final int end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.zrange(key, start, end);
            }
        });
    }

    @Override
    public Long zrem( final byte[] key, final byte[]... member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zrem(key, member);
            }
        });
    }

    @Override
    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Double>() {
            @Override
            public Double apply(JedisClient client) {
                return client.zincrby(key, score, member);
            }
        });
    }

    @Override
    public Long zrank(final byte[] key, final byte[] member) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zrank(key, member);
            }
        });
    }

    @Override
    public Long zrevrank(final byte[] key, final byte[] member) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zrevrank(key, member);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrange(final byte[] key, final int start, final int end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.zrevrange(key, start, end);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeWithScores(final byte[] key, final int start, final int end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeWithScores(key, start, end);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(final byte[] key, final int start, final int end) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeWithScores(key, start, end);
            }
        });
    }

    @Override
    public Long zcard(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zcard(key);
            }
        });
    }

    @Override
    public Double zscore(final byte[] key, final byte[] member) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Double>() {
            @Override
            public Double apply(JedisClient client) {
                return client.zscore(key, member);
            }
        });
    }

    @Override
    public List<byte[]> sort(final byte[] key) {
        return this.doAction(ClientType.MASTER, new ClientFunction<List<byte[]>>() {
            @Override
            public List<byte[]> apply(JedisClient client) {
                return client.sort(key);
            }
        });
    }

    @Override
    public List<byte[]> sort(final byte[] key, final SortingParams sortingParameters) {
        return this.doAction(ClientType.MASTER, new ClientFunction<List<byte[]>>() {
            @Override
            public List<byte[]> apply(JedisClient client) {
                return client.sort(key, sortingParameters);
            }
        });
    }

    @Override
    public Long zcount(final byte[] key, final double min, final double max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zcount(key, min, max);
            }
        });
    }

    @Override
    public Long zcount(final byte[] key, final byte[] min, final byte[] max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zcount(key, min, max);
            }
        });
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.zrangeByScore(key, min, max);
            }
        });
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.zrangeByScore(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.zrevrangeByScore(key, max, min);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.zrevrangeByScore(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.zrevrangeByScore(key, max, min);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<byte[]>>() {
            @Override
            public Set<byte[]> apply(JedisClient client) {
                return client.zrevrangeByScore(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min, final int offset, final int count) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min, final int offset, final int count) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Set<Tuple>>() {
            @Override
            public Set<Tuple> apply(JedisClient client) {
                return client.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Long zremrangeByRank(final byte[] key, final int start, final int end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zremrangeByRank(key, start, end);
            }
        });
    }

    @Override
    public Long zremrangeByScore(final byte[] key, final double start, final double end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zremrangeByScore(key, start, end);
            }
        } );
    }

    @Override
    public Long zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.zremrangeByScore(key, start, end);
            }
        });
    }

    @Override
    public Long linsert(final byte[] key, final BinaryClient.LIST_POSITION where, final byte[] pivot, final byte[] value) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.linsert(key, where, pivot, value);
            }
        });
    }

    @Override
    public Long objectRefcount(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.objectRefcount(key);
            }
        });
    }

    @Override
    public Long objectIdletime(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.objectIdletime(key);
            }
        });
    }

    @Override
    public byte[] objectEncoding(final byte[] key) {
        return this.doAction(ClientType.SLAVE, new ClientFunction<byte[]>() {
            @Override
            public byte[] apply(JedisClient client) {
                return client.objectEncoding(key);
            }
        });
    }

    @Override
    public Long lpushx(final byte[] key, final byte[] string) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.lpushx(key, string);
            }
        });
    }

    @Override
    public Long rpushx(final byte[] key, final byte[] string) {
        return this.doAction(ClientType.MASTER, new ClientFunction<Long>() {
            @Override
            public Long apply(JedisClient client) {
                return client.rpushx(key, string);
            }
        });
    }
}

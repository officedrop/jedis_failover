package com.officedrop.redis.failover.jedis;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: Maurício Linhares
 * Date: 1/12/13
 * Time: 11:24 AM
 */
public class NamespacedJedisActions implements JedisActions {

    private final JedisActions actions;
    private final String namespace;
    private final String namespaceFormat;

    public NamespacedJedisActions(String namespace, JedisActions actions) {

        if ( namespace == null ) {
            throw new NullPointerException("The 'namespace' property can not ne null");
        }

        this.actions = actions;
        this.namespace = namespace;
        this.namespaceFormat = this.namespace + ":%s";

    }

    private String namespaceKey(String key) {

        if ( key == null ) {
            throw new NullPointerException("The parameter 'key' can not be null");
        }

        return String.format(this.namespaceFormat, key);
    }

    private String[] arrayNamespace(String... keys) {
        String[] result = new String[keys.length];
        for (int x = 0; x < keys.length; x++) {
            result[x] = namespaceKey(keys[x]);
        }
        return result;
    }

    @Override
    public Long del(final String... keys) {
        return this.actions.del( arrayNamespace(keys) );
    }

    @Override
    public String quit() {
        return this.actions.quit();
    }

    @Override
    public String ping() {
        return this.actions.ping();
    }

    @Override
    public String slaveof(final String host, final int port) {
        return this.actions.slaveof(host, port);
    }

    @Override
    public String slaveofNoOne() {
        return this.actions.slaveofNoOne();
    }

    @Override
    public String info() {
        return this.actions.info();
    }

    @Override
    public String set(final String key, final String value) {
        return this.actions.set(this.namespaceKey(key), value);
    }

    @Override
    public String get(final String key) {
        return this.actions.get( this.namespaceKey(key) );
    }

    @Override
    public Boolean exists(final String key) {
        return this.actions.exists(this.namespaceKey(key));
    }

    @Override
    public String type(final String key) {
        return this.actions.type( this.namespaceKey(key) );
    }

    @Override
    public Long expire(final String key, final int seconds) {
        return this.actions.expire( this.namespaceKey(key), seconds );
    }

    @Override
    public Long expireAt(final String key, final long unixTime) {
        return this.actions.expireAt( this.namespaceKey(key), unixTime );
    }

    @Override
    public Long ttl(final String key) {
        return this.actions.ttl( this.namespaceKey(key) );
    }

    @Override
    public Boolean setbit(final String key, final long offset, final boolean value) {
        return this.actions.setbit(this.namespaceKey(key), offset, value);
    }

    @Override
    public Boolean getbit(final String key, final long offset) {
        return this.actions.getbit( this.namespaceKey( key ), offset );
    }

    @Override
    public Long setrange(final String key, final long offset, final String value) {
        return this.actions.setrange(this.namespaceKey(key), offset, value);
    }

    @Override
    public String getrange(final String key, final long startOffset, final long endOffset) {
        return this.actions.getrange(this.namespaceKey(key), startOffset, endOffset);
    }

    @Override
    public String getSet(final String key, final String value) {
        return this.actions.getSet(this.namespaceKey(key), value);
    }

    @Override
    public Long setnx(final String key, final String value) {
        return this.actions.setnx( this.namespaceKey(key), value );
    }

    @Override
    public String setex(final String key, final int seconds, final String value) {
        return this.actions.setex( this.namespaceKey(key), seconds, value );
    }

    @Override
    public Long decrBy(final String key, final long integer) {
        return this.actions.decrBy(this.namespaceKey(key), integer);
    }

    @Override
    public Long decr(final String key) {
        return this.actions.decr(this.namespaceKey(key));
    }

    @Override
    public Long incrBy(final String key, final long integer) {
        return this.actions.incrBy(this.namespaceKey(key), integer);
    }

    @Override
    public Long incr(final String key) {
        return this.actions.incr(this.namespaceKey(key));
    }

    @Override
    public Long append(final String key, final String value) {
        return this.actions.append( this.namespaceKey(key), value );
    }

    @Override
    public String substr(final String key, final int start, final int end) {
        return this.actions.substr(this.namespaceKey(key), start, end);
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        return this.actions.hset(this.namespaceKey(key), field, value);
    }

    @Override
    public String hget(final String key, final String field) {
        return this.actions.hget( this.namespaceKey(key), field );
    }

    @Override
    public Long hsetnx(final String key, final String field, final String value) {
        return this.actions.hsetnx(this.namespaceKey(key), field, value);
    }

    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        return this.actions.hmset(this.namespaceKey(key), hash);
    }

    @Override
    public List<String> hmget(final String key, final String... fields) {
        return this.actions.hmget(this.namespaceKey(key), fields);
    }

    @Override
    public Long hincrBy(final String key, final String field, final long value) {
        return this.actions.hincrBy(this.namespaceKey(key), field, value);
    }

    @Override
    public Boolean hexists(final String key, final String field) {
        return this.actions.hexists(this.namespaceKey(key), field);
    }

    @Override
    public Long hdel(final String key, final String... field) {
        return this.actions.hdel(this.namespaceKey(key), field);
    }

    @Override
    public Long hlen(final String key) {
        return this.actions.hlen(this.namespaceKey(key));
    }

    @Override
    public Set<String> hkeys(final String key) {
        return this.actions.hkeys(this.namespaceKey(key));
    }

    @Override
    public List<String> hvals(final String key) {
        return this.actions.hvals(this.namespaceKey(key));
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        return this.actions.hgetAll(this.namespaceKey(key));
    }

    @Override
    public Long rpush(final String key, final String... string) {
        return this.actions.rpush(this.namespaceKey(key), string);
    }

    @Override
    public Long lpush(final String key, final String... string) {
        return this.actions.lpush(this.namespaceKey(key), string);
    }

    @Override
    public Long llen(final String key) {
        return this.actions.llen(this.namespaceKey(key));
    }

    @Override
    public List<String> lrange(final String key, final long start, final long end) {
        return this.actions.lrange(this.namespaceKey(key), start, end);
    }

    @Override
    public String ltrim(final String key, final long start, final long end) {
        return this.actions.ltrim(this.namespaceKey(key), start, end);
    }

    @Override
    public String lindex(final String key, final long index) {
        return this.actions.lindex(this.namespaceKey(key), index);
    }

    @Override
    public String lset(final String key, final long index, final String value) {
        return this.actions.lset( this.namespaceKey(key), index, value );
    }

    @Override
    public Long lrem(final String key, final long count, final String value) {
        return this.actions.lrem( this.namespaceKey(key), count, value );
    }

    @Override
    public String lpop(final String key) {
        return this.actions.lpop( this.namespaceKey(key) );
    }

    @Override
    public String rpop(final String key) {
        return this.actions.rpop(this.namespaceKey(key));
    }

    @Override
    public Long sadd(final String key, final String... member) {
        return this.actions.sadd(this.namespaceKey(key), member);
    }

    @Override
    public Set<String> smembers(final String key) {
        return this.actions.smembers(this.namespaceKey(key));
    }

    @Override
    public Long srem(final String key, final String... member) {
        return this.actions.srem(this.namespaceKey(key), member);
    }

    @Override
    public String spop(final String key) {
        return this.actions.spop(this.namespaceKey(key));
    }

    @Override
    public Long scard(final String key) {
        return this.actions.scard(this.namespaceKey(key));
    }

    @Override
    public Boolean sismember(final String key, final String member) {
        return this.actions.sismember(this.namespaceKey(key), member);
    }

    @Override
    public String srandmember(final String key) {
        return this.actions.srandmember(this.namespaceKey(key));
    }

    @Override
    public Long zadd(final String key, final double score, final String member) {
        return this.actions.zadd(this.namespaceKey(key), score, member);
    }

    @Override
    public Long zadd(final String key, final Map<Double, String> scoreMembers) {
        return this.actions.zadd( this.namespaceKey(key), scoreMembers );
    }

    @Override
    public Set<String> zrange(final String key, final long start, final long end) {
        return this.actions.zrange( this.namespaceKey(key), start, end );
    }

    @Override
    public Long zrem(final String key, final String... member) {
        return this.actions.zrem(this.namespaceKey(key), member);
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        return this.actions.zincrby(this.namespaceKey(key), score, member);
    }

    @Override
    public Long zrank(final String key, final String member) {
        return this.actions.zrank(this.namespaceKey(key), member);
    }

    @Override
    public Long zrevrank(final String key, final String member) {
        return this.actions.zrevrank(this.namespaceKey(key), member);
    }

    @Override
    public Set<String> zrevrange(final String key, final long start, final long end) {
        return this.actions.zrevrange(this.namespaceKey(key), start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
        return this.actions.zrangeWithScores(this.namespaceKey(key), start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
        return this.actions.zrevrangeWithScores(this.namespaceKey(key), start, end);
    }

    @Override
    public Long zcard(final String key) {
        return this.actions.zcard(this.namespaceKey(key));
    }

    @Override
    public Double zscore(final String key, final String member) {
        return this.actions.zscore(this.namespaceKey(key), member);
    }

    @Override
    public List<String> sort(final String key) {
        return this.actions.sort(this.namespaceKey(key));
    }

    @Override
    public List<String> sort(final String key, final SortingParams sortingParameters) {
        return this.actions.sort(this.namespaceKey(key), sortingParameters);
    }

    @Override
    public Long zcount(final String key, final double min, final double max) {
        return this.actions.zcount(this.namespaceKey(key), min, max);
    }

    @Override
    public Long zcount(final String key, final String min, final String max) {
        return this.actions.zcount(this.namespaceKey(key), min, max);
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return this.actions.zrangeByScore(this.namespaceKey(key), min, max);
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        return this.actions.zrangeByScore(this.namespaceKey(key), min, max);
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        return this.actions.zrevrangeByScore(this.namespaceKey(key), max, min);
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset, final int count) {
        return this.actions.zrangeByScore(this.namespaceKey(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        return this.actions.zrevrangeByScore(this.namespaceKey(key), max, min);
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max, final int offset, final int count) {
        return this.actions.zrangeByScore(this.namespaceKey(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, final int count) {
        return this.actions.zrevrangeByScore(this.namespaceKey(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return this.actions.zrangeByScoreWithScores(this.namespaceKey(key), min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return this.actions.zrevrangeByScoreWithScores(this.namespaceKey(key), max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset, final int count) {
        return this.actions.zrangeByScoreWithScores(this.namespaceKey(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset, final int count) {
        return this.actions.zrevrangeByScore(this.namespaceKey(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
        return this.actions.zrangeByScoreWithScores(this.namespaceKey(key), min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
        return this.actions.zrevrangeByScoreWithScores(this.namespaceKey(key), max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max, final int offset, final int count) {
        return this.actions.zrangeByScoreWithScores(this.namespaceKey(key), min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset, final int count) {
        return this.actions.zrevrangeByScoreWithScores(this.namespaceKey(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min, final int offset, final int count) {
        return this.actions.zrevrangeByScoreWithScores(this.namespaceKey(key), max, min, offset, count);
    }

    @Override
    public Long zremrangeByRank(final String key, final long start, final long end) {
        return this.actions.zremrangeByRank(this.namespaceKey(key), start, end);
    }

    @Override
    public Long zremrangeByScore(final String key, final double start, final double end) {
        return this.actions.zremrangeByScore(this.namespaceKey(key), start, end);
    }

    @Override
    public Long zremrangeByScore(final String key, final String start, final String end) {
        return this.actions.zremrangeByScore(this.namespaceKey(key), start, end);
    }

    @Override
    public Long linsert(final String key, final BinaryClient.LIST_POSITION where, final String pivot, final String value) {
        return this.actions.linsert(this.namespaceKey(key), where, pivot, value);
    }

    @Override
    public Long lpushx(final String key, final String string) {
        return this.actions.lpushx(this.namespaceKey(key), string);
    }

    @Override
    public Long rpushx(final String key, final String string) {
        return this.actions.rpushx(this.namespaceKey(key), string);
    }
}

# jedis_failover - a failover solution for redis server clusters

This plugin is an implementation of the behaviour implemented originally at the [redis_failover](https://github.com/ryanlecompte/redis_failover)
Ruby gem. It's main goal is to make your redis cluster to be highly available by automatically switching over the
 master status to a server that is available once the real master goes down or removing slaves that are not currently
 available so that clients don't try to access them.

The cluster status data lives in a [ZooKeeper](http://zookeeper.apache.org/) server or cluster. Each client process then
becomes a **NodeManager**, that is, a watcher over the overall cluster status. Each **NodeManager** watches over the **Redis**
cluster and validates the cluster status. They periodically send their current view of the system to ZooKeeper and one
of them is selected as the **cluster leader** and he is the one that makes the final decision if a node is down or
if there is a need to switch the master to a different server (and also makes the configuration changes on Redis as
expected).

## Usage

To use this project with just the defaults set, you can just create a pool using the pool builder as in:

```java
JedisPool pool = new JedisPoolBuilder()
    .withFailoverConfiguration(
        "localhost:2838", // ZooKeeper cluster URL
        Arrays.asList(new HostConfiguration("localhost", 7000), new HostConfiguration("localhost", 7001))) // List of redis servers
    .build();

pool.withJedis(new JedisFunction() {
    @Override
    public void execute(final JedisActions jedis) throws Exception {
        jedis.ping();
    }
});
```

And you can just use the pool to access your Redis servers as expected. If you would like to be able to configure more
stuff, you can, for now, check the source code and tests and see the other variables that can be changed on the
configuration.

This library is compatible with the [redis_failover](https://github.com/ryanlecompte/redis_failover) as long as you don't
the gem's node manager daemon. If you run only in client mode, the clients will correctly read the failover configuration
and follow it.

## Pending

* More configuration
* Fix leader election bug that prevents the library to work with the **redis_failover** gem node managers
* Allow for pools to be created without a node manager (client only mode, as in the Ruby gem)
* More tests
* Open HTTP port to allow for easy manual failover even for running processes
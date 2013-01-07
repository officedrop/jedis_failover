package com.officedrop.redis.failover;

import com.officedrop.redis.failover.jedis.ConnectionException;
import com.officedrop.redis.failover.jedis.JedisActions;
import com.officedrop.redis.failover.jedis.JedisClientFactory;
import com.officedrop.redis.failover.utils.Action;
import com.officedrop.redis.failover.utils.Action1;
import com.officedrop.redis.failover.utils.Benchmarker;
import com.officedrop.redis.failover.utils.Function1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: Maurício Linhares
 * Date: 12/24/12
 * Time: 6:22 PM
 */
public class Node {

    private static final Logger log = LoggerFactory.getLogger(Node.class);

    private JedisActions client;
    private volatile int currentErrorCount = 0;
    private volatile boolean shutdown;
    private final long sleepDelay;
    private final List<NodeListener> listeners = new CopyOnWriteArrayList<NodeListener>();
    private final int maxErrors;
    private final HostConfiguration hostConfiguration;
    private final JedisClientFactory factory;
    private final ReentrantLock lock = new ReentrantLock();
    private NodeState currentState = new NodeState();

    public Node(HostConfiguration hostConfiguration, JedisClientFactory factory, long sleepDelay, int maxErrors) {
        this.hostConfiguration = hostConfiguration;
        this.factory = factory;
        this.sleepDelay = sleepDelay;
        this.maxErrors = maxErrors;
    }

    public void addNodeListeners(NodeListener... listeners) {
        this.listeners.addAll(Arrays.asList(listeners));
    }

    public HostConfiguration getHostConfiguration() {
        return this.hostConfiguration;
    }

    public void stop() {
        this.shutdown = true;
        this.listeners.clear();
        this.currentState = new NodeState();
    }

    public NodeState getCurrentState() {
        return this.currentState;
    }

    public void start() {
        this.shutdown = false;

        while (!this.shutdown) {
            try {
                long latency = Benchmarker.benchmark(new Action() {
                    @Override
                    public void apply() {
                        nodeAction(new Action1<JedisActions>() {
                            @Override
                            public void apply(final JedisActions parameter) {
                                client.ping();
                            }
                        });
                    }
                });

                this.currentErrorCount = 0;

                this.currentState = new NodeState(latency);

                for (NodeListener listener : this.listeners) {
                    try {
                        listener.nodeIsOnline(this, latency);
                    } catch (Exception e) {
                        log.error(String.format("Error sending online event to listener - %s", listener), e);
                    }
                }

                try {
                    Thread.sleep(this.sleepDelay);
                } catch (InterruptedException interrupted) {
                    log.error("Interrupted while sleeping", interrupted);
                }

            } catch (Exception e) {
                log.error("Exception at loop, ignoring it since it's going to be sent as an event", e);
            }
        }
    }

    public void makeSlaveOf(final String host, final int port) {
        this.nodeAction(new Action1<JedisActions>() {
            @Override
            public void apply(final JedisActions parameter) {
                client.slaveof(host, port);
            }
        });
    }

    public void becomeMaster() {
        this.nodeAction(new Action1<JedisActions>() {
            @Override
            public void apply(final JedisActions parameter) {
                client.slaveofNoOne();
            }
        });
    }


    public boolean isMaster() {
        return InfoKeys.MASTER.equals(this.info().get(InfoKeys.ROLE));
    }

    public HostConfiguration master() {

        Map<String,String> info = this.info();

        if ( info.containsKey( InfoKeys.MASTER_HOST ) && info.containsKey(InfoKeys.MASTER_PORT) ) {
            return new HostConfiguration( info.get( InfoKeys.MASTER_HOST ), Integer.valueOf( InfoKeys.MASTER_PORT ) );
        } else {
            return null;
        }

    }

    private void fireErrorEvent(Exception e) {

        this.currentErrorCount++;

        log.error(String.format("Failed to talk to redis - error count is %s", this.currentErrorCount), e);

        if (this.currentErrorCount > this.maxErrors) {
            this.currentState = new NodeState();
            for (NodeListener listener : this.listeners) {
                try {
                    listener.nodeIsOffline(this, e);
                } catch (Exception exception) {
                    log.error(String.format("Failed to signal offline event to listener %s", listener), exception);
                }

            }
        }
    }

    private void nodeAction(final Action1<JedisActions> action) {
        this.nodeFunction(new Function1<JedisActions, Object>() {
            @Override
            public Object apply(final JedisActions parameter) {
                action.apply(parameter);
                return null;
            }
        });
    }

    private <OUT> OUT nodeFunction(Function1<JedisActions, OUT> function) {
        try {
            this.lock.lock();

            if (this.client == null) {
                this.client = this.factory.create(this.hostConfiguration);
            }

            return function.apply(this.client);

        } catch (Exception e) {
            this.client = null;
            this.fireErrorEvent(e);
            throw new ConnectionException(e);
        } finally {
            this.lock.unlock();
        }
    }

    public Map<String, String> info() {
        return this.nodeFunction(new Function1<JedisActions, Map<String, String>>() {
            @Override
            public Map<String, String> apply(final JedisActions parameter) {
                return parseInfo(parameter.info());
            }
        });
    }

    Map<String, String> parseInfo(String data) {

        Map<String, String> parameters = new HashMap<String, String>();

        for (String line : data.split("\r\n")) {

            if (!line.trim().isEmpty()) {
                String[] pair = line.split(":");
                parameters.put(pair[0], pair[1]);
            }

        }

        return parameters;
    }

    public int hashCode() {
        return this.hostConfiguration.hashCode();
    }

    public boolean equals( Object o ) {
        boolean result = false;

        if ( o instanceof Node ) {
            result = this.getHostConfiguration().equals( ((Node) o).getHostConfiguration() );
        }

        return result;
    }

}

package com.officedrop.redis.failover.redis;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.utils.Action1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Maurício Linhares
 * Date: 1/2/13
 * Time: 9:31 PM
 */
public class RedisServer implements Closeable {

    private static final AtomicInteger PORT = new AtomicInteger(10000);
    private static final Logger log = LoggerFactory.getLogger(RedisServer.class);

    private final String address;
    private final int port;
    private final ServerSocket server;
    private final ConcurrentMap<String, String> map = new ConcurrentHashMap<String, String>();
    private String masterHost;
    private String masterPort;
    private volatile boolean running;
    private boolean alwaysTimeout;
    private final List<RedisClientHandler> handlers = new CopyOnWriteArrayList<RedisClientHandler>();

    public RedisServer(String address, int port) {
        try {
            this.address = address;
            this.port = port;
            this.server = new ServerSocket();
            this.server.bind(new InetSocketAddress(address, port));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void set(String key, String value) {
        this.map.put(key, value);
    }

    public String get(String key) {
        return this.map.get(key);
    }

    public boolean isAlwaysTimeout() {
        return this.alwaysTimeout;
    }

    public String getAddress() {
        return this.address;
    }

    public int getPort() {
        return this.port;
    }

    public void setAlwaysTimeout(boolean value) {
        this.alwaysTimeout = value;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public void setMasterHost(final String masterHost) {
        this.masterHost = masterHost;
    }

    public String getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(final String masterPort) {
        this.masterPort = masterPort;
    }

    public void setMasterPort(final int masterPort) {
        this.masterPort = String.valueOf(masterPort);
    }

    public void start() {

        this.running = true;

        log.info("started server - {}:{}", this.getAddress(), this.getPort());

        Thread acceptor = new Thread(new Runnable() {
            @Override
            public void run() {

                while (isRunning()) {
                    try {
                        final Socket socket = getServer().accept();

                        log.info("Accepted client {}:{}", socket.getInetAddress(), getPort());

                        RedisClientHandler handler = new RedisClientHandler(RedisServer.this, socket);

                        handlers.add(handler);

                        handler.start();

                    } catch (Exception e) {
                        log.error("Failed to accept socket connection", e);
                    }
                }

            }
        });

        acceptor.start();

    }

    public ServerSocket getServer() {
        return this.server;
    }

    public boolean isRunning() {
        return this.running;
    }

    public void close() {
        this.stop();
    }

    public void stop() {
        if (this.running) {
            this.running = false;
            try {
                getServer().close();
            } catch (Exception e) {
                log.error("Failed to close socket server", e);
                throw new IllegalStateException(e);
            }
        }
    }

    public HostConfiguration getHostConfiguration() {
        return new HostConfiguration(this.address, this.port);
    }

    public HostConfiguration getMasterConfiguration() {
        return new HostConfiguration( this.masterHost, Integer.valueOf(this.masterPort) );
    }

    public static void withServer(Action1<RedisServer> action) {
        RedisServer server = new RedisServer("localhost", PORT.incrementAndGet());

        server.start();

        try {
            action.apply(server);
        } finally {
            server.stop();
        }
    }


}
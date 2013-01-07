package com.officedrop.redis.failover;

import redis.clients.jedis.Protocol;

/**
 * User: Maurício Linhares
 * Date: 12/18/12
 * Time: 5:17 PM
 */
public class HostConfiguration {

    private final String host;
    private final int port;
    private final int timeout;

    public HostConfiguration(String host, int port) {
        this( host, port, Protocol.DEFAULT_TIMEOUT );
    }

    public HostConfiguration(String host, int port, int timeout) {

        if ( host == null || host.trim().isEmpty() ) {
            throw new IllegalArgumentException("'host' can not be null");
        }

        this.port = port;
        this.timeout = timeout;
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public int getTimeout() {
        return this.timeout;
    }

    public String asHost() {
        return String.format("%s:%s", this.getHost(), this.getPort());
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof HostConfiguration)) return false;

        HostConfiguration that = (HostConfiguration) o;

        if (port != that.port) return false;
        if (!host.equals(that.host)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "HostConfiguration{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", timeout=" + timeout +
                '}';
    }

}
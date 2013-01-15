package com.officedrop.redis.failover.redis;

import java.io.*;
import java.nio.charset.Charset;

/**
 * User: Maurício Linhares
 * Date: 1/2/13
 * Time: 10:39 PM
 */
public class RedisWriter extends FilterOutputStream {

    private static final String SUCCESS =  "+%s\r\n";
    private static final String ERROR = "-ERR_%s\r\n";

    public RedisWriter( final OutputStream out ) {
        super(out);
    }

    public void write( String data ) {
        try {
            this.write(data.getBytes(Charset.forName("utf-8")));
            this.flush();
        } catch ( Exception e ) {
            throw new IllegalStateException(e);
        }
    }

    public void writeSuccess( String data ) {
        this.write(String.format(SUCCESS, data));
    }

    public void writeError( String data ) {
        this.write(String.format(ERROR, data));
    }

    public void writeEmpty() {
        this.write("$-1\r\n");
    }

    public void writeData( String data ) {
        byte[] bytes = data.getBytes();

        try {
            this.write(String.format("$%s\r\n", bytes.length));
            this.write(bytes);
            this.write("\r\n");
        } catch ( Exception e ) {
            throw new IllegalStateException(e);
        }

    }

}

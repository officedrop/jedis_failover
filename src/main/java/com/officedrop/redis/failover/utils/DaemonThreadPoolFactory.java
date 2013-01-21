package com.officedrop.redis.failover.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created with IntelliJ IDEA.
 * User: mauricio
 * Date: 1/21/13
 * Time: 11:35 AM
 */
public class DaemonThreadPoolFactory implements ThreadFactory {

    public static final ThreadFactory INSTANCE = new DaemonThreadPoolFactory();
    private static final ThreadFactory DEFAULT = Executors.defaultThreadFactory();

    @Override
    public Thread newThread(Runnable r) {
        Thread t = DEFAULT.newThread(r);
        t.setDaemon(true);
        return t;
    }

    public static final ExecutorService newCachedPool() {
        return Executors.newCachedThreadPool( INSTANCE );
    }


}

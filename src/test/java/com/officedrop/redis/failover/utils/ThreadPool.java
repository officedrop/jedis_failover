package com.officedrop.redis.failover.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 9:49 AM
 */
public class ThreadPool {

    private static final ExecutorService POOL = Executors.newCachedThreadPool();

    public static void submit( Runnable r ) {
        POOL.submit(r);
    }

}

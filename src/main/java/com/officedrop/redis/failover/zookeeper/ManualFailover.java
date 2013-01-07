package com.officedrop.redis.failover.zookeeper;

/**
 * User: Maurício Linhares
 * Date: 12/31/12
 * Time: 6:07 PM
 */

public class ManualFailover {

    private static final String ZNODE_PATH = "%s/manual_failover";
    private static final String ANY_SLAVE = "ANY_SLAVE";

    public static String path( String rootNode ) {
        return String.format(ZNODE_PATH, rootNode );
    }

}
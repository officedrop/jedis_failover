package com.officedrop.redis.failover;

/**
 * User: Maurício Linhares
 * Date: 1/5/13
 * Time: 6:38 PM
 */
public enum ClusterStatusDifference {

    MASTER,
    SLAVES,
    BOTH,
    NO_DIFFERENCE

}
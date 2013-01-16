package com.officedrop.redis.failover;

import java.util.Map;

/**
 * User: Maurício Linhares
 * Date: 12/19/12
 * Time: 2:08 PM
 */
public interface ZooKeeperEventListener {

    public void clusterDataChanged( ZooKeeperClient client, ClusterStatus clusterStatus );

}

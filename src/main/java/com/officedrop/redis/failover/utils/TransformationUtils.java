package com.officedrop.redis.failover.utils;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.Node;
import com.officedrop.redis.failover.NodeState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: mauricio
 * Date: 1/21/13
 * Time: 4:30 PM
 */
public class TransformationUtils {

    public static Map<HostConfiguration, Collection<NodeState>> toStatusByNode(Map<String, Map<HostConfiguration, NodeState>> nodesData) {
        Map<HostConfiguration, Collection<NodeState>> statusByNode = new HashMap<HostConfiguration, Collection<NodeState>>();

        for (Map<HostConfiguration, NodeState> entry : nodesData.values()) {
            for (Map.Entry<HostConfiguration, NodeState> state : entry.entrySet()) {
                Collection<NodeState> nodeStates = statusByNode.get(state.getKey());

                if (nodeStates == null) {
                    nodeStates = new ArrayList<NodeState>();
                    statusByNode.put(state.getKey(), nodeStates);
                }

                nodeStates.add(state.getValue());
            }
        }

        return statusByNode;
    }

    public static Map<HostConfiguration, NodeState> toNodeStates( Collection<Node> nodes ) {

        Map<HostConfiguration, NodeState> states = new HashMap<HostConfiguration, NodeState>();

        for (Node node : nodes) {
            states.put(node.getHostConfiguration(), node.getCurrentState());
        }

        return states;
    }

}

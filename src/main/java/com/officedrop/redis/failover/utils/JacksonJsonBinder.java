package com.officedrop.redis.failover.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.officedrop.redis.failover.ClusterStatus;
import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 4:20 PM
 */
public class JacksonJsonBinder implements JsonBinder {

    private static final String AVAILABLE = "available";
    private static final String UNAVAILABLE = "unavailable";
    private static final String MASTER = "master";
    private static final String SLAVES = "slaves";

    private static final Logger log = LoggerFactory.getLogger(JacksonJsonBinder.class);

    public static final JacksonJsonBinder BINDER = new JacksonJsonBinder();

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] toBytes(Map<HostConfiguration, NodeState> nodeStates) {
        Map<String,Object> result = new HashMap<String, Object>();

        try {
            Map<String,Long> availables = new HashMap<String, Long>();
            List<String> unavailables = new ArrayList<String>();

            for ( Map.Entry<HostConfiguration,NodeState> entry : nodeStates.entrySet() ) {

                if ( entry.getValue().isOffline() ) {
                    unavailables.add( entry.getKey().asHost() );
                } else {
                    availables.put( entry.getKey().asHost(), entry.getValue().getLatency() );
                }
            }

            result.put(AVAILABLE, availables);
            result.put(UNAVAILABLE, unavailables);

            return this.mapper.writeValueAsBytes(result);
        } catch ( Exception e ) {
            log.error("Failed to generate JSON from data", e);
            throw new JsonBinderException(e);
        }
    }

    @Override
    public Map<HostConfiguration,NodeState> toNodeState(byte[] data) {

        try {

            Map<HostConfiguration,NodeState> nodeStates = new HashMap<HostConfiguration, NodeState>();

            JsonNode node = this.mapper.readTree(data);

            JsonNode availables = node.get( AVAILABLE );

            if ( availables != null ) {
                Iterator<Map.Entry<String,JsonNode>> iterator = availables.fields();
                while ( iterator.hasNext() ) {
                    Map.Entry<String,JsonNode> element = iterator.next();
                    String[] pairs = element.getKey().split(":");
                    HostConfiguration host = new HostConfiguration(pairs[0], Integer.valueOf(pairs[1]));
                    nodeStates.put(host, new NodeState(element.getValue().asLong(), false));
                }
            }

            JsonNode unavailables = node.get(UNAVAILABLE);

            if ( unavailables != null ) {
                Iterator<JsonNode> iterator = unavailables.iterator();
                while ( iterator.hasNext() ) {
                    JsonNode element = iterator.next();
                    String[] pairs = element.textValue().split(":");
                    HostConfiguration host = new HostConfiguration(pairs[0], Integer.valueOf(pairs[1]));
                    nodeStates.put(host, new NodeState());
                }
            }

            return nodeStates;
        } catch ( Exception e ) {
            log.error("Failed to read JSON data", e);
            throw new JsonBinderException(e);
        }
    }

    @Override
    public byte[] toBytes(final ClusterStatus clusterStatus) {

        try {

            Map<String,Object> data = new HashMap<String, Object>();

            if ( clusterStatus.hasMaster() ) {
                data.put(MASTER, clusterStatus.getMaster().asHost());
            }

            List<String> slaves = new ArrayList<String>();

            for ( HostConfiguration c : clusterStatus.getSlaves() ) {
                slaves.add(c.asHost());
            }

            List<String> unavailable = new ArrayList<String>();

            for ( HostConfiguration c : clusterStatus.getUnavailables() ) {
                unavailable.add(c.asHost());
            }

            data.put(SLAVES, slaves);
            data.put(UNAVAILABLE, unavailable);

            return this.mapper.writeValueAsBytes( data );
        } catch ( Exception e ) {
            throw new JsonBinderException(e);
        }

    }

    @Override
    public ClusterStatus toClusterStatus(final byte[] data) {

        try {

            JsonNode node = mapper.readTree(data);

            HostConfiguration master = null;

            if ( node.has(MASTER) ) {

                JsonNode masterNode = node.get(MASTER);

                if ( masterNode != null
                        || !masterNode.isNull() ) {
                    String[] paths = masterNode.asText().split(":");
                    master = new HostConfiguration( paths[0], Integer.valueOf( paths[1] ) );
                }

            }

            List<HostConfiguration> slaves = this.toConfigurations(node.get(SLAVES));
            List<HostConfiguration> unavailable = this.toConfigurations(node.get(UNAVAILABLE));

            return new ClusterStatus(master, slaves, unavailable);
        } catch ( Exception e ) {
            throw new JsonBinderException(e);
        }

    }

    List<HostConfiguration> toConfigurations( JsonNode node ) {

        List<HostConfiguration> result = new ArrayList<HostConfiguration>();

        for ( JsonNode element : node ) {
            String[] paths = element.asText().split(":");
            result.add( new HostConfiguration( paths[0], Integer.valueOf( paths[1] ) ) );
        }

        return result;
    }

}

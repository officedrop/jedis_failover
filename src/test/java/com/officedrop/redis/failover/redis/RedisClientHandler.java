package com.officedrop.redis.failover.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 9:30 AM
 */
public class RedisClientHandler {

    private static final Logger log = LoggerFactory.getLogger(RedisClientHandler.class);

    private final RedisServer server;
    private final Socket socket;

    public RedisClientHandler(RedisServer server, Socket socket) {
        this.server = server;
        this.socket = socket;
    }

    public void start() {

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {

                try {

                    Scanner scanner = new Scanner(socket.getInputStream());
                    scanner.useDelimiter("\r\n");

                    RedisWriter writer = new RedisWriter(socket.getOutputStream());

                    while (server.isRunning()) {
                        String line = scanner.nextLine();

                        if (line.startsWith("*")) {
                            int parameterCount = Integer.valueOf(line.substring(1));

                            List<String> parameters = new ArrayList<String>();

                            while (parameterCount != parameters.size()) {
                                scanner.nextLine();
                                parameters.add(scanner.nextLine());
                            }

                            log.debug("Received command {}", parameters);

                            if (!server.isAlwaysTimeout()) {
                                switch (RedisCommand.valueOf(parameters.get(0))) {
                                    case PING:
                                        writer.writeSuccess("PONG");
                                        break;
                                    case INFO:
                                        if (server.getMasterHost() == null || "NO".equals(server.getMasterHost())) {
                                            writer.writeData("role:master\r\nconnected_slaves:0");
                                        } else {
                                            writer.writeData(
                                                    String.format("role:slave\r\nmaster_port:%s\r\nmaster_host:%s",
                                                            server.getMasterPort(),
                                                            server.getMasterHost()
                                                            )
                                            );
                                        }
                                        break;
                                    case SLAVEOF:
                                        server.setMasterHost(parameters.get(1));
                                        server.setMasterPort(parameters.get(2));
                                        writer.writeSuccess("OK");
                                        break;
                                    case GET:
                                        String value = server.get(parameters.get(1));
                                        if ( value != null ) {
                                            writer.writeData(value);
                                        } else {
                                            writer.writeEmpty();
                                        }
                                        break;
                                    case SET:
                                        server.set(parameters.get(1), parameters.get(2));
                                        writer.writeSuccess("OK");
                                        break;
                                    case SELECT:
                                        writer.writeSuccess("OK");
                                        break;
                                    case QUIT:
                                        writer.writeSuccess("OK");
                                        break;
                                    default:
                                        writer.writeError("NOT_IMPLEMENTED");
                                }
                            }

                        } else {
                            writer.writeError("INVALID_PROTOCOL");
                            socket.close();
                        }

                    }

                    scanner.close();

                } catch (Exception e) {
                    log.error("Error reading from socket", e);
                } finally {
                    try {
                        socket.close();
                    } catch (Exception e) {
                        log.error("Failed to close the socket", e);
                    }
                }

            }
        });

        t.start();

    }

}

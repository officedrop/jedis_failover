package com.officedrop.redis.failover.utils;

/**
 * User: Maurício Linhares
 * Date: 1/2/13
 * Time: 5:46 PM
 */
public class PathUtils {

    public static final String toPath( String ... paths ) {

        StringBuilder builder = new StringBuilder();

        for ( String path : paths ) {

            if ( path.endsWith("/") ) {
                path = path.substring(0, path.length() - 1);
            }

            if ( path.startsWith("/") ) {
                builder.append(path);
            } else {
                builder.append("/" + path);
            }

        }

        return builder.toString();
    }

}

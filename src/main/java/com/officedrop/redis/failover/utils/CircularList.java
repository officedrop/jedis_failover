package com.officedrop.redis.failover.utils;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: Maurício Linhares
 * Date: 12/19/12
 * Time: 2:12 PM
 */
public class CircularList<T> implements Iterable<T> {

    private final ReentrantLock lock = new ReentrantLock();
    private final List<T> collection;
    private int currentIndex;

    public CircularList(Collection<T> iterator) {
        this.collection = new ArrayList<T>(iterator);
    }

    public CircularList( T ... items ) {
        this(Arrays.asList(items));
    }

    public int getSize() {
        return this.collection.size();
    }

    public boolean isEmpty() {
        return this.collection.size() == 0;
    }

    public T next() {

        try {
            this.lock.lock();

            T item = this.collection.get(this.currentIndex);

            if ( this.currentIndex >= (this.collection.size() - 1) ) {
                this.currentIndex = 0;
            } else {
                this.currentIndex++;
            }

            return item;

        } finally {
            this.lock.unlock();
        }

    }

    @Override
    public Iterator<T> iterator() {
        return this.collection.iterator();
    }
}

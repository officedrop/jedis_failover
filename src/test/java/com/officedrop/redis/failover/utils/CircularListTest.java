package com.officedrop.redis.failover.utils;

import junit.framework.Assert;
import org.junit.Test;

/**
 * User: Maurício Linhares
 * Date: 12/21/12
 * Time: 12:44 PM
 */
public class CircularListTest {

    @Test
    public void testNext() {

        CircularList<String> items = new CircularList<String>( "1", "2", "3", "4");

        Assert.assertEquals("1", items.next());
        Assert.assertEquals("2", items.next());
        Assert.assertEquals("3", items.next());
        Assert.assertEquals("4", items.next());
        Assert.assertEquals("1", items.next());
    }

    @Test
    public void testIsEmptyIsFalse() {

        CircularList<String> items = new CircularList<String>("1");
        Assert.assertFalse(items.isEmpty());

    }

    @Test
    public void testIsEmptyIsTrue() {
        CircularList<String> items = new CircularList<String>();
        Assert.assertTrue(items.isEmpty());
    }

    @Test
    public void testGetSize() {

        CircularList<String> items = new CircularList<String>("me");

        Assert.assertEquals(1, items.getSize());

    }

}

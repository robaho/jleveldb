package com.robaho.jleveldb;

import junit.framework.TestCase;

public class UtilsTest extends TestCase {
    public void testGetSegmentID() {
        String s = "keys.1.2";
        assertEquals(1,Utils.getSegmentID(s));
        try {
            s = "keys";
            Utils.getSegmentID(s);
            fail("should have thrown exception");
        } catch (Exception e){}
    }
    public void testGetSegmentIDs() {
        String s = "keys.1.2";
        assertEquals(1,Utils.getSegmentIDs(s)[0]);
        assertEquals(2,Utils.getSegmentIDs(s)[1]);
        try {
            s = "keys";
            Utils.getSegmentIDs(s);
            fail("should have thrown exception");
        } catch (Exception e){}
    }
}

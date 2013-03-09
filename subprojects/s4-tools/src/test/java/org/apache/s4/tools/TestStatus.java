package org.apache.s4.tools;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.base.Strings;

public class TestStatus {

    @Test
    public void testStringFormatting() {

        String repeat = Strings.repeat("A", 20);
        String middle = Status.inMiddle(repeat, 20);
        Assert.assertTrue(middle.contains(repeat));
        Assert.assertTrue("Failed to parse correctly", true);

    }

}

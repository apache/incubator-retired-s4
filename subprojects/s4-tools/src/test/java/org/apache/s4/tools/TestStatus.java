package org.apache.s4.tools;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.base.Strings;

public class TestStatus {

    @Test
    public void testStringFormatting() {

        Status.inMiddle(Strings.repeat("A", 20), 20);
        Assert.assertTrue("Failed to parse correctly", true);

    }

}

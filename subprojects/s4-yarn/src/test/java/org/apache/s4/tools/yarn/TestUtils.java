package org.apache.s4.tools.yarn;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestUtils {

    @Test
    public void testUtils() {
        Assert.assertEquals(200,
                Utils.extractMemoryParam(10, ImmutableList.of(CommonS4YarnArgs.S4_NODE_JVM_PARAMETERS, "-Xmx200M")));
        Assert.assertEquals(200,
                Utils.extractMemoryParam(500, ImmutableList.of(CommonS4YarnArgs.S4_NODE_JVM_PARAMETERS, "-Xmx200M")));
        Assert.assertEquals(((int) (500 * Utils.CONTAINER_MEMORY_REDUCTION_FACTOR)),
                Utils.extractMemoryParam(500, ImmutableList.of(CommonS4YarnArgs.S4_NODE_JVM_PARAMETERS, "-Xms200M")));
    }

}

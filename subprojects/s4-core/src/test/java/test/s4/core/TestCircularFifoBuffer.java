package test.s4.core;

import org.apache.commons.collections15.buffer.CircularFifoBuffer;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestCircularFifoBuffer extends TestCase {

    protected void setUp() {

    }

    public void test1() {

        System.out.println("Buffer size is 10.\n");
        CircularFifoBuffer<Integer> circularBuffer = new CircularFifoBuffer<Integer>(
                10);

        System.out.println("Add ints 100-114.");
        for (int i = 0; i < 15; i++) {
            circularBuffer.add(i + 100);
        }

        System.out.println("Iterate.");
        int j = 5;
        for(Integer num : circularBuffer) {
            System.out.print(num + " ");
            Assert.assertEquals(j + 100, num.intValue());
            j++;
        }
        System.out.println("\nLeast recent value: " + circularBuffer.get());
        Assert.assertEquals(105, circularBuffer.get().intValue());
        System.out.println("\n");
        
        circularBuffer.clear();
        
        /* Less than max size. */
        System.out.println("Clear and add ints 200-204.");
        for (int i = 0; i < 5; i++) {
            circularBuffer.add(i + 200);
        }
        
        System.out.println("Iterate.");
        int z = 0;
        for(Integer num : circularBuffer) {
            System.out.print(num + " ");
            Assert.assertEquals(z + 200, num.intValue());
            z++;
        }
        System.out.println("\n");
    }
}

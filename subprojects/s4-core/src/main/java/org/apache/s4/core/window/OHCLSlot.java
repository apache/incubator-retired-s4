package org.apache.s4.core.window;

public class OHCLSlot implements Slot<Double> {

    double open = -1;
    double high = -1;
    double low = -1;
    double close = -1;
    long ticks = 0;
    boolean isOpen;

    @Override
    public void update(Double data) {
        if (isOpen) {
            if (open == -1) {
                open = low = high = close = data;
            } else if (data > high) {
                high = data;
            } else if (data < low) {
                low = data;
            }
            close = data;
            ticks++;
        }
    }

    @Override
    public void close() {
        isOpen = false;
    }

    double getOpen() {
        return open;
    }

    double getClose() {
        return close;
    }

    double getHigh() {
        return high;
    }

    double getLow() {
        return low;
    }

    long getTicksCount() {
        return ticks;
    }

    public static class OHCLSlotFactory implements SlotFactory<OHCLSlot> {

        @Override
        public OHCLSlot createSlot() {
            return new OHCLSlot();
        }

    }

}

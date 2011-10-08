package io.s4.comm;

import io.s4.base.Hasher;

public class DefaultHasher implements Hasher {

    @Override
    public long hash(String hashKey) {
        int b = 378551;
        int a = 63689;
        long hash = 0;

        for (int i = 0; i < hashKey.length(); i++) {
            hash = hash * a + hashKey.charAt(i);
            a = a * b;
        }

        return Math.abs(hash);
    }

}

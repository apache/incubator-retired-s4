package io.s4.core;

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

        return hash;
    }

}

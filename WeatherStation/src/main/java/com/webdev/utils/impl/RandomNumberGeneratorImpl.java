package com.webdev.utils.impl;

import com.webdev.utils.RandomNumberGenerator;

import java.util.concurrent.ThreadLocalRandom;

public class RandomNumberGeneratorImpl implements RandomNumberGenerator {

    @Override
    public int nextInt(int low, int high) {
        return ThreadLocalRandom.current().nextInt(low, high + 1);
    }

    @Override
    public double nextDouble (double low, double high)
    {
        return ThreadLocalRandom.current().nextDouble(low, high);
    }
}

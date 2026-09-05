package com.webdev.utils.impl;

import com.webdev.utils.Clock;

public class SystemClock implements Clock {

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}

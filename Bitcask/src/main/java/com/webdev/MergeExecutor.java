package com.webdev;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MergeExecutor {

    public static final ExecutorService mergeExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "bitcask-merge");
        t.setDaemon(true);
        return t;
    });
}

package com.webdev;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class RefCountFileChannel {

    private final int closing = -1;
    private final FileChannel fileChannel;



    private AtomicInteger refCount = new AtomicInteger(0);
    private volatile boolean retired = false;
    private final Path path;

    public RefCountFileChannel(FileChannel fileChannel, Path path) {
        this.fileChannel = fileChannel;
        this.path = path;
    }

    public boolean acquire () {
        while (true) {
            int current = refCount.get();

            if (current == closing) {
                return false;
            }

            if (refCount.compareAndSet(current, current + 1))
                return true;
        }
    }

    public void release () throws IOException {
        int current = refCount.decrementAndGet();
        if(current == 0 && retired)
        {
            tryCloseAndDelete();
        }
    }

    public void setRetired ()
    {
        retired = true;
        tryCloseAndDelete();
    }

    public int read (ByteBuffer buffer, long pos) throws IOException {
        return fileChannel.read(buffer, pos);
    }

    private void tryCloseAndDelete()
    {
        if(refCount.compareAndSet(0, closing))
        {
            try {
                fileChannel.close();
            } catch (IOException e) {

            }
        }

    }

    public void close () throws IOException {
        fileChannel.close();
    }

    public Path getPath() {
        return path;
    }
}

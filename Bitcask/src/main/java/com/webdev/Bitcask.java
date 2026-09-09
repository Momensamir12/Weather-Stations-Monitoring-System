package com.webdev;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

public class Bitcask implements AutoCloseable {

    private ConcurrentHashMap<ByteArrayKey, KeyDirEntry> keyDirectory;
    private final String BASE_FILE_PATH = "Bitcask/bitcask-files/";
    private final String BASE_FILE_NAME = ".data";
    private Path activeFilePath;
    private int fileSequenceNumber;
    private FileChannel writeactiveFileChannel;
    private FileChannel readActiveFileChannel;
    private long nextWriteOffset;
    private final long sizeThreshold = 512;
    private long lastForceMillis = System.currentTimeMillis();
    private static final long FORCE_TIME_THRESHOLD_MILLIS = 50;
    private final int keySize = 4;
    private final int valueSize = 4;


    private long lastWriteTimeStamp;

    public Bitcask() throws IOException {
        keyDirectory = new ConcurrentHashMap<>();
        fileSequenceNumber = 0;
        openNewFileChannel();
    }

    public void put(byte[] key, byte[] value) throws IOException {
        int recordSize = keySize + valueSize + key.length + value.length;
        long valueByteOffset = nextWriteOffset + keySize + valueSize + key.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putInt(key.length);
        buffer.putInt(value.length);
        buffer.put(key);
        buffer.put(value);

        buffer.flip();

        while (buffer.hasRemaining()) {
            int write = writeactiveFileChannel.write(buffer);
        }
        ByteArrayKey byteArrayKey = new ByteArrayKey(key);
        KeyDirEntry entry = new KeyDirEntry();
        entry.filePath = activeFilePath;
        entry.valueOffset = valueByteOffset;
        entry.valueSize = value.length;
        nextWriteOffset += recordSize;
        keyDirectory.put(byteArrayKey, entry);

        forcePolicy();
        sizePolicy();
    }

    public byte[] get(byte[] key) throws IOException {
        ByteArrayKey byteArrayKey = new ByteArrayKey(key);
        KeyDirEntry entry = keyDirectory.get(byteArrayKey);

        int size = entry.valueSize;
        long pos = entry.valueOffset;

        ByteBuffer buffer = ByteBuffer.allocate(size);
        int total = 0;
        while (total < size) {
            int n = readActiveFileChannel.read(buffer, pos + total);

            if (n == -1)
                throw new EOFException("End of file reached");
            total += n;
        }

        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        return bytes;
    }

    private void openNewFileChannel() throws IOException {

        if (writeactiveFileChannel != null && writeactiveFileChannel.isOpen())
            writeactiveFileChannel.close();

        if (readActiveFileChannel != null && readActiveFileChannel.isOpen())
            readActiveFileChannel.close();

        fileSequenceNumber++;
        activeFilePath = Path.of(BASE_FILE_PATH + fileSequenceNumber + BASE_FILE_NAME);
        writeactiveFileChannel = FileChannel.open(activeFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        readActiveFileChannel = FileChannel.open(activeFilePath,
                StandardOpenOption.READ);

        nextWriteOffset = writeactiveFileChannel.size();
    }

    @Override
    public void close() throws Exception {

        writeactiveFileChannel.close();
        readActiveFileChannel.close();
    }

    void sizePolicy () throws IOException {
        if(nextWriteOffset >= sizeThreshold)
            openNewFileChannel();
    }

    void forcePolicy() throws IOException {
        if(System.currentTimeMillis() - lastForceMillis >= FORCE_TIME_THRESHOLD_MILLIS)
        {
            writeactiveFileChannel.force(false);
            lastForceMillis = System.currentTimeMillis();
        }
    }
}

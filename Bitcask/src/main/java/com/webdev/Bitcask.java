package com.webdev;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class Bitcask implements AutoCloseable {

    private static final long FORCE_TIME_THRESHOLD_MILLIS = 50;
    private final ConcurrentHashMap<Integer, FileChannel> readFileChannels;
    private final String BASE_FILE_PATH = "Bitcask/bitcask-files/";
    private final String BASE_FILE_NAME = ".data";
    private final long sizeThreshold = 512;
    private final int keySizeBytes = 4;
    private final int valueSizeBytes = 4;
    private final ConcurrentHashMap<ByteArrayKey, KeyDirEntry> keyDirectory;
    private int fileSequenceNumber;
    private FileChannel writeactiveFileChannel;
    private long nextWriteOffset;
    private long lastForceMillis = System.currentTimeMillis();

    public Bitcask() throws IOException {
        keyDirectory = new ConcurrentHashMap<>();
        readFileChannels = new ConcurrentHashMap<>();
        fileSequenceNumber = 0;
        buildKeyDirectory();
        setupActiveFileChannel();
    }

    public void put(byte[] key, byte[] value) throws IOException {
        int recordSize = keySizeBytes + valueSizeBytes + key.length + value.length;
        long valueByteOffset = nextWriteOffset + keySizeBytes + valueSizeBytes + key.length;
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
        entry.fileId = fileSequenceNumber;
        entry.valueOffset = valueByteOffset;
        entry.valueSize = value.length;
        nextWriteOffset += recordSize;
        keyDirectory.put(byteArrayKey, entry);

        forcePolicy();
        sizePolicy();
    }

    public byte[] get(byte[] key) throws IOException {

        ByteArrayKey byteArrayKey = new ByteArrayKey(key);
        if (!keyDirectory.containsKey(byteArrayKey))
            return null;

        KeyDirEntry entry = keyDirectory.get(byteArrayKey);
        FileChannel channel = getFileChannel(entry.fileId);

        int size = entry.valueSize;
        long pos = entry.valueOffset;

        ByteBuffer buffer = ByteBuffer.allocate(size);
        int total = 0;
        while (total < size) {
            int n = channel.read(buffer, pos + total);

            if (n == -1)
                throw new EOFException("End of file reached");
            total += n;
        }

        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        return bytes;
    }

    private void openNewActiveFileChannel() throws IOException {

        if (writeactiveFileChannel != null && writeactiveFileChannel.isOpen())
            writeactiveFileChannel.close();

        fileSequenceNumber++;
        writeactiveFileChannel = FileChannel.open(pathForFileId(fileSequenceNumber),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        nextWriteOffset = writeactiveFileChannel.size();
    }

    @Override
    public void close() throws Exception {

        writeactiveFileChannel.close();
        readFileChannels.values().forEach(channel -> {
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    boolean sizePolicy() throws IOException {
        if (nextWriteOffset >= sizeThreshold){
            openNewActiveFileChannel();
            return true;
        }
        return false;
    }

    boolean forcePolicy() throws IOException {
        if (System.currentTimeMillis() - lastForceMillis >= FORCE_TIME_THRESHOLD_MILLIS) {
            writeactiveFileChannel.force(false);
            lastForceMillis = System.currentTimeMillis();

            return true;
        }
        return false;
    }

    Path pathForFileId(int id) {
        return Path.of(BASE_FILE_PATH + id + BASE_FILE_NAME);
    }

    FileChannel getFileChannel(int fileId) {
        return readFileChannels.computeIfAbsent(fileId, id -> {
            try {
                return FileChannel.open(pathForFileId(id), StandardOpenOption.READ);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    void setupActiveFileChannel() throws IOException {
        try {
            Path path = scanForActiveFile();
            if (path != null) {
                writeactiveFileChannel = FileChannel.open(path,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND);

                if(sizePolicy())
                    return;

                nextWriteOffset = writeactiveFileChannel.size();
                return;
            }
            openNewActiveFileChannel();

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Path scanForActiveFile() throws IOException {
        Path folderPath = Path.of(BASE_FILE_PATH);
        AtomicBoolean foundFile = new AtomicBoolean(false);

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .map(name -> {
                        int dot = name.lastIndexOf('.');
                        return (dot > 0) ? name.substring(0, dot) : name;
                    }).forEach(name -> {
                        int seq = Integer.parseInt(name);
                        fileSequenceNumber = Math.max(fileSequenceNumber, seq);
                        foundFile.set(true);
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (foundFile.get())
            return pathForFileId(fileSequenceNumber);

        return null;
    }

    void buildKeyDirectory () throws IOException {
        Path folderPath = Path.of(BASE_FILE_PATH);
        try (Stream<Path> stream = Files.walk(folderPath))
        {
            stream.filter(Files::isRegularFile)
                    .sorted(Comparator.comparingInt(this::filIdFromPath))
                    .forEach(path -> {
                        try {
                            scanKeysFromFile(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }

                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    int filIdFromPath (Path path)
    {
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');

        int fileId = Integer.parseInt(name.substring(0, dot));

        return fileId;
    }

    void scanKeysFromFile (Path path) throws IOException {

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ))
        {
            int fileId = filIdFromPath(path);
            long bytesRead = 0;
            boolean EOF = false;
            int keyValSize = keySizeBytes + valueSizeBytes;

            while (!EOF)
           {
               ByteBuffer keyValueSizeBuffer = ByteBuffer.allocate(keyValSize);
               int keyValSizeTotal = 0;

               while(keyValSizeTotal < keyValSize)
               {
                   int read = channel.read(keyValueSizeBuffer, bytesRead);
                   if(read == -1)
                   {
                       EOF = true;
                       break;
                   }
                   keyValSizeTotal += read;
                   bytesRead += read;
               }
               if(EOF)
                   break;

               keyValueSizeBuffer.flip();
               int keySize = keyValueSizeBuffer.getInt();
               int valueSize = keyValueSizeBuffer.getInt();
               int keyTotal = 0;

               ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
               while(keyTotal < keySize)
               {
                   int read = channel.read(keyBuffer, bytesRead);
                   if(read == -1)
                   {
                       EOF = true;
                       break;
                   }
                   keyTotal += read;
                   bytesRead += read;
               }

               if(EOF)
                   break;

               keyBuffer.flip();

               byte [] key =  new byte[keySize];
               keyBuffer.get(key);
               long valueOffset = bytesRead;

               KeyDirEntry entry = new KeyDirEntry();
               entry.fileId = fileId;
               entry.valueSize = valueSize;
               entry.valueOffset = valueOffset;

               ByteArrayKey byteArrayKey = new ByteArrayKey(key);

               keyDirectory.put(byteArrayKey, entry);
               bytesRead += valueSize;
           }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}

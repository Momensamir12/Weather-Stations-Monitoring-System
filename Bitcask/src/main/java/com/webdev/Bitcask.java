package com.webdev;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class Bitcask implements AutoCloseable {

    private static final long FORCE_TIME_THRESHOLD_MILLIS = 50;
    private static final long NUMBER_OF_FILES_THRESHOLD = 30;
    private final ConcurrentHashMap<Integer, FileChannel> readFileChannels;
    private final String BASE_FILE_PATH = "Bitcask/bitcask-files/";
    private final String BASE_FILE_NAME = ".data";
    private final String BASE_MERGE_FILE_NAME = ".tmp";
    private final String BASE_DELETE_FILE_NAME = ".odata";
    private final long sizeThreshold = 512;
    private final int keySizeBytes = 4;
    private final int valueSizeBytes = 4;
    private final ConcurrentHashMap<ByteArrayKey, KeyDirEntry> keyDirectory;
    private int fileSequenceNumber;
    private FileChannel writeactiveFileChannel;
    private Path writeactiveFilePath;
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
        KeyDirEntry entry = new KeyDirEntry(fileSequenceNumber, valueByteOffset, value.length);
        nextWriteOffset += recordSize;
        keyDirectory.put(byteArrayKey, entry);

        forcePolicy();
        if(sizePolicy())
            openNewActiveFileChannel();
    }

    public int putInFile(byte[] key, byte[] value, long offset, FileChannel fileChannel) throws IOException {
        int recordSize = keySizeBytes + valueSizeBytes + key.length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putInt(key.length);
        buffer.putInt(value.length);
        buffer.put(key);
        buffer.put(value);

        buffer.flip();

        while (buffer.hasRemaining()) {
            int write = fileChannel.write(buffer, offset);
        }

        return recordSize;
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
        writeactiveFileChannel = FileChannel.open(pathForDataFileId(fileSequenceNumber),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        writeactiveFilePath = pathForDataFileId(fileSequenceNumber);

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

    Path pathForDataFileId(int id) {
        return Path.of(BASE_FILE_PATH + id + BASE_FILE_NAME);
    }

    Path pathForMergeFileId (int id)
    {
        return Path.of(BASE_FILE_PATH + id + BASE_MERGE_FILE_NAME);
    }

    FileChannel getFileChannel(int fileId) {
        return readFileChannels.computeIfAbsent(fileId, id -> {
            try {
                return FileChannel.open(pathForDataFileId(id), StandardOpenOption.READ);
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
                writeactiveFilePath = path;

                if(sizePolicy())
                {
                    openNewActiveFileChannel();
                    return;
                }

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
                    .filter(path -> path.getFileName().toString().endsWith(BASE_FILE_NAME))
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
            return pathForDataFileId(fileSequenceNumber);

        return null;
    }

    void buildKeyDirectory() {
        Path folderPath = Path.of(BASE_FILE_PATH);
        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(BASE_FILE_NAME))
                    .sorted(Comparator.comparingInt(this::fileIdFromPath))
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

    void deleteMergedDataFiles() {
        Path folderPath = Path.of(BASE_FILE_PATH);
        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(BASE_DELETE_FILE_NAME))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    int fileIdFromPath (Path path)
    {
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');

        int fileId = Integer.parseInt(name.substring(0, dot));

        return fileId;
    }

    void scanKeysFromFile (Path path) throws IOException {

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ))
        {
            int fileId = fileIdFromPath(path);
            long bytesRead = 0;
            int keyValSize = keySizeBytes + valueSizeBytes;

            while (true)
           {
               ByteBuffer keyValueSizeBuffer = ByteBuffer.allocate(keyValSize);
               int keyValSizeTotal = 0;

               while(keyValSizeTotal < keyValSize)
               {
                   int read = channel.read(keyValueSizeBuffer, bytesRead);
                   if(read == -1)
                   {
                       return;
                   }
                   keyValSizeTotal += read;
                   bytesRead += read;
               }

               keyValueSizeBuffer.flip();
               int keySize = keyValueSizeBuffer.getInt();
               int valueSize = keyValueSizeBuffer.getInt();
               int keyTotal = 0;

               ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
               while(keyTotal < keySize)
               {
                   int read = channel.read(keyBuffer, bytesRead);
                   keyTotal += read;
                   bytesRead += read;
               }

               keyBuffer.flip();

               byte [] key =  new byte[keySize];
               keyBuffer.get(key);
               long valueOffset = bytesRead;

               KeyDirEntry entry = new KeyDirEntry(fileId, valueOffset, valueSize);


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

    public boolean mergeCompactionPolicy ()
    {
        Path folderPath = Path.of(BASE_FILE_PATH);
        try (Stream<Path> stream = Files.walk(folderPath))
        {
            long fileCount = stream.filter(Files::isRegularFile)
                    .count();

            return fileCount >= NUMBER_OF_FILES_THRESHOLD;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void merge() {
        Path folderPath = Path.of(BASE_FILE_PATH);

        try {
            int mergeFileSequence = 1;
            Path mergeFilePath = pathForMergeFileId(mergeFileSequence);

            FileChannel currentMergeFile = FileChannel.open(
                    mergeFilePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
            );

            HashMap<ByteArrayKey, TempKeyDirEntry> tempKeyDirectory = new HashMap<>();
            long mergeFileOffset = 0;

            List<Path> filesToMerge;
            try (Stream<Path> stream = Files.walk(folderPath)) {
                filesToMerge = stream
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(BASE_FILE_NAME))
                        .filter(path -> !path.equals(writeactiveFilePath))
                        .sorted(Comparator.comparingInt(this::fileIdFromPath))
                        .toList();
            }

            for (Path path : filesToMerge) {
                long bytesWritten;
                try {
                    bytesWritten = scanKeysFromFile(
                            path, currentMergeFile, mergeFileOffset, tempKeyDirectory, mergeFileSequence
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

                mergeFileOffset += bytesWritten;

                if (mergeFileOffset >= sizeThreshold) {
                    currentMergeFile.close();
                    mergeFileSequence++;
                    mergeFilePath = pathForMergeFileId(mergeFileSequence);
                    currentMergeFile = FileChannel.open(
                            mergeFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
                    );
                    mergeFileOffset = 0;
                }
            }

            currentMergeFile.close();

            final int liveMergeCount = mergeFileSequence; // freeze for use below

            setMergeFilesAsLive(); // K.tmp -> K.data, K in [1, liveMergeCount]; overwrites old files with those same ids

            for (ByteArrayKey key : tempKeyDirectory.keySet()) {
                if (tempKeyDirectory.containsKey(key) && keyDirectory.containsKey(key)) {
                    TempKeyDirEntry tempKeyDirEntry = tempKeyDirectory.get(key);
                    KeyDirEntry keyDirEntry = keyDirectory.get(key);
                    if (tempKeyDirEntry.oldFileId == keyDirEntry.fileId) {
                        KeyDirEntry newEntry = new KeyDirEntry(
                                tempKeyDirEntry.fileId, tempKeyDirEntry.valueOffset, tempKeyDirEntry.valueSize
                        );
                        keyDirectory.replace(key, keyDirEntry, newEntry);
                    }
                }
            }

            for (Path path : filesToMerge) {
                FileChannel old = readFileChannels.remove(fileIdFromPath(path));
                if (old != null) old.close();
            }


            for (Path path : filesToMerge) {
                int id = fileIdFromPath(path);
                if (id > liveMergeCount) {
                    Files.move(path, pathForOldDataFileId(id), StandardCopyOption.REPLACE_EXISTING);
                }
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void setMergeFilesAsLive() {
        Path folderPath = Path.of(BASE_FILE_PATH);
        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(BASE_MERGE_FILE_NAME))
                    .forEach(path -> {
                        try {
                            int seq = fileIdFromPath(path);
                            Files.move(path, pathForDataFileId(seq), StandardCopyOption.REPLACE_EXISTING);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    long scanKeysFromFile (Path path, FileChannel mergeFileChannel, long mergeFileOffset,
                           HashMap<ByteArrayKey, TempKeyDirEntry> tempKeyDirectory, int mergeFileSequence) throws IOException {

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ))
        {
            int fileId = fileIdFromPath(path);
            long bytesRead = 0;
            long bytesWritten = 0;
            int keyValSize = keySizeBytes + valueSizeBytes;

            while (true)
            {
                ByteBuffer keyValueSizeBuffer = ByteBuffer.allocate(keyValSize); // read size of key and value
                int keyValSizeTotal = 0;

                while(keyValSizeTotal < keyValSize)
                {
                    int read = channel.read(keyValueSizeBuffer, bytesRead);
                    if(read == -1)
                    {
                        return bytesWritten;
                    }
                    keyValSizeTotal += read;
                    bytesRead += read;
                }

                keyValueSizeBuffer.flip();
                int keySize = keyValueSizeBuffer.getInt();
                int valueSize = keyValueSizeBuffer.getInt();
                int keyTotal = 0;

                ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);          // read key
                while(keyTotal < keySize)
                {
                    int read = channel.read(keyBuffer, bytesRead);
                    keyTotal += read;
                    bytesRead += read;
                }

                keyBuffer.flip();

                byte [] key =  new byte[keySize];
                keyBuffer.get(key);
                long valueOffset = bytesRead;
                ByteArrayKey byteArrayKey = new ByteArrayKey(key);        // create entry for key directory hash table

                long liveValueOffset = keyDirectory.get(byteArrayKey).valueOffset;
                if(valueOffset != liveValueOffset)                       // if the offset doesn't match live key directory offset , skip the key
                {
                    bytesRead += valueSize;
                    continue;
                }

                int valueTotal = 0;
                ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize); // read the value
                while(valueTotal < valueSize)
                {
                    int read = channel.read(valueBuffer, bytesRead);
                    valueTotal += read;
                    bytesRead += read;
                }
                byte [] value = new byte[valueSize];
                valueBuffer.get(value);
                long mergeFileValueOffset = mergeFileOffset + keySizeBytes + valueSizeBytes + keySize; // calculate value offset in the merged file
                int recordSize = putInFile(key, value, mergeFileOffset, mergeFileChannel);              // put the record in the merge file

                mergeFileOffset += recordSize;
                bytesWritten += recordSize;

                int oldFileId = keyDirectory.get(byteArrayKey).fileId;
                TempKeyDirEntry entry = new TempKeyDirEntry(mergeFileSequence, oldFileId, mergeFileValueOffset, value.length);

                tempKeyDirectory.put(byteArrayKey, entry);                                          // put the entry in the merge file
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    Path pathForOldDataFileId(int id) {
        return Path.of(BASE_FILE_PATH + id + BASE_DELETE_FILE_NAME);
    }
}


package com.webdev;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class Bitcask implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Bitcask.class);
    private static final long FORCE_TIME_THRESHOLD_MILLIS = 50;
    private static final long NUMBER_OF_FILES_THRESHOLD = 10;
    private final ConcurrentHashMap<Integer, RefCountFileChannel> readFileChannels;
    private final Path baseDir;
    private final String BASE_FILE_NAME = ".data";
    private final String BASE_MERGE_FILE_NAME = ".tmp";
    private final String BASE_DELETE_FILE_NAME = ".odata";
    private final String BASE_HINT_FILE_NAME = ".hint";
    private final long sizeThreshold = 1024 * 1024;
    private final int keySizeBytes = 4;
    private final int valueSizeBytes = 4;
    private final ConcurrentHashMap<ByteArrayKey, KeyDirEntry> keyDirectory;
    private AtomicInteger fileSequenceNumber = new AtomicInteger(0);
    private FileChannel writeactiveFileChannel;
    private volatile Path writeactiveFilePath;
    private long nextWriteOffset;
    private long lastForceMillis = System.currentTimeMillis();
    private AtomicBoolean mergeInProgress = new AtomicBoolean(false);
    private static final long GARBAGE_THRESHOLD_BYTES = 64L * 1024;
    private final AtomicLong deadBytesSinceLastMerge = new AtomicLong(0);

    public Bitcask(String baseDir) throws IOException {
        keyDirectory = new ConcurrentHashMap<>();
        readFileChannels = new ConcurrentHashMap<>();
        this.baseDir = Path.of(baseDir);

        Files.createDirectories(this.baseDir);
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
        KeyDirEntry entry = new KeyDirEntry(fileSequenceNumber.get(), valueByteOffset, value.length);
        nextWriteOffset += recordSize;
        KeyDirEntry previous = keyDirectory.put(byteArrayKey, entry);
        if (previous != null) {
            // this key already existed - the old on-disk record is now garbage
            int deadRecordSize = keySizeBytes + valueSizeBytes + key.length + previous.valueSize;
            deadBytesSinceLastMerge.addAndGet(deadRecordSize);
        }
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
        final int maxRetries = 5;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            KeyDirEntry entry = keyDirectory.get(byteArrayKey);
            if (entry == null) {
                return null;
            }

            RefCountFileChannel channel;
            try {
                channel = getReadFileChannel(entry.fileId);
            } catch (NoSuchFileException e) {
                // file was retired and renamed away between our keydir read and
                // our open attempt — keydir has since been updated, retry against it
                continue;
            }

            if (!channel.acquire()) {
                continue;
            }

            try {
                int size = entry.valueSize;
                long pos = entry.valueOffset;

                ByteBuffer buffer = ByteBuffer.allocate(size);
                int total = 0;
                while (total < size) {
                    int n = channel.read(buffer, pos + total);
                    if (n == -1)
                        throw new EOFException("End of file reached, fileId=" + entry.fileId);
                    total += n;
                }

                buffer.flip();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return bytes;

            } catch (ClosedChannelException e) {
                throw new IOException("Read failed, channel closed unexpectedly for fileId=" + entry.fileId, e);

            } finally {
                channel.release();
            }
        }

        throw new IOException("Exceeded retry limit (" + maxRetries + ")");
    }

    private void openNewActiveFileChannel() throws IOException {


        if (writeactiveFileChannel != null && writeactiveFileChannel.isOpen())
            writeactiveFileChannel.close();

        int sequence = fileSequenceNumber.incrementAndGet();
        writeactiveFileChannel = FileChannel.open(pathForDataFileId(sequence),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        writeactiveFilePath = pathForDataFileId(sequence);

        nextWriteOffset = writeactiveFileChannel.size();
        if(mergeCompactionPolicy() && mergeInProgress.compareAndSet(false,true))
        {
            MergeExecutor.mergeExecutor.submit(
                    ()->{
                        try {
                            merge();
                        } catch (Exception e) {
                            log.error("Background merge failed", e);
                        }
                        finally {
                            mergeInProgress.set(false);
                        }
                    }
            );
        }
    }

    @Override
    public void close() throws Exception {
        List<Exception> failures = new ArrayList<>();
        try {
            writeactiveFileChannel.close();
        } catch (IOException e) {
            failures.add(e);
        }
        for (RefCountFileChannel channel : readFileChannels.values()) {
            try {
                channel.close();
            } catch (IOException e) {
                log.warn("Failed to close read channel for path {}", channel.getPath(), e);
                failures.add(e);
            }
        }
        if (!failures.isEmpty()) {
            Exception first = failures.get(0);
            failures.subList(1, failures.size()).forEach(first::addSuppressed);
            throw first;
        }
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

    RefCountFileChannel getReadFileChannel(int fileId) throws IOException {

        if(readFileChannels.containsKey(fileId))
            return readFileChannels.get(fileId);

        Path path = pathForDataFileId(fileId);
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        RefCountFileChannel refCountFileChannel = new RefCountFileChannel(channel, path);

        readFileChannels.put(fileId, refCountFileChannel);

        return refCountFileChannel;
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

                // nextWriteOffset has to be known *before* sizePolicy() can mean anything -
                // previously this was checked while nextWriteOffset was still its default 0.
                nextWriteOffset = writeactiveFileChannel.size();

                if (sizePolicy()) {
                    openNewActiveFileChannel();
                }
                return;
            }
            openNewActiveFileChannel();

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Path scanForActiveFile() throws IOException {
        int activeCandidate = -1; // highest sequence among files WITHOUT a hint file

        try (Stream<Path> stream = Files.walk(baseDir)) {
            List<Path> dataFiles = stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(BASE_FILE_NAME))
                    .toList();

            for (Path path : dataFiles) {
                int seq = fileIdFromPath(path);

                if (seq > fileSequenceNumber.get()) {
                    fileSequenceNumber.set(seq);
                }

                if (!Files.exists(pathForHintFileId(seq)) && seq > activeCandidate) {
                    activeCandidate = seq;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (activeCandidate >= 0)
            return pathForDataFileId(activeCandidate);

        return null;
    }

    void buildKeyDirectory() {

        Set<Integer> processedFiles = new HashSet<>();

        try (Stream<Path> stream = Files.walk(baseDir)) {
            stream.filter(Files::isRegularFile)
                    .sorted(Comparator.comparingInt(this::fileIdFromPath))
                    .forEach(path -> {
                        try {
                            String fileName = path.getFileName().toString();
                            int dot = fileName.indexOf('.');
                            int sequence = Integer.parseInt(fileName.substring(0, dot));

                            if(processedFiles.contains(sequence))
                            {
                                return;
                            }

                            Path hintFilePath = pathForHintFileId(sequence);
                            if(Files.exists(hintFilePath))
                            {
                                scanKeysFromHintFile(hintFilePath);
                            }

                            else
                            {
                                scanKeysFromDataFile(path);
                            }
                            processedFiles.add(sequence);

                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void deleteMergedDataFiles() {

        try (Stream<Path> stream = Files.walk(baseDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(BASE_DELETE_FILE_NAME))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
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

    void scanKeysFromHintFile (Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ))
        {
            int fileId = fileIdFromPath(path);
            long bytesRead = 0;
            int keyValOffsetSize = keySizeBytes + valueSizeBytes + 8;
            long fileSize = channel.size();

            while(true)
            {
                ByteBuffer keyValSizeOffsetBuffer = ByteBuffer.allocate(keyValOffsetSize);

                int keyValOffsetTotal = 0;

                while(keyValOffsetTotal < keyValOffsetSize)
                {
                    int read = channel.read(keyValSizeOffsetBuffer, bytesRead);
                    if(read == -1)
                        return;

                    keyValOffsetTotal += read;
                    bytesRead += read;
                }

                keyValSizeOffsetBuffer.flip();
                int keySize = keyValSizeOffsetBuffer.getInt();
                int valueSize = keyValSizeOffsetBuffer.getInt();
                long valueOffset = keyValSizeOffsetBuffer.getLong();
                ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);

                byte[] key = new byte[keySize];
                int keyTotal = 0;

                while(keyTotal < keySize)
                {
                    int read = channel.read(keyBuffer, bytesRead);
                    bytesRead += read;
                    keyTotal += read;
                }

                keyBuffer.flip();
                keyBuffer.get(key);

                KeyDirEntry entry = new KeyDirEntry(fileId, valueOffset, valueSize);
                ByteArrayKey byteArrayKey = new ByteArrayKey(key);

                keyDirectory.put(byteArrayKey, entry);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void scanKeysFromDataFile (Path path) throws IOException {

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ))
        {
            int fileId = fileIdFromPath(path);
            long bytesRead = 0;
            int keyValSize = keySizeBytes + valueSizeBytes;
            long fileSize = channel.size();

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
                    if (read == -1)
                    {
                        return;
                    }
                    keyTotal += read;
                    bytesRead += read;
                }

                keyBuffer.flip();

                byte [] key =  new byte[keySize];
                keyBuffer.get(key);
                long valueOffset = bytesRead;

                if (valueOffset + valueSize > fileSize)
                {
                    // the value itself was never fully written (torn tail record) -
                    // don't register a key dir entry pointing at bytes that don't exist
                    return;
                }

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
        if (deadBytesSinceLastMerge.get() >= GARBAGE_THRESHOLD_BYTES) {
            return true;
        }

        try (Stream<Path> stream = Files.walk(baseDir))
        {
            long fileCount = stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(BASE_FILE_NAME))
                    .count();

            return fileCount >= NUMBER_OF_FILES_THRESHOLD;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void merge() {
        try {

            log.info("merge thread started");
            int mergeFileSequence = fileSequenceNumber.incrementAndGet();
            Path mergeFilePath = pathForMergeFileId(mergeFileSequence);
            Path hintFilePath = pathForHintFileId(mergeFileSequence);

            FileChannel currentMergeFile = FileChannel.open(
                    mergeFilePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
            );

            FileChannel currentHintFile = FileChannel.open(
                    hintFilePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);

            HashMap<ByteArrayKey, TempKeyDirEntry> tempKeyDirectory = new HashMap<>();
            long mergeFileOffset = 0;

            List<Path> filesToMerge;
            try (Stream<Path> stream = Files.walk(baseDir)) {
                filesToMerge = stream
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(BASE_FILE_NAME))
                        .filter(path -> !path.equals(writeactiveFilePath))
                        .sorted(Comparator.comparingInt(this::fileIdFromPath))
                        .toList();
            }

            try {
                for (Path path : filesToMerge) {
                    long bytesWritten;
                    try {
                        bytesWritten = buildMergeFile(
                                path, currentMergeFile, mergeFileOffset, tempKeyDirectory, mergeFileSequence,
                                currentHintFile);

                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    mergeFileOffset += bytesWritten;

                    if (mergeFileOffset >= sizeThreshold) {

                        currentMergeFile.force(false);
                        currentMergeFile.close();

                        mergeFileSequence = fileSequenceNumber.incrementAndGet();
                        mergeFilePath = pathForMergeFileId(mergeFileSequence);
                        currentMergeFile = FileChannel.open(
                                mergeFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
                        );

                        currentHintFile.force(false);
                        currentHintFile.close();

                        hintFilePath = pathForHintFileId(mergeFileSequence);
                        currentHintFile = FileChannel.open(hintFilePath, StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE, StandardOpenOption.APPEND);

                        mergeFileOffset = 0;
                    }
                }
            } finally {
                currentMergeFile.force(false);
                currentMergeFile.close();

                currentHintFile.force(false);
                currentHintFile.close();
            }

            if (mergeFileOffset == 0) {

                Files.deleteIfExists(mergeFilePath);
                Files.deleteIfExists(hintFilePath);
            }

            setMergeFilesAsLive();

            for (ByteArrayKey key : tempKeyDirectory.keySet()) {
                TempKeyDirEntry tempKeyDirEntry = tempKeyDirectory.get(key);
                KeyDirEntry keyDirEntry = keyDirectory.get(key);
                if (keyDirEntry != null && tempKeyDirEntry.oldFileId == keyDirEntry.fileId) {
                    KeyDirEntry newEntry = new KeyDirEntry(
                            tempKeyDirEntry.fileId, tempKeyDirEntry.valueOffset, tempKeyDirEntry.valueSize
                    );
                    keyDirectory.replace(key, keyDirEntry, newEntry);
                }
            }

            for (Path path : filesToMerge) {
                RefCountFileChannel old = readFileChannels.remove(fileIdFromPath(path));
                if (old != null) old.setRetired();
            }

            for (Path path : filesToMerge) {
                int id = fileIdFromPath(path);
                Files.move(path, pathForOldDataFileId(id), StandardCopyOption.REPLACE_EXISTING);
            }

            deleteMergedDataFiles();
            deadBytesSinceLastMerge.set(0);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    long buildMergeFile (Path path, FileChannel mergeFileChannel, long mergeFileOffset,
                         HashMap<ByteArrayKey, TempKeyDirEntry> tempKeyDirectory, int mergeFileSequence,
                         FileChannel hintFileChannel) throws IOException {

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
                    if (read == -1)
                    {
                        // truncated key at the tail of the file (torn write) - stop scanning
                        return bytesWritten;
                    }
                    keyTotal += read;
                    bytesRead += read;
                }

                keyBuffer.flip();

                byte [] key =  new byte[keySize];
                keyBuffer.get(key);
                long valueOffset = bytesRead;
                ByteArrayKey byteArrayKey = new ByteArrayKey(key);        // create entry for key directory hash table

                KeyDirEntry liveEntry = keyDirectory.get(byteArrayKey);
                if (liveEntry == null || liveEntry.fileId != fileId || valueOffset != liveEntry.valueOffset)  // no longer live, or offset doesn't match live key directory offset - skip the key
                {
                    bytesRead += valueSize;
                    continue;
                }

                int valueTotal = 0;
                ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize); // read the value
                while(valueTotal < valueSize)
                {
                    int read = channel.read(valueBuffer, bytesRead);
                    if (read == -1)
                    {
                        // truncated value at the tail of the file (torn write) - stop scanning
                        return bytesWritten;
                    }
                    valueTotal += read;
                    bytesRead += read;
                }
                byte [] value = new byte[valueSize];
                valueBuffer.flip();
                valueBuffer.get(value);
                long mergeFileValueOffset = mergeFileOffset + keySizeBytes + valueSizeBytes + keySize; // calculate value offset in the merged file
                int recordSize = putInFile(key, value, mergeFileOffset, mergeFileChannel);              // put the record in the merge file

                addEntryToHintFile(keySize, valueSize, mergeFileValueOffset, key, hintFileChannel);
                mergeFileOffset += recordSize;
                bytesWritten += recordSize;

                int oldFileId = liveEntry.fileId;
                TempKeyDirEntry entry = new TempKeyDirEntry(mergeFileSequence, oldFileId, mergeFileValueOffset, value.length);

                tempKeyDirectory.put(byteArrayKey, entry);                                          // put the entry in the merge file
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    void setMergeFilesAsLive() {
        try (Stream<Path> stream = Files.walk(baseDir)) {
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
            log.error("failed set temporary merge file as live", e);
            throw new UncheckedIOException(e);
        }
    }

    public List<KeyValue> getAllKeysValues () throws IOException {
        List<KeyValue> keysValues = new ArrayList<>();

        for(ByteArrayKey key : keyDirectory.keySet())
        {
            byte[] value = get(key.getKey());
            KeyValue keyValue = new KeyValue(key.getKey(), value);

            keysValues.add(keyValue);
        }

        return keysValues;
    }

    void addEntryToHintFile (int keySize, int valueSize, long valueOffset, byte [] key, FileChannel hintFileChannel) throws IOException {
        int recordSize = keySizeBytes + valueSizeBytes + 8 + key.length;
        ByteBuffer record = ByteBuffer.allocate(recordSize);

        record.putInt(keySize);
        record.putInt(valueSize);
        record.putLong(valueOffset);
        record.put(key);

        record.flip();

        while(record.hasRemaining())
        {
            hintFileChannel.write(record);
        }
    }

    Path pathForOldDataFileId(int id) {
        return baseDir.resolve(id + BASE_DELETE_FILE_NAME);
    }

    Path pathForDataFileId(int id) {
        return baseDir.resolve(id + BASE_FILE_NAME);
    }

    Path pathForMergeFileId(int id) {
        return baseDir.resolve(id + BASE_MERGE_FILE_NAME);
    }

    Path pathForHintFileId(int id) {
        return baseDir.resolve(id + BASE_HINT_FILE_NAME);
    }
}
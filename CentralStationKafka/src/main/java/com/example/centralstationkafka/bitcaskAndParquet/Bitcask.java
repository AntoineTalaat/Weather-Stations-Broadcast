package com.example.centralstationkafka.bitcaskAndParquet;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Bitcask {
    private String directoryPath;
    private RandomAccessFile activeWritingFile;
    private Hashtable<ByteArrayWrapper, KeyDirEntry> keyDir; // TODO
    private final long offset_limit = 10000; // roughly max file size 10 kB
    private final AtomicInteger currentDataFiles = new AtomicInteger(0);
    private final long MAX_DATA_FILES = 5;
    private final ArrayDeque<Thread> compactThreads = new ArrayDeque<>();

    /**
     * open new Or existing bitcask datastore
     * creating is trivial.
     * opening will require that I save the path string
     * such that when accessing the bitcask we can concatenate it
     *
     * @param directoryPath the directory to keep data of bitcask in
     */
    public void open(String directoryPath) throws IOException {
        this.directoryPath = directoryPath;
        this.openBitcaskFoldersCreatedOrCreateNew();
    }


    /***
     * This function creates the bitcask parent folder in the directory path that was previously set by open()
     *
     * it modifies the directory path from "dir" to "dir/bitcask"
     * then creates the active file if it doesn't exist
     *
     * this function also creates the keyDir and load it in case of recovery
     *
     *
     * @throws IOException if active file creation failed, or any
     */
    private void openBitcaskFoldersCreatedOrCreateNew() throws IOException {
        // Check parent folder
        this.directoryPath = this.directoryPath + FileSystems.getDefault().getSeparator() + Constants.MainDirectoryName;

        this.checkDirectoryExistOrCreate(this.directoryPath);
        List<String> fileNames = getFilesNames(directoryPath);
        this.createActiveFile(directoryPath);

        this.keyDir = new Hashtable<>();

        List<String> oldFilesNames = getFilesNames(directoryPath);
        if (!fileNames.isEmpty())
            this.rebuildKeyDirectory(oldFilesNames);

    }


    /***
     * this function retrieves value bytes from bitcask given the key bytes
     * @param keyBytes byte representation of the key
     * @return byte representation of the value
     * @throws IOException in case of IO error when accessing the files
     */
    public byte[] get(byte[] keyBytes) throws IOException {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
        if (keyDir.getOrDefault(key,null) == null )
            return null;
        KeyDirEntry pointer = keyDir.get(key);

        String filePath = pointer.getFileID();
        int offset = pointer.getValuePos();
        int sizeToBeRead = pointer.getValueSz();

        RandomAccessFile reader = new RandomAccessFile(filePath, "r");
        byte[] value = new byte[sizeToBeRead];
        reader.seek(offset);
        reader.readFully(value);
        reader.close();
        return value;
    }


    /**
     * this function puts the value bytes in the bitcask and handles updating the keyDir.
     * it is responsible for:
     * closing active file when the offset exceeds certain limit, also creating copy file(replica)
     * calls another function to start compaction in another thread
     * @param keyBytes bytes representation of the key
     * @param valueBytes bytes representation of the value
     * @throws Exception in case of any error in creating, renaming, copying, reading or writing the files.
     */
    public void put(byte[] keyBytes, byte[] valueBytes) throws Exception {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
        // store it on disc
        long offset = this.activeWritingFile.getFilePointer() + Constants.TimeStampSize + Constants.KeySize + Constants.ValueSize + keyBytes.length;
        long placementTimestamp = System.currentTimeMillis();


        byte[] record = (new BitcaskPersistedRecord(placementTimestamp, keyBytes, valueBytes)).getRecordBytes();
        int valSize = valueBytes.length;

        // normal appending
        KeyDirEntry pointer = new KeyDirEntry(directoryPath + FileSystems.getDefault().getSeparator() + "active.data", valSize, (int)offset, placementTimestamp);
        this.activeWritingFile.write(record);
        keyDir.put(key, pointer);

        /*
         * if the offset exceeds certain value, we need to
         * 1/ close the active file (give it a new id)
         * 2/ change all fileIDs entry that have the value "active" to the new generated ID
         *  and create hint file for the new (old file)
         * 3/ replicate the newly created File
         * 4/ create new file with active name
         * 5/increase the number of old files
         * 6 check if compaction is needed
         */
        if (offset >= this.offset_limit) {
            // the above procedure
            // STEP 1 & 2
            String newName = this.generateFileId();

            String activeFilePath = this.directoryPath + FileSystems.getDefault().getSeparator() + "active.data";
            String activeNewNamePath = directoryPath + FileSystems.getDefault().getSeparator() + newName + ".data";
            String activeNewNamePathCopy = directoryPath + FileSystems.getDefault().getSeparator() + newName + "copy" + ".data";

            activeWritingFile.close();
            renameFile(activeFilePath, activeNewNamePath);
            this.changeKeyDirEntries(newName);
            // step 3
            startCopying(activeNewNamePath, activeNewNamePathCopy);


            // step 4
            createActiveFile(directoryPath);

            //step 5
            this.currentDataFiles.incrementAndGet();

            //step 6
            if (this.currentDataFiles.get() > this.MAX_DATA_FILES) {
                startingMerging(directoryPath);
            }
        }

    }

    private void startingMerging(String path) {
        Thread newThread = new Thread(() -> {
            try {
                Thread lastThread;
                if (!compactThreads.isEmpty()) {
                    lastThread = compactThreads.getLast();
                    compactThreads.addLast(Thread.currentThread());
                    lastThread.join();
                } else
                    compactThreads.addLast(Thread.currentThread());
                merge(path);
                compactThreads.pollFirst();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        newThread.start();
    }

    private void startCopying(String oldPath, String newPath) throws IOException {
        copy(oldPath, newPath);
    }


    /***
     * this function supposed to change the filesID in keyDirectory from active file to the new File in the old directory
     * Note that the fileID is the name of the file combined with its directory
     * e.g. if the file in active file its fileID will be "active/active.data"
     *     if the file in data file in old directory its fileID will be "old/old.data"
     * @param newName the new name of the active file when transfer it to old directory
     */
    private void changeKeyDirEntries(String newName) {
        for (var key : this.keyDir.keySet()) {
            var cur = this.keyDir.get(key);
            if (Objects.equals(cur.getFileID(), directoryPath + FileSystems.getDefault().getSeparator() + "active.data")) {
                cur.setFileID(directoryPath + FileSystems.getDefault().getSeparator() + newName + ".data");
            }
        }
    }


    private void rebuildKeyDirectory(List<String> oldFilesNames) throws IOException {
        //rebuild from old directory
        Map<String, Integer> fileNameCount = new HashMap<>();
        List<String> hintFiles = new ArrayList<>();
        List<String> dataFiles = new ArrayList<>();

        // Count the occurrences of each filename
        for (String fileName : oldFilesNames) {
            if (fileName.contains("copy"))
                continue;
            fileNameCount.put(fileName.split("\\.")[0], fileNameCount.getOrDefault(fileName.split("\\.")[0], 0) + 1);
        }

        // Identify filenames that are repeated twice to use their hint files and the ones which repeated once
        // use their data files
        for (Map.Entry<String, Integer> entry : fileNameCount.entrySet()) {
            if (entry.getValue() == 2)
                hintFiles.add(entry.getKey());
            else
                dataFiles.add(entry.getKey());
        }

        //rebuild from dataFiles
        for (String file : dataFiles) {
            String fileID = file + ".data";
            String dataFile = directoryPath + FileSystems.getDefault().getSeparator() + fileID;
            this.rebuildKeyDirFromFile(dataFile, false);
        }

        //rebuild from hintFiles
        for (String file : hintFiles) {
            String fileID = file + ".hint";
            String dataFile = directoryPath + FileSystems.getDefault().getSeparator() + fileID;
            this.rebuildKeyDirFromFile(dataFile, true);
        }
    }

    /***
     * this function takes an filePath to build the current keyDirectory from it
     * @param filePath the patch to the file to build from it
     * @param hint to identify if the file is hint file or not
     */
    private void rebuildKeyDirFromFile(String filePath, boolean hint) throws IOException {
        HashMap<ByteArrayWrapper, Long> timeStamp = new HashMap<>();
        RandomAccessFile activeReader = new RandomAccessFile(filePath, "r");

        //rebuild from active file
        while (activeReader.getFilePointer() < activeReader.length()) {
            long ts = activeReader.readLong();
            int keySize = activeReader.readInt();
            int valueSize = activeReader.readInt();
            byte[] key = new byte[keySize];
            activeReader.readFully(key);
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);


            if (!timeStamp.containsKey(keyWrapper) || timeStamp.get(keyWrapper) < ts) {
                timeStamp.put(keyWrapper, ts);
                if (!hint) {
                    int offset = (int) activeReader.getFilePointer();
                    KeyDirEntry pointer = new KeyDirEntry(filePath, valueSize, offset, ts);
                    this.keyDir.put(keyWrapper, pointer);
                } else {
                    int offset = activeReader.readInt();
                    filePath = filePath.split("\\.")[0] + ".data" ;
                    KeyDirEntry pointer = new KeyDirEntry(filePath, valueSize, offset, ts);
                    this.keyDir.put(keyWrapper, pointer);
                }
            }
            if (!hint)
                activeReader.skipBytes(valueSize);
        }
        activeReader.close();
    }

    /**
     * @param dataPathFile a data file PATH that ends with .data
     * @throws IOException
     */
    private void createHintFile(String dataPathFile) throws IOException {
        String hintFilePath = dataPathFile.split("\\.data")[0] + ".hint";
        this.checkFileExistsOrCreate(hintFilePath);

        RandomAccessFile dataReader = new RandomAccessFile(dataPathFile, "r");
        RandomAccessFile hintReader = new RandomAccessFile(hintFilePath, "rw");

        HashMap<ByteArrayWrapper, Long> timeStamp = new HashMap<>();
        while (dataReader.getFilePointer() < dataReader.length()) {
            long ts = dataReader.readLong();
            int keySize = dataReader.readInt();
            int valueSize = dataReader.readInt();
            byte[] key = new byte[keySize];
            dataReader.readFully(key);
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            if (!timeStamp.containsKey(keyWrapper) || timeStamp.get(keyWrapper) < ts) {
                timeStamp.put(keyWrapper, ts);
                hintReader.write(new BitcaskHintRecord(ts, key, (int) dataReader.getFilePointer(), valueSize).getHintRecordBytes());
            }
            dataReader.skipBytes(valueSize);
        }
        dataReader.close();
        hintReader.close();
    }

    public void merge(String fullDirectory) throws IOException {
        // TODO
        // create new compactionKeyDir
        Hashtable<ByteArrayWrapper, KeyDirEntry> compressionKeyDir = new Hashtable<>();
//        getMostRecentKeyDirForCompression(compressionKeyDir);
        ArrayList<String> toBeDeletedFiles = new ArrayList<>();
        List<String> fileNames = getFilesNames(fullDirectory);
        this.currentDataFiles.set(1);
        for (String file : fileNames) {
            if (file.contains("hint")) {
                toBeDeletedFiles.add(fullDirectory + FileSystems.getDefault().getSeparator() + file);
                continue;
            }
            if (file.contains("active") || !file.contains("copy")) continue;
            processFileBeforeCompaction(fullDirectory + FileSystems.getDefault().getSeparator() + file, compressionKeyDir, toBeDeletedFiles);
        }
        String uniqueID = generateFileId();
        String compressedFullPath = fullDirectory + FileSystems.getDefault().getSeparator() + uniqueID + "compressed.data";
        String compressedCopyPath = fullDirectory + FileSystems.getDefault().getSeparator() + uniqueID + "compressedcopy.data";
        checkFileExistsOrCreate(compressedFullPath);
        keyDir = writeCompressedFile(compressedFullPath, compressionKeyDir);
        // deleting
        deleteNonCompressedFiles(fullDirectory, toBeDeletedFiles);
        createHintFile(compressedFullPath);
        startCopying(compressedFullPath, compressedCopyPath);

    }

    private void deleteNonCompressedFiles(String parentDirectory, List<String> list) throws IOException {

        List<String> files = getFilesNames(parentDirectory);

        // DELETE COPY FILES
        for (String file : list) {
            Path path = Paths.get(file);
            Files.delete(path);
        }

        // DELETE ORIGINALS FILE
        for (String fileCopy : list) {
            if (fileCopy.contains("hint")) continue;
            String[] filePart = fileCopy.split("copy");
            String file = filePart[0] + filePart[1];
            Path path = Paths.get(file);
            Files.delete(path);
        }


    }

    private void copy(String source, String destination) throws IOException {
        Path sourceFile = Paths.get(source);
        Path destinationFile = Paths.get(destination);
        Files.copy(sourceFile, destinationFile);
    }

    private Hashtable<ByteArrayWrapper, KeyDirEntry> writeCompressedFile
            (String fileFullPath, Hashtable<ByteArrayWrapper, KeyDirEntry> modifiedKeyDir) throws IOException {
        RandomAccessFile compressedFile = new RandomAccessFile(fileFullPath, "rw");
        Hashtable<ByteArrayWrapper, KeyDirEntry> afterCompressionKeyDir = new Hashtable<>();

        for (Map.Entry<ByteArrayWrapper, KeyDirEntry> entry : modifiedKeyDir.entrySet()) {
            ByteArrayWrapper keyWrapper = entry.getKey();
            KeyDirEntry info = entry.getValue();

            int valuesz = info.getValueSz();
            int startPos = info.getValuePos();
            long timestamp = info.getTimestamp();
            String realPositionFileID = info.getFileID();

            RandomAccessFile file = new RandomAccessFile(realPositionFileID, "r");
            file.seek(startPos);
            byte[] valueBytes = new byte[valuesz];
            file.readFully(valueBytes);
            byte[] keyBytes = keyWrapper.getBytes();

            // CREATE THE NEW KEYDIR

            KeyDirEntry newEntryAfterCompaction = new KeyDirEntry(
                    fileFullPath,
                    valuesz,
                    (int) (compressedFile.getFilePointer() + BitcaskPersistedRecord.getBeforeValueBytes(keyBytes.length)),
                    timestamp
            );
            afterCompressionKeyDir.put(keyWrapper, newEntryAfterCompaction);
            compressedFile.write((new BitcaskPersistedRecord(System.currentTimeMillis(), keyBytes, valueBytes)).getRecordBytes());
            file.close();
        }
        compressedFile.close();
        return afterCompressionKeyDir;
    }

    private void getMostRecentKeyDirForCompression(Hashtable<ByteArrayWrapper, KeyDirEntry> newKeyDir) {
        for (Map.Entry<ByteArrayWrapper, KeyDirEntry> entry : keyDir.entrySet()) {
            if (!entry.getValue().getFileID().contains("active")) {
                String newFileID = entry.getValue().getFileID().split("\\.data")[0] + "copy.data";
                KeyDirEntry newEntryVal = new KeyDirEntry(newFileID,
                        entry.getValue().getValueSz(),
                        entry.getValue().getValuePos(),
                        entry.getValue().getTimestamp());
                newKeyDir.put(entry.getKey(), newEntryVal);
            }
        }
    }


    /**
     * This function reads a certain file and modifies given KeyDir that is used for compaction
     * It stores in the keyDir the most recent reference
     * FOR BACKUP PURPOSES IF I DON'T USE ORIGINAL KEY DIR, THIS FUNCTION WILL RELY COMPLETELY ON THE REPLICAS
     *
     * @param path
     * @param currKeyDir
     * @throws IOException
     */
    private void processFileBeforeCompaction(String path, Hashtable<ByteArrayWrapper, KeyDirEntry> currKeyDir, List<String> toBeDeleted) throws IOException {
        RandomAccessFile file = new RandomAccessFile(path, "r");
//        if(path.contains("hint")) return;
        toBeDeleted.add(path);
        while (file.getFilePointer() < file.length()) {
            long ts = file.readLong();
            int keySize = file.readInt();
            int valueSize = file.readInt();
            byte[] key = new byte[keySize];
            file.readFully(key);
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

            if (!currKeyDir.containsKey(keyWrapper) || currKeyDir.get(keyWrapper).getTimestamp() < ts) {
                //if more recent or doesn't exist
                int positionInFile = (int) file.getFilePointer();
                KeyDirEntry entry = new KeyDirEntry(path, valueSize, positionInFile, ts);
                currKeyDir.put(keyWrapper, entry);
            }
            file.skipBytes(valueSize);
        }
        file.close();
    }


    private String generateFileId() {
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        return timestamp + "-" + uuid;
    }

    private void renameFile(String oldFilePath, String newFilePath) throws IOException {
        File oldFile = new File(oldFilePath);
        File newFile = new File(newFilePath);
        // Rename the file
//        boolean isRenamed = oldFile.renameTo(newFile);
        FileUtils.moveFile(oldFile, newFile);
//        if (!isRenamed)
//            throw new FileSystemException("Failed to rename the file from:" + oldFilePath + " to:" + newFilePath);
    }

    private List<String> getFolderNames(String directoryPath) {
        List<String> folderNames = new ArrayList<>();
        File directory = new File(directoryPath);
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            assert files != null;
            for (File file : files) {
                if (file.isDirectory()) {
                    folderNames.add(file.getName());
                }
            }
        }
        return folderNames;
    }

    private List<String> getFilesNames(String directoryPath) {
        List<String> fileNames = new ArrayList<>();
        File directory = new File(directoryPath);
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            assert files != null;
            for (File file : files) {
                if (!file.isDirectory()) {
                    fileNames.add(file.getName());
                }
            }
        }
        return fileNames;
    }

    private void createActiveFile(String activeDirectoryPath) throws IOException {
        this.checkFileExistsOrCreate(activeDirectoryPath + FileSystems.getDefault().getSeparator() + Constants.ActiveFileName);
        this.activeWritingFile = new RandomAccessFile(activeDirectoryPath + FileSystems.getDefault().getSeparator() + Constants.ActiveFileName, "rw");
    }

    private boolean checkFileExistsOrCreate(String filePath) throws IOException {
        File file = new File(filePath);
        // Check if the file exists
        if (!file.exists()) {
            // Create the file if it doesn't exist
            boolean created = file.createNewFile();
            if (!created)
                throw new FileSystemException("File not found, Failed to create file at: " + filePath);
            return true;
        }

        return false;
    }

    /***
     * check Directory Exist Or Create it
     * @param directoryPath
     * @return true if the directory already exist and false if it isn't exist, and we created it
     * @throws FileSystemException
     */
    private boolean checkDirectoryExistOrCreate(String directoryPath) throws FileSystemException {
        File directory = new File(directoryPath);
        // Check if the directory exists
        if (!directory.exists()) {
            // Attempt to create the directory
            boolean created = directory.mkdirs();
            if (!created)
                throw new FileSystemException("Folder not found, Failed to create folder at: " + directoryPath);
            return false;
        }
        return true;
    }
}

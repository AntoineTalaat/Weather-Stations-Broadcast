import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.*;

public class Bitcask {
    private String directoryPath;
    private RandomAccessFile activeWritingFile;
    private Hashtable<ByteArrayWrapper, KeyDirEntry> keyDir; // TODO
    private final long offset_limit = 10000; // roughly max file size 10 kB

    private long currentDataFiles = 0;
    private final long MAX_DATA_FILES = 5;

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

    /**
     * let's say u want to open a bitcask file at home/userprograms
     * this function should create new directory home/userprograms/bitcask/
     * inside that we should have a FOLDER for each segment such that
     * the folder contains both the data files and the hint file
     * <p>
     * this method creates the main directory if it doesn't exist and the active segment file
     * if the directory exists we should check only if we need the active folder
     * <p>
     * The structure should be like that:
     * home/userprograms/
     * bitcask --> configurations file ???? TODO
     * IGNORE /files/ for now
     * --> files --> active --> active.data
     * --> active.hint
     * --> oldFile1 --> oldFile1.data
     * --> oldFile1.hint
     * --> replicas
     * etc
     */
    private void openBitcaskFoldersCreatedOrCreateNew() throws IOException {
        // Check parent folder
        this.directoryPath = this.directoryPath + FileSystems.getDefault().getSeparator() + Constants.MainDirectoryName;
//        this.directoryPath = this.directoryPath + "/"+ Constants.MainDirectoryName;

        this.checkDirectoryExistOrCreate(this.directoryPath);
        // get the names of the folders in the directory
        List<String> fileNames = getFilesNames(directoryPath);
        this.createActiveFile(directoryPath);

        this.keyDir = new Hashtable<>();

        List<String> oldFilesNames = getFilesNames(directoryPath);
        if(!fileNames.isEmpty()) this.rebuildKeyDirectory(oldFilesNames);

    }

    public byte[] get(byte[] keyBytes) throws IOException {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
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

    /*
     *  when we need to add a record to the bitcask file, we expect to append the entry to the opened active file
     */
    public void put(byte[] keyBytes, byte[] valueBytes) throws Exception {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
        // store it on disc
        long offset = this.activeWritingFile.getFilePointer() + Constants.TimeStampSize + Constants.KeySize + Constants.ValueSize + keyBytes.length;
        long placementTimestamp = System.currentTimeMillis();


        byte[] record = (new BitcaskPersistedRecord(placementTimestamp, keyBytes, valueBytes)).getRecordBytes();
        int valSize = valueBytes.length;

        // normal appending
        KeyDirEntry pointer = new KeyDirEntry(directoryPath+FileSystems.getDefault().getSeparator()+"active.data", valSize, (int) offset, placementTimestamp);
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

            String activeFilePath= this.directoryPath + FileSystems.getDefault().getSeparator()+ "active.data";
            String activeNewNamePath = directoryPath +FileSystems.getDefault().getSeparator() + newName + ".data";
            String activeNewNamePathCopy = directoryPath + FileSystems.getDefault().getSeparator()+ newName + "copy"+".data";


//
//            String activeFilePath= this.directoryPath +"/"+ "active.data";
//            String activeNewNamePath = directoryPath +"/" + newName + ".data";
//            String activeNewNamePathCopy = directoryPath + "/"+ newName + "copy"+".data";
            activeWritingFile.close();
            renameFile(activeFilePath,activeNewNamePath );
//            checkFileExistsOrCreate(activeNewNamePathCopy);
            this.changeKeyDirEntries(newName);
            // step 3
            var copyThread = openCopyThread(activeNewNamePath,activeNewNamePathCopy);

            // step 4
            createActiveFile(directoryPath);

            //step 5
            this.currentDataFiles += 1;

            //step 6
            if (this.currentDataFiles > this.MAX_DATA_FILES) {
                startingMerging(directoryPath, copyThread);
            }
        }

    }

    private void startingMerging(String path , Thread copyThread) throws InterruptedException {
        Thread thread = new Thread(()->{
            try {
                merge(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        copyThread.join();
        thread.start();
    }

    private Thread openCopyThread(String oldPath, String newPath){
        Thread thread = new Thread(() -> {
            try {
                copy(oldPath,newPath);
            } catch (IOException e) {
                e.printStackTrace();
            }

        });
        thread.start();
        return thread;
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
            if (Objects.equals(cur.getFileID(), directoryPath + FileSystems.getDefault().getSeparator() +"active.data")) {
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
            if(fileName.contains("copy")) continue;
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
            String fileID =  file + ".data";
            String dataFile = directoryPath + FileSystems.getDefault().getSeparator() + fileID;
            this.rebuildKeyDirFromFile(dataFile, false);
        }

        //rebuild from hintFiles
        for (String file : hintFiles) {
            String fileID =  file + ".data";
            String dataFile = directoryPath +FileSystems.getDefault().getSeparator() + fileID;
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
     *
     * @param fileNewPathStr a data file PATH that ends with .data
     * @throws IOException
     */
    private void createHintFile(String fileNewPathStr) throws IOException {
        String hintFilePath = fileNewPathStr.split("\\.data")[0] + ".hint";

        this.checkFileExistsOrCreate(hintFilePath);

        RandomAccessFile dataReader = new RandomAccessFile(fileNewPathStr, "r");
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
        Hashtable<ByteArrayWrapper,KeyDirEntry> compressionKeyDir = new Hashtable<>();
//        getMostRecentKeyDirForCompression(compressionKeyDir);
        List<String> fileNames = getFilesNames(fullDirectory);
        for(String file:fileNames){
            if(file.contains("active") || !file.contains("copy")) continue;
            System.out.println(fullDirectory+ FileSystems.getDefault().getSeparator() + file);
            processFileBeforeCompaction(fullDirectory+ FileSystems.getDefault().getSeparator() + file,compressionKeyDir);
        }
        String compressedFullPath = fullDirectory + FileSystems.getDefault().getSeparator() +generateFileId()+"compressed.data";
        String compressedCopyPath = fullDirectory + FileSystems.getDefault().getSeparator() +generateFileId()+ "compressedcopy.data";
        checkFileExistsOrCreate(compressedFullPath);
        keyDir = writeCompressedFile(compressedFullPath,compressionKeyDir);
        copy(compressedFullPath,compressedCopyPath);
        /*
            At this point the following should've been achieved
            - created new hash table containing the same as keyDir but with changing the mode to read from copies
            - made new file in the same directory named xxxxxxxxxcompressed.data where the xs are numbers
            - write the info from the duplicate keyDir to the file and updated the keyDir to make new reads from the compressed
            - create hint file


            At this point the directory should look like the following
            bitcask/
                   active.data
                   xxxxxxxxxxx1.data x
                   xxxxxxxxxxx1copy.data x // TODO
                   xxxxxxxxxxx2.data        x
                   xxxxxxxxxxx2copy.data    x
                   xxxxxxxxxxx5compressed.data
                   xxxxxxxxxxx5compressedcopy.data //TODO
                   xxxxxxxxxxx5compressed.hint

                  NOW WE ADDRESS THE FOLLOWING,
                  1/ handle creating copies TODO
                  2/ recheck the previous code TODO
                  3/ plan the replacement
                  4/ delete any file that doesn't contain compressed or active DONE

         */

        // deleting
        deleteNonCompressedFiles(fullDirectory,compressedFullPath,compressedCopyPath);
        createHintFile(compressedFullPath);

    }

    private void deleteNonCompressedFiles(String parentDirectory, String compressedRealFile, String compressedCopyFile) throws IOException {

        List<String> files = getFilesNames(parentDirectory);
        for(String file : files){
            if(file.contains("active") || file.equals(compressedCopyFile) || file.equals(compressedRealFile)) continue;
            Path path = Paths.get(parentDirectory + FileSystems.getDefault().getSeparator() + file);
            // Delete the file
            Files.delete(path);
        }


    }

    private void copy(String source,String destination) throws IOException {
        Path sourceFile = Paths.get(source);
        Path destinationFile = Paths.get(destination);

        // Copy the file from source to destination
        Files.copy(sourceFile, destinationFile);
    }

    private Hashtable<ByteArrayWrapper,KeyDirEntry> writeCompressedFile
                        (String fileFullPath, Hashtable<ByteArrayWrapper,KeyDirEntry> modifiedKeyDir) throws IOException {
        RandomAccessFile compressedFile = new RandomAccessFile(fileFullPath,"rw");
        Hashtable<ByteArrayWrapper,KeyDirEntry> afterCompressionKeyDir = new Hashtable<>();

        for(Map.Entry<ByteArrayWrapper, KeyDirEntry> entry : modifiedKeyDir.entrySet()){
            ByteArrayWrapper keyWrapper = entry.getKey();
            KeyDirEntry info = entry.getValue();

            int valuesz = info.getValueSz();
            int startPos = info.getValuePos();
            long timestamp = info.getTimestamp();
            String realPositionFileID = info.getFileID();

            RandomAccessFile file = new RandomAccessFile(realPositionFileID,"r");
            file.seek(startPos);
            byte[] valueBytes = new byte[valuesz];
            file.readFully(valueBytes);
            byte[] keyBytes = keyWrapper.getBytes();

            // CREATE THE NEW KEYDIR

            KeyDirEntry newEntryAfterCompaction = new KeyDirEntry(
                    fileFullPath,
                    valuesz,
                    (int) (compressedFile.getFilePointer()+BitcaskPersistedRecord.getBeforeValueBytes(keyBytes.length)),
                    timestamp
            );
            afterCompressionKeyDir.put(keyWrapper,newEntryAfterCompaction);
            compressedFile.write((new BitcaskPersistedRecord(System.currentTimeMillis(),keyBytes,valueBytes)).getRecordBytes());
            file.close();
        }
        compressedFile.close();
        return afterCompressionKeyDir;
    }

    private void getMostRecentKeyDirForCompression(Hashtable<ByteArrayWrapper,KeyDirEntry> newKeyDir){
        for (Map.Entry<ByteArrayWrapper, KeyDirEntry> entry : keyDir.entrySet()) {
            // so the original place for the data was
            // bitcask/id.data
            if(!entry.getValue().getFileID().contains("active")) {
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
     * @param path
     * @param currKeyDir
     * @throws IOException
     */
    private void processFileBeforeCompaction(String path,Hashtable<ByteArrayWrapper,KeyDirEntry> currKeyDir) throws IOException {
        RandomAccessFile file = new RandomAccessFile(path,"r");
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
                KeyDirEntry entry = new KeyDirEntry(path,valueSize,positionInFile,ts);
                currKeyDir.put(keyWrapper,entry);
            }
            file.skipBytes(valueSize);
        }
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
        FileUtils.moveFile(oldFile,newFile);

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
        this.checkFileExistsOrCreate(activeDirectoryPath +FileSystems.getDefault().getSeparator() + Constants.ActiveFileName);
        this.activeWritingFile = new RandomAccessFile(activeDirectoryPath +FileSystems.getDefault().getSeparator() + Constants.ActiveFileName, "rw");
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

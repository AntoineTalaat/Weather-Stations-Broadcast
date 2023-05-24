import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystemException;
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
     * --> replicas ?? TODO
     * etc
     */
    private void openBitcaskFoldersCreatedOrCreateNew() throws IOException {
        // Check parent folder
        this.directoryPath = this.directoryPath + "/" + Constants.MainDirectoryName;

        this.checkDirectoryExistOrCreate(this.directoryPath);
        // get the names of the folders in the directory
        List<String> folderNames = getFolderNames(directoryPath);

        // Check active folder
        String activeDirectoryPath = this.directoryPath + "/" + Constants.ActiveDirectoryName;

        this.checkDirectoryExistOrCreate(activeDirectoryPath);
        this.createActiveFile(activeDirectoryPath);
        // Create hashtable and Load From Hint files if available
        // TODO I will assume the hashtable doesn't exist and create empty one,
        //  but when the project is more complete I need to consider reading the existing directories and
        //  check if I had stored data before, and read them through the hint files

        //if there is no active or old folders or there is only active folder
        if (folderNames.isEmpty() || folderNames.size() == 1)
            this.keyDir = new Hashtable<>();
        else {
            this.keyDir = new Hashtable<>();
            String oldDirectoryPath = this.directoryPath + "/" + Constants.OldDirectoryName;

            List<String> oldFilesNames = getFilesNames(oldDirectoryPath);
            this.rebuildKeyDirectory(oldFilesNames);
        }
    }

    public byte[] get(byte[] keyBytes) throws IOException {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
        KeyDirEntry pointer = keyDir.get(key);

        String filePath = this.directoryPath + "/" + pointer.getFileID();
        int offset = pointer.getValuePos();
        int sizeToBeRead = pointer.getValueSz();

        RandomAccessFile reader = new RandomAccessFile(filePath, "r");
        byte[] value = new byte[sizeToBeRead];
        reader.seek(offset);
        reader.readFully(value);

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

        String fileID = Constants.ActiveFilePath;

        byte[] record = (new BitcaskPersistedRecord(placementTimestamp, keyBytes, valueBytes)).getRecordBytes();
        int valSize = valueBytes.length;

        // normal appending
        KeyDirEntry pointer = new KeyDirEntry(fileID, valSize, (int) offset, placementTimestamp);
        this.activeWritingFile.write(record);
        keyDir.put(key, pointer);

        /*
         * if the offset exceeds certain value, we need to
         * 1/ close the active file (give it a new id)
         * 2/ change all fileIDs entry that have the value "active" to the new generated ID
         *  and create hint file for the new (old file)
         * 3/ replicate the newly created File TODO
         * 4/ create new file with active name
         * 5/increase the number of old files
         * 6 check if compaction is needed
         */
        if (offset >= this.offset_limit) {
            //close the file with zero
            this.activeWritingFile.writeLong(0);

            // the above procedure
            // STEP 1 & 2
            String newName = this.generateFileId();
            String activeDirectoryPath = directoryPath + "/" + Constants.ActiveDirectoryName;
            String newDirectory = directoryPath + "/" + Constants.OldDirectoryName;

            //check or create old directory
            this.checkDirectoryExistOrCreate(newDirectory);
            renameFile(this.directoryPath + "/" + Constants.ActiveFilePath, newDirectory + "/" + newName + ".data");

            this.changeKeyDirEntries(newName);

            // step 3 TODO when doing compaction part

            // step 4
            createActiveFile(activeDirectoryPath);

            //step 5
            this.currentDataFiles += 1;

            //step 6 TODO
            if (this.currentDataFiles > this.MAX_DATA_FILES) {
                // TODO COMPACTION
            }
        }

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
            if (Objects.equals(cur.getFileID(), Constants.ActiveFilePath)) {
                cur.setFileID(Constants.OldDirectoryName + "/" + newName + ".data");
            }
        }
    }

    // TODO change file id of records in active file to old file
    //  continue rebuild key directory
    private void rebuildKeyDirectory(List<String> oldFilesNames) throws IOException {
        String active = directoryPath + "/" + Constants.ActiveFilePath;
        this.rebuildKeyDirFromFile(active, Constants.ActiveFilePath, false);

        //rebuild from old directory
        Map<String, Integer> fileNameCount = new HashMap<>();
        List<String> hintFiles = new ArrayList<>();
        List<String> dataFiles = new ArrayList<>();

        // Count the occurrences of each filename
        for (String fileName : oldFilesNames) {
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
            String fileID = Constants.OldDirectoryName + "/" + file + ".data";
            String dataFile = directoryPath + "/" + fileID;
            this.rebuildKeyDirFromFile(dataFile, fileID, false);
        }

        //rebuild from hintFiles
        for (String file : hintFiles) {
            String fileID = Constants.OldDirectoryName + "/" + file + ".data";
            String dataFile = directoryPath + "/" + fileID;
            this.rebuildKeyDirFromFile(dataFile, fileID, true);
        }
    }

    /***
     * this function takes an filePath to build the current keyDirectory from it
     * @param filePath the patch to the file to build from it
     * @param hint to identify if the file is hint file or not
     */
    private void rebuildKeyDirFromFile(String filePath, String fileID, boolean hint) throws IOException {
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
                    KeyDirEntry pointer = new KeyDirEntry(fileID, valueSize, offset, ts);
                    this.keyDir.put(keyWrapper, pointer);
                } else {
                    int offset = activeReader.readInt();
                    KeyDirEntry pointer = new KeyDirEntry(fileID, valueSize, offset, ts);
                    this.keyDir.put(keyWrapper, pointer);
                }
            }
            if (!hint)
                activeReader.skipBytes(valueSize);
        }
    }

    private void createHintFile(String fileNewName) throws IOException {
        String hintFile = directoryPath + "/" + Constants.OldDirectoryName + "/" + fileNewName + ".hint";
        this.checkFileExistsOrCreate(hintFile);

        RandomAccessFile dataReader = new RandomAccessFile(directoryPath + "/" + Constants.ActiveFilePath, "r");
        RandomAccessFile hintReader = new RandomAccessFile(hintFile, "r");

        HashMap<ByteArrayWrapper, Long> timeStamp = new HashMap<>();
        long ts = dataReader.readLong();
        while (ts != 0) {
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

            ts = dataReader.readLong();
        }
        dataReader.close();
        hintReader.close();
    }

    public void merge(String directoryName) {
        // TODO

    }


    private String generateFileId() {
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        return timestamp + "-" + uuid;
    }

    private void renameFile(String oldFilePath, String newFilePath) throws FileSystemException {
        File oldFile = new File(oldFilePath);
        File newFile = new File(newFilePath);
        // Rename the file
        boolean isRenamed = oldFile.renameTo(newFile);

        if (!isRenamed)
            throw new FileSystemException("Failed to rename the file from:" + oldFilePath + " to:" + newFilePath);
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
        this.checkFileExistsOrCreate(activeDirectoryPath + "/" + Constants.ActiveFileName);
        this.activeWritingFile = new RandomAccessFile(activeDirectoryPath + "/" + Constants.ActiveFileName, "rw");
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
     * @return true if the directory already exist and false if it isn't exist and we created it
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

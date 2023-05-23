import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystemException;
import java.util.*;

public class Bitcask {
    private String directoryPath;
    private RandomAccessFile activeWritingFile;
    //    private RandomAccessFile activeWritingHintFile; // TODO check the type of active hint files
    private Hashtable<ByteArrayWrapper, KeyDirEntry> keyDir; // TODO
    private long offset_limit = 10000; // roughly max file size 10 kB

    /**
     * Opens the bitcask in a given directory
     *
     * @param directoryPath
     */
    public void open(String directoryPath) throws IOException {
        // TODO

        /*
         * open new Or existing bitcask datastore
         * creating is trivial.
         * opening will require that I save the path string
         * such that when accessing the bitcask we can concatenate it
         */
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
        this.directoryPath = this.directoryPath + "/bitcask";
        this.checkDirectoryExistOrCreate(this.directoryPath);
        // get the names of the folders in the directory
        List<String> folderNames = getFolderNames(directoryPath);

        // Check active folder
        String activeDirectoryPath = this.directoryPath + "/active";
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
            // TODO REBUILDING HASH TABLE,
            //  now we are only creating new one
            this.keyDir = new Hashtable<>();

            String oldDirectoryPath = this.directoryPath + "/old";
            List<String> oldFilesNames = getFilesNames(oldDirectoryPath);
            this.rebuildKeyDirectory(oldDirectoryPath, oldFilesNames);
        }
    }

    public byte[] get(byte[] keyBytes) throws IOException {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
        KeyDirEntry pointer = keyDir.get(key);

        String filePath = this.directoryPath + "/" + pointer.getFileID() + "/" + pointer.getFileID() + ".data";
        long offset = pointer.getValuePos();
        int sizeToBeRead = pointer.getValueSz();

        RandomAccessFile reader = new RandomAccessFile(filePath, "r");
        byte[] record = new byte[sizeToBeRead];
        reader.seek(offset);
        reader.read(record);

        BitcaskPersistedRecord retrievedRecord = new BitcaskPersistedRecord(record);
        assert (key.equals(new ByteArrayWrapper(retrievedRecord.getKey())));

        return retrievedRecord.getValue();
    }

    /*
     *  when we need to add a record to the bitcask file, we expect to append the entry to the opened active file
     */
    public void put(byte[] keyBytes, byte[] valueBytes) throws Exception {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
        // store it on disc
        long offset = this.activeWritingFile.getFilePointer();
        long placementTimestamp = System.currentTimeMillis();
        String fileID = "active";
        byte[] record = (new BitcaskPersistedRecord(placementTimestamp, keyBytes, valueBytes)).getRecordBytes();
        int valSize = record.length;

        // normal appending
        KeyDirEntry pointer = new KeyDirEntry(fileID, valSize, offset, placementTimestamp);
        this.activeWritingFile.write(record);
        keyDir.put(key, pointer);

        /*
         * if the offset exceeds certain value, we need to
         * 1/ close the active file (give it a new id)
         * 2/ change all fileids entry that have the value "active" to the new generated ID
         *  and create hint file for the new (old file)
         * 3/ replicate the newly created File TODO
         * 4/ create new file with active name
         * 5/ get the new offset
         * 6/ store offset in temp keyDir
         * 7/ write the value
         * 8/ store the value in keyDir
         */
        offset = this.offset_limit;
        if (offset >= this.offset_limit) {
            //close the file with zero
            this.activeWritingFile.writeLong(0);

            // the above procedure
            // STEP 1 & 2
            String newName = this.generateFileId();

            String activeDirectoryPath = directoryPath + "/active";
            String newDirectory = directoryPath + "/" + "old";

            //check or create old directory
            this.checkDirectoryExistOrCreate(newDirectory);

            this.createHintFile(newName);
            renameFile(this.directoryPath + "/active.data", newDirectory + "/" + newName + ".data");

            // step 3 TODO
            // step 4
            createActiveFile(activeDirectoryPath);
            // step 5
            offset = this.activeWritingFile.getFilePointer();
        }

    }

    // TODO
    private void rebuildKeyDirectory(String oldFilesDirectory, List<String> oldFilesNames) {

    }

    private void createHintFile(String fileNewName) throws IOException {
        String hintFile = directoryPath + "/old/" + fileNewName + ".hint" ;
        this.checkFileExistsOrCreate(hintFile);

        RandomAccessFile dataReader = new RandomAccessFile(directoryPath + "/active/active.data", "r");
        RandomAccessFile hintReader = new RandomAccessFile(hintFile, "r");

        HashMap<ByteArrayWrapper, Long> timeStamp = new HashMap<>() ;
        long ts = dataReader.readLong();
        while (ts != 0){
            int keySize = dataReader.readInt();
            int valueSize = dataReader.readInt();
            byte[] key = new byte[keySize] ;
            dataReader.readFully(key);
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            if(!timeStamp.containsKey(keyWrapper) || timeStamp.get(keyWrapper) < ts){
                timeStamp.put(keyWrapper, ts);

                hintReader.write(new BitcaskHintRecord(ts, key, (int)dataReader.getFilePointer() , valueSize).getHintRecordBytes());
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
        this.checkFileExistsOrCreate(activeDirectoryPath + "/active.data");
        this.activeWritingFile = new RandomAccessFile(activeDirectoryPath + "/active.data", "rw");
//        this.activeWritingHintFile = new RandomAccessFile(activeDirectoryPath+"/active.hint","rw");
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

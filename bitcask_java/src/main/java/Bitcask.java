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
    private long offset_limit = 10000; // 10 kB

    /**
     * Opens the bitcask in a given directory
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
     *
     * this method creates the main directory if it doesn't exist and the active segment file
     * if the directory exists we should check only if we need the active folder
     *
     * The structure should be like that:
     * home/userprograms/
     *                  bitcask --> configurations file ???? TODO
     *                             IGNORE /files/ for now
     *                          --> files --> active --> active.data
     *                                               --> active.hint
     *                                    --> oldFile1 --> oldFile1.data
     *                                                 --> oldFile1.hint
     *                          --> replicas ?? TODO
     *                 etc
     *
     */
    private void openBitcaskFoldersCreatedOrCreateNew() throws IOException {
        // Check parent folder
        this.directoryPath = this.directoryPath + "/bitcask";
        this.checkDirectoryExistOrCreate(this.directoryPath);
        // get the names of the files in
        List<String> folderNames = getFolderNames(directoryPath);

        // Check active folder
        String activeDirectoryPath = this.directoryPath + "/active";
        this.checkDirectoryExistOrCreate(activeDirectoryPath);
        this.createActiveFile(activeDirectoryPath);
        // Create hashtable and Load From Hint files if available
        // TODO I will assume the hashtable doesn't exist and create empty one,
        //  but when the project is more complete I need to consider reading the existing directories and
        //  check if I had stored data before, and read them through the hint files

        if(folderNames.isEmpty())
            this.keyDir = new Hashtable<>();
        else{
            // CASE REBUILDING HASH TABLE
            this.keyDir = new Hashtable<>();

            //sort to get most recent first
            folderNames.sort(Collections.reverseOrder());
            folderNames.remove("active");

            // check hint file of active
            this.processIntoKeyDir(directoryPath+"/active/active.hint");
            // check the rest after one by one

        }

    }


    private void processIntoKeyDir(String hintFilePath){

    }

    public byte[] get(byte[] keyBytes) throws IOException {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
        KeyDirEntry pointer = keyDir.get(key);

        String filePath = this.directoryPath + "/" + pointer.getFileID() + "/" + pointer.getFileID() + ".data";
        long offset = pointer.getValuePos();
        int sizeToBeRead = pointer.getValueSz();

        RandomAccessFile reader = new RandomAccessFile(filePath,"r");
        byte[] record = new byte[sizeToBeRead];
        reader.seek(offset);
        reader.read(record);

        BitcaskPersistedRecord retrievedRecord = new BitcaskPersistedRecord(record);
        assert(key.equals(new ByteArrayWrapper(retrievedRecord.getKey())));

        return retrievedRecord.getValue();
    }

    /*
     *  when we need to add a record to the bitcask file, we expect to append the entry to the opened active file
     */
    public void put( byte[] keyBytes, byte[] valueBytes) throws Exception {
        ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
        // store it on disc
        long offset = this.activeWritingFile.getFilePointer();
        /*
         * if the offset exceeds certain value, we need to
         * 1/ close the active file (give it a new id)
         * 2/ change all fileids entry that have the value "active" to the new generated ID
         * 3/ replicate the newly created File TODO
         * 4/ create new file with active name
         * 5/ get the new offset
         * 6/ store offset in temp keyDir
         * 7/ write the value
         * 8/ store the value in keyDir
         */
        long placementTimestamp = System.currentTimeMillis();
        String fileID = "active";
        byte[] record = (new BitcaskPersistedRecord(placementTimestamp,keyBytes,valueBytes)).getRecordBytes();
        byte[] hintRecord = (new BitcaskHintRecord(placementTimestamp,offset,keyBytes)).getHintRecordBytes();
        int valSize = record.length;

        if(offset < this.offset_limit){
            // normal appending
            KeyDirEntry pointer = new KeyDirEntry(fileID,valSize,offset,placementTimestamp);
            this.activeWritingFile.write(record);
//            this.activeWritingHintFile.write(hintRecord);
            keyDir.put(key,pointer);

        } else {
            // the above procedure
            // STEP 1 & 2
            String newName = this.generateFileId();

            String activeDirectoryPath = directoryPath+"/active";
            String newDirectory = directoryPath+ "/" + newName;

            boolean createdNewDir = this.checkDirectoryExistOrCreate(newDirectory);
            if(!createdNewDir)
                throw new Exception("Weird behaviour, new file id is already present!");

            renameFile(this.directoryPath+"/active.data", newDirectory + "/" + newName + ".data");
            renameFile(this.directoryPath+"/active.hint", newDirectory + "/" + newName + ".hint");

            // step 3 TODO
            // step 4
            createActiveFile(activeDirectoryPath);
            // step 5
            offset = this.activeWritingFile.getFilePointer();
            // normal appending
            KeyDirEntry pointer = new KeyDirEntry(fileID,valSize,offset,placementTimestamp);
            this.activeWritingFile.write(record);
//            this.activeWritingHintFile.write(hintRecord);

            keyDir.put(key,pointer);
        }

    }

    private void updateHintFile(){

    }

    public void merge(String directoryName) {
        // TODO

    }


    private String generateFileId() {
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        return timestamp + "-" + uuid;
    }

    private void renameFile(String oldFilePath,String newFilePath) throws FileSystemException {
        File oldFile = new File(oldFilePath);
        File newFile = new File(newFilePath);
        // Rename the file
        boolean isRenamed = oldFile.renameTo(newFile);

        if (!isRenamed)
            throw new FileSystemException("Failed to rename the file from:" + oldFilePath +" to:" + newFilePath);

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

    private void createActiveFile(String activeDirectoryPath) throws IOException {
        this.checkFileExistsOrCreate(activeDirectoryPath+"/active.data");
        this.checkFileExistsOrCreate(activeDirectoryPath+"/active.hint");
        this.activeWritingFile = new RandomAccessFile(activeDirectoryPath+"/active.data","rw");
//        this.activeWritingHintFile = new RandomAccessFile(activeDirectoryPath+"/active.hint","rw");
    }

    private boolean checkFileExistsOrCreate(String filePath) throws IOException {
        File file = new File(filePath);
        // Check if the file exists
        if (!file.exists()) {
            // Create the file if it doesn't exist
            boolean created = file.createNewFile();
            if (!created)
                throw new FileSystemException("File not found, Failed to create file at: "+filePath);
            return true;
        }
        return false;
    }

    private boolean checkDirectoryExistOrCreate(String directoryPath) throws FileSystemException {
        File directory = new File(directoryPath);
        // Check if the directory exists
        if (!directory.exists()) {
            // Attempt to create the directory
            boolean created = directory.mkdirs();
            if (!created)
                throw new FileSystemException("Folder not found, Failed to create folder at: "+directoryPath);
            return true;
        }
        return false;
    }
}

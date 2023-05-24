public class Constants {
    public static final int TimeStampSize = Long.BYTES ;
    public static final int KeySize = Integer.BYTES ;
    public static final int ValueSize = Integer.BYTES ;
    public static final int ValueOffsetSize = Integer.BYTES ;
    public static final String MainDirectoryName = "bitcask" ;
    public static final String ActiveDirectoryName = "active" ;
    public static final String ActiveFileName = "active.data" ;
    public static final String ActiveFilePath = Constants.ActiveDirectoryName+ "/" + Constants.ActiveFileName  ;
    public static final String OldDirectoryName = "old" ;
}

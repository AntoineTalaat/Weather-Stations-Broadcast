public class Main {
    public static void main(String[] args) throws Exception {
        Bitcask bitcask = new Bitcask();
//        bitcask.open("/home/mohamed/Desktop/Bitcask-Implementation/bitcask_java");

        bitcask.open("bitcask_java");
//
//        bitcask.open("bitcask_java");
        byte[] arr1 = {1,2,3};
        byte[] arr2 = {1,2,3,2};
        byte[] arr3 = {1,2,3,4};
        for(int i=0;i<10000;i++)
            bitcask.put(arr1, arr2);
        bitcask.put(arr1, arr3);

    }
}

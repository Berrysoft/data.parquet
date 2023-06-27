package berrysoft.data;

public class ParquetNative {
    static {
        System.loadLibrary("berrysoft_data_parquet_jni");
    }

    public static native String hello();
}

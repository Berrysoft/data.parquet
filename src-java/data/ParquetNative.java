package data;

import java.util.List;

public class ParquetNative {
    static {
        System.loadLibrary("berrysoft_data_parquet_jni");
    }

    public static native long open(String path);

    public static native void close(long reader);

    public static native List<String> getColumns(long reader);

    public static native List<Object> getColumn(long reader, String name);
}

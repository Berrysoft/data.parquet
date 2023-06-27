package data;

import java.util.List;
import java.util.Map;

public class ParquetNative {
    static {
        System.loadLibrary("berrysoft_data_parquet_jni");
    }

    public static native long openReader(String path);

    public static native void closeReader(long reader);

    public static native List<String> getColumns(long reader);

    public static native long getColumn(long reader, String name);

    public static native void closeColumn(long col);

    public static native Object columnNext(long col);

    public static native long openWriter(String path, Map<String, Class<?>> schema);

    public static native void closeWriter(long writer);

    public static native void writeRow(long writer, Map<String, Object> values);
}

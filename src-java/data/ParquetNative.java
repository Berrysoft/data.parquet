package data;

import java.util.List;
import java.util.Map;

public class ParquetNative {
    static {
        System.loadLibrary("berrysoft_data_parquet_jni");
    }

    public static native Map<String, List<Object>> open(String path);
}

use arrow_array::RecordBatchReader;
use jni::{
    objects::{JClass, JString, JValue},
    sys::jobject,
    JNIEnv,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

#[no_mangle]
pub extern "system" fn Java_data_ParquetNative_open<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jobject {
    let path: String = env.get_string(&path).unwrap().into();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();

    let map = env.new_object("Ljava/util/HashMap;", "()V", &[]).unwrap();

    let schema = reader.schema();
    for f in schema.fields() {
        let list = env.new_object("Ljava/util/ArrayList;", "()V", &[]).unwrap();
        let name = env.new_string(f.name()).unwrap();
        env.call_method(
            &map,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[JValue::Object(&name), JValue::Object(&list)],
        )
        .unwrap();
    }
    map.into_raw()
}

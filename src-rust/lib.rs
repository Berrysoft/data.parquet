#![allow(clippy::missing_safety_doc)]

use arrow_array::RecordBatchReader;
use arrow_schema::DataType;
use jni::{
    objects::{JClass, JObject, JString, JValue},
    sys::{jlong, jobject},
    JNIEnv,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

struct NativeReader {
    file: File,
}

impl Clone for NativeReader {
    fn clone(&self) -> Self {
        Self {
            file: self.file.try_clone().unwrap(),
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_data_ParquetNative_open<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    let path: String = env.get_string(&path).unwrap().into();
    let file = File::open(path).unwrap();
    let reader = NativeReader { file };
    let reader = Box::new(reader);
    Box::leak(reader) as *mut _ as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_close<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
) {
    let _: Box<NativeReader> = Box::from_raw(reader as *mut _);
}

fn new_array_list<'local>(env: &mut JNIEnv<'local>) -> JObject<'local> {
    env.new_object("Ljava/util/ArrayList;", "()V", &[]).unwrap()
}

fn add_array_list<'local>(env: &mut JNIEnv<'local>, list: &JObject<'local>, obj: &JObject<'local>) {
    env.call_method(list, "add", "(Ljava/lang/Object;)Z", &[JValue::Object(obj)])
        .unwrap();
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_getColumns<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
) -> jobject {
    let reader = reader as *mut NativeReader;
    let reader = reader.as_ref().unwrap().clone();

    let list = new_array_list(&mut env);

    let reader = ParquetRecordBatchReaderBuilder::try_new(reader.file)
        .unwrap()
        .build()
        .unwrap();

    for f in reader.schema().fields() {
        let name = env.new_string(f.name()).unwrap();
        add_array_list(&mut env, &list, &name);
    }
    list.into_raw()
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_getColumn<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
    name: JString<'local>,
) -> jobject {
    let reader = reader as *mut NativeReader;
    let reader = reader.as_ref().unwrap().clone();

    let name: String = env.get_string(&name).unwrap().into();

    let list = new_array_list(&mut env);

    let reader = ParquetRecordBatchReaderBuilder::try_new(reader.file)
        .unwrap()
        .build()
        .unwrap();

    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column_by_name(&name).unwrap();
        for buffer in col.to_data().buffers() {
            let len = buffer.len();
            let obj: JObject = match col.data_type() {
                DataType::Boolean => {
                    let arr = env.new_boolean_array(len as _).unwrap();
                    env.set_boolean_array_region(&arr, 0, buffer.typed_data())
                        .unwrap();
                    arr.into()
                }
                DataType::Int8 | DataType::UInt8 => {
                    let arr = env.new_byte_array(len as _).unwrap();
                    env.set_byte_array_region(&arr, 0, buffer.typed_data())
                        .unwrap();
                    arr.into()
                }
                DataType::Int16 | DataType::UInt16 => {
                    let arr = env.new_short_array(len as _).unwrap();
                    env.set_short_array_region(&arr, 0, buffer.typed_data())
                        .unwrap();
                    arr.into()
                }
                DataType::Int32 | DataType::UInt32 => {
                    let arr = env.new_int_array(len as _).unwrap();
                    env.set_int_array_region(&arr, 0, buffer.typed_data())
                        .unwrap();
                    arr.into()
                }
                DataType::Int64 | DataType::UInt64 => {
                    let arr = env.new_long_array(len as _).unwrap();
                    env.set_long_array_region(&arr, 0, buffer.typed_data())
                        .unwrap();
                    arr.into()
                }
                DataType::Float32 => {
                    let arr = env.new_float_array(len as _).unwrap();
                    env.set_float_array_region(&arr, 0, buffer.typed_data())
                        .unwrap();
                    arr.into()
                }
                DataType::Float64 => {
                    let arr = env.new_double_array(len as _).unwrap();
                    env.set_double_array_region(&arr, 0, buffer.typed_data())
                        .unwrap();
                    arr.into()
                }
                _ => continue,
            };
            add_array_list(&mut env, &list, &obj)
        }
    }
    list.into_raw()
}

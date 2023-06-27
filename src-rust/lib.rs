#![allow(clippy::missing_safety_doc)]

use arrow_array::RecordBatchReader;
use arrow_buffer::ArrowNativeType;
use arrow_data::Buffers;
use arrow_schema::DataType;
use jni::{
    objects::{JClass, JObject, JPrimitiveArray, JString, JValue, TypeArray},
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

fn concat_buffers<'local, T: TypeArray + ArrowNativeType>(
    buffers: Buffers,
    env: &JNIEnv<'local>,
) -> JObject<'local>
where
    Vec<T>: ToJPrimitiveArray<T>,
{
    let mut vec: Vec<T> = vec![];
    for buffer in buffers {
        vec.extend_from_slice(buffer.typed_data());
    }
    vec.to_primitive_array(env).into()
}

trait ToJPrimitiveArray<T: TypeArray + ArrowNativeType> {
    fn to_primitive_array<'local>(&self, env: &JNIEnv<'local>) -> JPrimitiveArray<'local, T>;
}

macro_rules! impl_to_primitive_array {
    ($t: ty, $new_method: ident, $set_method: ident) => {
        impl ToJPrimitiveArray<$t> for Vec<$t> {
            fn to_primitive_array<'local>(
                &self,
                env: &JNIEnv<'local>,
            ) -> JPrimitiveArray<'local, $t> {
                let arr = env.$new_method(self.len() as _).unwrap();
                env.$set_method(&arr, 0, self.as_slice()).unwrap();
                arr
            }
        }
    };
}

impl_to_primitive_array!(u8, new_boolean_array, set_boolean_array_region);
impl_to_primitive_array!(i8, new_byte_array, set_byte_array_region);
impl_to_primitive_array!(i16, new_short_array, set_short_array_region);
impl_to_primitive_array!(i32, new_int_array, set_int_array_region);
impl_to_primitive_array!(i64, new_long_array, set_long_array_region);
impl_to_primitive_array!(f32, new_float_array, set_float_array_region);
impl_to_primitive_array!(f64, new_double_array, set_double_array_region);

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
        let data = col.to_data();
        let buffers = data.buffers();
        let obj = match col.data_type() {
            DataType::Boolean => concat_buffers::<u8>(buffers, &env),
            DataType::Int8 | DataType::UInt8 => concat_buffers::<i8>(buffers, &env),
            DataType::Int16 | DataType::UInt16 => concat_buffers::<i16>(buffers, &env),
            DataType::Int32 | DataType::UInt32 => concat_buffers::<i32>(buffers, &env),
            DataType::Int64 | DataType::UInt64 => concat_buffers::<i64>(buffers, &env),
            DataType::Float32 => concat_buffers::<f32>(buffers, &env),
            DataType::Float64 => concat_buffers::<f64>(buffers, &env),
            _ => continue,
        };
        add_array_list(&mut env, &list, &obj)
    }
    list.into_raw()
}

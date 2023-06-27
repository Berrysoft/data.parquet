#![allow(clippy::missing_safety_doc)]

use arrow_array::{
    new_empty_array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, RecordBatchReader,
};
use arrow_buffer::{bit_iterator::BitIterator, ArrowNativeType};
use arrow_data::Buffers;
use arrow_schema::{DataType, Field, Schema};
use jni::{
    objects::{JClass, JList, JMap, JObject, JPrimitiveArray, JString, TypeArray},
    sys::{jlong, jobject},
    JNIEnv,
};
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    ArrowWriter,
};
use std::{fs::File, sync::Arc};

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
pub extern "system" fn Java_data_ParquetNative_openReader<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    let path: String = env.get_string(&path).unwrap().into();
    let file = File::open(path).unwrap();
    let reader = NativeReader { file };
    Box::leak(Box::new(reader)) as *mut _ as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_closeReader<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
) {
    let _: Box<NativeReader> = Box::from_raw(reader as *mut _);
}

fn new_array_list<'local>(env: &mut JNIEnv<'local>) -> JObject<'local> {
    env.new_object("Ljava/util/ArrayList;", "()V", &[]).unwrap()
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_getColumns<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
) -> jobject {
    let reader = reader as *mut NativeReader;
    let reader = reader.as_ref().unwrap().clone();

    let list_raw = new_array_list(&mut env);
    let list = JList::from_env(&mut env, &list_raw).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(reader.file)
        .unwrap()
        .build()
        .unwrap();

    for f in reader.schema().fields() {
        let name = env.new_string(f.name()).unwrap();
        list.add(&mut env, &name).unwrap();
    }
    list_raw.into_raw()
}

fn concat_buffers_bool<'local>(
    buffers: Buffers,
    env: &JNIEnv<'local>,
    len: usize,
) -> JObject<'local> {
    let mut buf = vec![];
    for buffer in buffers {
        buf.extend_from_slice(buffer.as_slice());
    }
    let vec = BitIterator::new(&buf, 0, len)
        .map(|bit| bit as u8)
        .collect::<Vec<_>>();
    vec.to_primitive_array(env).into()
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

struct NativeColumn {
    name: String,
    reader: ParquetRecordBatchReader,
}

impl NativeColumn {
    pub fn next<'local>(&mut self, env: &JNIEnv<'local>) -> Option<JObject<'local>> {
        self.reader.next().map(|batch| {
            let batch = batch.unwrap();
            let col = batch.column_by_name(&self.name).unwrap();
            let data = col.to_data();
            let buffers = data.buffers();
            match col.data_type() {
                DataType::Boolean => concat_buffers_bool(buffers, env, data.len()),
                DataType::Int8 | DataType::UInt8 => concat_buffers::<i8>(buffers, env),
                DataType::Int16 | DataType::UInt16 => concat_buffers::<i16>(buffers, env),
                DataType::Int32 | DataType::UInt32 => concat_buffers::<i32>(buffers, env),
                DataType::Int64 | DataType::UInt64 => concat_buffers::<i64>(buffers, env),
                DataType::Float32 => concat_buffers::<f32>(buffers, env),
                DataType::Float64 => concat_buffers::<f64>(buffers, env),
                _ => JObject::null(),
            }
        })
    }
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_getColumn<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
    name: JString<'local>,
) -> jlong {
    let reader = reader as *mut NativeReader;
    let reader = reader.as_ref().unwrap().clone();

    let name: String = env.get_string(&name).unwrap().into();

    let reader = ParquetRecordBatchReaderBuilder::try_new(reader.file)
        .unwrap()
        .build()
        .unwrap();

    let reader = NativeColumn { name, reader };
    Box::leak(Box::new(reader)) as *mut _ as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_closeColumn<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    col: jlong,
) {
    let _: Box<NativeColumn> = Box::from_raw(col as *mut _);
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_columnNext<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    col: jlong,
) -> jobject {
    let col = col as *mut NativeColumn;
    let col = col.as_mut().unwrap();
    col.next(&env).unwrap_or_default().into_raw()
}

struct NativeWriter {
    schema: Arc<Schema>,
    writer: ArrowWriter<File>,
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_openWriter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    schema: JObject<'local>,
) -> jlong {
    let schema = JMap::from_env(&mut env, &schema).unwrap();

    let mut fields: Vec<Field> = vec![];

    let mut schema_iter = schema.iter(&mut env).unwrap();
    while let Some((key, ty)) = schema_iter.next(&mut env).unwrap() {
        let key = env.auto_local(JString::from_raw(key.into_raw()));
        let ty = env.auto_local(JClass::from_raw(ty.into_raw()));

        let key: String = env.get_string(&key).unwrap().into();

        let ty_name = JString::from_raw(
            env.call_method(&ty, "getTypeName", "()Ljava/lang/String;", &[])
                .unwrap()
                .l()
                .unwrap()
                .into_raw(),
        );
        let ty_name: String = env.get_string(&ty_name).unwrap().into();
        let ty = match ty_name.as_str() {
            "java.lang.Boolean" => DataType::Boolean,
            "java.lang.Byte" => DataType::Int8,
            "java.lang.Short" => DataType::Int16,
            "java.lang.Integer" => DataType::Int32,
            "java.lang.Long" => DataType::Int64,
            "java.lang.Float" => DataType::Float32,
            "java.lang.Double" => DataType::Float64,
            _ => DataType::Null,
        };
        fields.push(Field::new(key, ty, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let path: String = env.get_string(&path).unwrap().into();
    let file = File::create(path).unwrap();
    let writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    let writer = NativeWriter { schema, writer };
    Box::leak(Box::new(writer)) as *mut _ as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_closeWriter<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    writer: jlong,
) {
    let writer: Box<NativeWriter> = Box::from_raw(writer as *mut _);
    writer.writer.close().unwrap();
}

trait FromJObject {
    fn from_jobject<'local>(env: &mut JNIEnv<'local>, obj: &JObject<'local>) -> Self;
}

macro_rules! impl_from_jobject {
    ($t: ty, $unbox_method: expr, $unbox_sig: expr, $unbox: ident) => {
        impl FromJObject for $t {
            fn from_jobject<'local>(env: &mut JNIEnv<'local>, obj: &JObject<'local>) -> Self {
                env.call_method(obj, $unbox_method, $unbox_sig, &[])
                    .unwrap()
                    .$unbox()
                    .unwrap()
            }
        }
    };
}

impl_from_jobject!(bool, "booleanValue", "()Z", z);
impl_from_jobject!(i8, "byteValue", "()B", b);
impl_from_jobject!(i16, "shortValue", "()S", s);
impl_from_jobject!(i32, "integerValue", "()I", i);
impl_from_jobject!(i64, "longValue", "()J", j);
impl_from_jobject!(f32, "floatValue", "()F", f);
impl_from_jobject!(f64, "doubleValue", "()D", d);

#[no_mangle]
pub unsafe extern "system" fn Java_data_ParquetNative_writeRow<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    writer: jlong,
    values: JObject<'local>,
) {
    let writer = writer as *mut NativeWriter;
    let writer = writer.as_mut().unwrap();

    let schema = writer.schema.clone();
    let mut columns = schema
        .fields()
        .iter()
        .map(|f| new_empty_array(f.data_type()))
        .collect::<Vec<_>>();

    let values = JMap::from_env(&mut env, &values).unwrap();
    let mut values_iter = values.iter(&mut env).unwrap();
    while let Some((key, value)) = values_iter.next(&mut env).unwrap() {
        let key = env.auto_local(JString::from_raw(key.into_raw()));
        let value = env.auto_local(value);

        let key: String = env.get_string(&key).unwrap().into();
        let (index, field) = schema.fields().find(&key).unwrap();

        match field.data_type() {
            DataType::Boolean => {
                columns[index] = Arc::new(BooleanArray::from(vec![bool::from_jobject(
                    &mut env, &value,
                )]));
            }
            DataType::Int8 => {
                columns[index] = Arc::new(Int8Array::from(vec![i8::from_jobject(&mut env, &value)]))
            }
            DataType::Int16 => {
                columns[index] =
                    Arc::new(Int16Array::from(vec![i16::from_jobject(&mut env, &value)]))
            }
            DataType::Int32 => {
                columns[index] =
                    Arc::new(Int32Array::from(vec![i32::from_jobject(&mut env, &value)]))
            }
            DataType::Int64 => {
                columns[index] =
                    Arc::new(Int64Array::from(vec![i64::from_jobject(&mut env, &value)]))
            }
            DataType::Float32 => {
                columns[index] = Arc::new(Float32Array::from(vec![f32::from_jobject(
                    &mut env, &value,
                )]))
            }
            DataType::Float64 => {
                columns[index] = Arc::new(Float64Array::from(vec![f64::from_jobject(
                    &mut env, &value,
                )]))
            }
            _ => {}
        }
    }

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    writer.writer.write(&batch).unwrap();
}

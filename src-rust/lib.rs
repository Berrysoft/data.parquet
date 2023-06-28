#![allow(clippy::missing_safety_doc)]

use arrow_array::{
    new_empty_array, Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, RecordBatchReader,
};
use arrow_buffer::{bit_iterator::BitIterator, ArrowNativeType};
use arrow_data::Buffers;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use jni::{
    errors::{Exception, ToException},
    objects::{JClass, JList, JMap, JObject, JPrimitiveArray, JString, JValue, TypeArray},
    sys::{jlong, jobject},
    JNIEnv,
};
use parquet::{
    arrow::{
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        ArrowWriter,
    },
    errors::ParquetError,
};
use std::{fs::File, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParquetNativeError {
    #[error(transparent)]
    JNI(#[from] jni::errors::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error("the native pointer is null")]
    Null,
    #[error("type {0} is not supported")]
    UnsupportedType(String),
    #[error("key not found")]
    KeyNotFound,
}

impl ToException for ParquetNativeError {
    fn to_exception(&self) -> Exception {
        let class = match self {
            Self::JNI(_)
            | Self::IO(_)
            | Self::Parquet(_)
            | Self::Arrow(_)
            | Self::UnsupportedType(_)
            | Self::KeyNotFound => "java/lang/RuntimeException",
            Self::Null => "java/lang/NullPointerException",
        };
        Exception {
            class: class.to_string(),
            msg: self.to_string(),
        }
    }
}

pub type ParquetNativeResult<T> = Result<T, ParquetNativeError>;

struct NativeReader {
    file: File,
}

impl NativeReader {
    pub fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            file: self.file.try_clone()?,
        })
    }
}

fn open_reader<'local>(
    env: &mut JNIEnv<'local>,
    path: JString<'local>,
) -> ParquetNativeResult<jlong> {
    let path: String = env.get_string(&path)?.into();
    let file = File::open(path)?;
    let reader = NativeReader { file };
    Ok(Box::leak(Box::new(reader)) as *mut _ as jlong)
}

#[no_mangle]
pub extern "system" fn Java_berrysoft_data_ParquetNative_openReader<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    open_reader(&mut env, path)
        .map_err(|e| env.throw(e.to_exception()))
        .unwrap_or_default()
}

#[no_mangle]
pub unsafe extern "system" fn Java_berrysoft_data_ParquetNative_closeReader<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
) {
    if reader != 0 {
        let _: Box<NativeReader> = Box::from_raw(reader as *mut _);
    }
}

fn new_array_list<'local>(env: &mut JNIEnv<'local>) -> ParquetNativeResult<JObject<'local>> {
    Ok(env.new_object("Ljava/util/ArrayList;", "()V", &[])?)
}

fn get_columns<'local>(
    env: &mut JNIEnv<'local>,
    reader: jlong,
) -> ParquetNativeResult<JObject<'local>> {
    let reader = reader as *mut NativeReader;
    let reader = unsafe { reader.as_ref() }
        .ok_or(ParquetNativeError::Null)?
        .try_clone()?;

    let list_raw = new_array_list(env)?;
    let list = JList::from_env(env, &list_raw)?;

    let reader = ParquetRecordBatchReaderBuilder::try_new(reader.file)?.build()?;

    for f in reader.schema().fields() {
        let name = env.new_string(f.name())?;
        list.add(env, &name)?;
    }
    Ok(list_raw)
}

#[no_mangle]
pub unsafe extern "system" fn Java_berrysoft_data_ParquetNative_getColumns<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
) -> jobject {
    get_columns(&mut env, reader)
        .map_err(|e| env.throw(e.to_exception()))
        .unwrap_or_default()
        .into_raw()
}

fn concat_buffers_bool<'local>(
    buffers: Buffers,
    env: &JNIEnv<'local>,
    len: usize,
) -> ParquetNativeResult<JObject<'local>> {
    let buf = buffers
        .into_iter()
        .flat_map(|buffer| buffer.as_slice())
        .copied()
        .collect::<Vec<_>>();
    let vec = BitIterator::new(&buf, 0, len)
        .map(|bit| bit as u8)
        .collect::<Vec<_>>();
    Ok(vec.to_primitive_array(env)?.into())
}

fn concat_buffers<'local, T: TypeArray + ArrowNativeType>(
    buffers: Buffers,
    env: &JNIEnv<'local>,
) -> ParquetNativeResult<JObject<'local>>
where
    Vec<T>: ToJPrimitiveArray<T>,
{
    let vec = buffers
        .into_iter()
        .flat_map(|buffer| buffer.typed_data::<T>())
        .copied()
        .collect::<Vec<_>>();
    Ok(vec.to_primitive_array(env)?.into())
}

trait ToJPrimitiveArray<T: TypeArray + ArrowNativeType> {
    fn to_primitive_array<'local>(
        &self,
        env: &JNIEnv<'local>,
    ) -> ParquetNativeResult<JPrimitiveArray<'local, T>>;
}

macro_rules! impl_to_primitive_array {
    ($t: ty, $new_method: ident, $set_method: ident) => {
        impl ToJPrimitiveArray<$t> for Vec<$t> {
            fn to_primitive_array<'local>(
                &self,
                env: &JNIEnv<'local>,
            ) -> ParquetNativeResult<JPrimitiveArray<'local, $t>> {
                let arr = env.$new_method(self.len() as _)?;
                env.$set_method(&arr, 0, self.as_slice())?;
                Ok(arr)
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
    pub fn next<'local>(
        &mut self,
        env: &JNIEnv<'local>,
    ) -> Option<ParquetNativeResult<JObject<'local>>> {
        self.reader.next().map(|batch| {
            let batch = batch?;
            let col = batch
                .column_by_name(&self.name)
                .ok_or(ParquetNativeError::KeyNotFound)?;
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
                _ => unreachable!(),
            }
        })
    }
}

fn get_column<'local>(
    env: &mut JNIEnv<'local>,
    reader: jlong,
    name: JString<'local>,
) -> ParquetNativeResult<jlong> {
    let reader = reader as *mut NativeReader;
    let reader = unsafe { reader.as_ref() }
        .ok_or(ParquetNativeError::Null)?
        .try_clone()?;

    let name: String = env.get_string(&name)?.into();

    let reader = ParquetRecordBatchReaderBuilder::try_new(reader.file)?.build()?;

    let reader = NativeColumn { name, reader };
    Ok(Box::leak(Box::new(reader)) as *mut _ as jlong)
}

#[no_mangle]
pub unsafe extern "system" fn Java_berrysoft_data_ParquetNative_getColumn<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    reader: jlong,
    name: JString<'local>,
) -> jlong {
    get_column(&mut env, reader, name)
        .map_err(|e| env.throw(e.to_exception()))
        .unwrap_or_default()
}

#[no_mangle]
pub unsafe extern "system" fn Java_berrysoft_data_ParquetNative_closeColumn<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    col: jlong,
) {
    if col != 0 {
        let _: Box<NativeColumn> = Box::from_raw(col as *mut _);
    }
}

fn column_next<'local>(
    env: &mut JNIEnv<'local>,
    col: jlong,
) -> ParquetNativeResult<JObject<'local>> {
    let col = col as *mut NativeColumn;
    let col = unsafe { col.as_mut() }.ok_or(ParquetNativeError::Null)?;
    col.next(env).unwrap_or_else(|| Ok(JObject::null()))
}

#[no_mangle]
pub unsafe extern "system" fn Java_berrysoft_data_ParquetNative_columnNext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    col: jlong,
) -> jobject {
    column_next(&mut env, col)
        .map_err(|e| env.throw(e.to_exception()))
        .unwrap_or_default()
        .into_raw()
}

fn get_class_name<'local>(
    env: &mut JNIEnv<'local>,
    class: &JClass<'local>,
) -> ParquetNativeResult<String> {
    let ty_name = env
        .call_method(class, "getName", "()Ljava/lang/String;", &[])?
        .l()?;
    let ty_name = unsafe { JString::from_raw(ty_name.into_raw()) };
    let ty_name = env.get_string(&ty_name)?.into();
    Ok(ty_name)
}

struct NativeWriter {
    schema: Arc<Schema>,
    writer: ArrowWriter<File>,
}

fn open_writer<'local>(
    env: &mut JNIEnv<'local>,
    path: JString<'local>,
    schema: JObject<'local>,
) -> ParquetNativeResult<jlong> {
    let schema = JMap::from_env(env, &schema)?;

    let mut fields: Vec<Field> = vec![];

    let mut schema_iter = schema.iter(env)?;
    while let Some((key, ty)) = schema_iter.next(env)? {
        let key = env.auto_local(unsafe { JString::from_raw(key.into_raw()) });
        let ty = env.auto_local(unsafe { JClass::from_raw(ty.into_raw()) });

        let key: String = env.get_string(&key)?.into();
        let ty_name = get_class_name(env, &ty)?;

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

    let path: String = env.get_string(&path)?.into();
    let file = File::create(path)?;
    let writer = ArrowWriter::try_new(file, schema.clone(), None)?;
    let writer = NativeWriter { schema, writer };
    Ok(Box::leak(Box::new(writer)) as *mut _ as jlong)
}

#[no_mangle]
pub unsafe extern "system" fn Java_berrysoft_data_ParquetNative_openWriter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    schema: JObject<'local>,
) -> jlong {
    open_writer(&mut env, path, schema)
        .map_err(|e| env.throw(e.to_exception()))
        .unwrap_or_default()
}

#[no_mangle]
pub unsafe extern "system" fn Java_berrysoft_data_ParquetNative_closeWriter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    writer: jlong,
) {
    if writer != 0 {
        let writer: Box<NativeWriter> = Box::from_raw(writer as *mut _);
        if let Err(e) = writer.writer.close() {
            env.throw(ParquetNativeError::Parquet(e).to_exception())
                .ok();
        }
    }
}

trait FromJObject: Sized {
    type TArray: Array;

    fn from_jobject<'local>(
        env: &mut JNIEnv<'local>,
        obj: &JObject<'local>,
    ) -> ParquetNativeResult<Self>;
}

macro_rules! impl_from_jobject {
    ($t: ty, $arrt: ty, $unbox_method: expr, $unbox_sig: expr, $unbox: ident) => {
        impl FromJObject for $t {
            type TArray = $arrt;

            fn from_jobject<'local>(
                env: &mut JNIEnv<'local>,
                obj: &JObject<'local>,
            ) -> ParquetNativeResult<Self> {
                Ok(env
                    .call_method(obj, $unbox_method, $unbox_sig, &[])?
                    .$unbox()?)
            }
        }
    };
}

impl_from_jobject!(bool, BooleanArray, "booleanValue", "()Z", z);
impl_from_jobject!(i8, Int8Array, "byteValue", "()B", b);
impl_from_jobject!(i16, Int16Array, "shortValue", "()S", s);
impl_from_jobject!(i32, Int32Array, "integerValue", "()I", i);
impl_from_jobject!(i64, Int64Array, "longValue", "()J", j);
impl_from_jobject!(f32, Float32Array, "floatValue", "()F", f);
impl_from_jobject!(f64, Float64Array, "doubleValue", "()D", d);

fn is_instance<'local>(
    env: &mut JNIEnv<'local>,
    obj: &JObject<'local>,
    ty: &str,
) -> ParquetNativeResult<bool> {
    let ty = env.find_class(ty)?;
    Ok(env
        .call_method(
            &ty,
            "isInstance",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(obj)],
        )?
        .z()?)
}

fn from_jobject<'local, T: FromJObject>(
    env: &mut JNIEnv<'local>,
    obj: &JObject<'local>,
) -> ParquetNativeResult<Arc<dyn Array>>
where
    T::TArray: From<Vec<T>> + 'static,
{
    let vec = if is_instance(env, obj, "clojure/lang/Seqable")? {
        let mut seq = env
            .call_method(obj, "seq", "()Lclojure/lang/ISeq;", &[])?
            .l()?;
        let mut res = vec![];
        while !seq.is_null() {
            let obj = env
                .call_method(&seq, "first", "()Ljava/lang/Object;", &[])?
                .l()?;
            res.push(T::from_jobject(env, &obj)?);
            seq = env
                .call_method(&seq, "next", "()Lclojure/lang/ISeq;", &[])?
                .l()?;
        }
        res
    } else {
        vec![T::from_jobject(env, obj)?]
    };
    Ok(Arc::new(T::TArray::from(vec)))
}

fn write_row<'local>(
    env: &mut JNIEnv<'local>,
    writer: jlong,
    values: JObject<'local>,
) -> ParquetNativeResult<()> {
    let writer = writer as *mut NativeWriter;
    let writer = unsafe { writer.as_mut() }.ok_or(ParquetNativeError::Null)?;

    let schema = writer.schema.clone();
    let mut columns = schema
        .fields()
        .iter()
        .map(|f| new_empty_array(f.data_type()))
        .collect::<Vec<_>>();

    let values = JMap::from_env(env, &values)?;
    let mut values_iter = values.iter(env)?;
    while let Some((key, value)) = values_iter.next(env)? {
        let key = env.auto_local(unsafe { JString::from_raw(key.into_raw()) });
        let value = env.auto_local(value);

        let key: String = env.get_string(&key)?.into();
        let (index, field) = schema
            .fields()
            .find(&key)
            .ok_or(ParquetNativeError::KeyNotFound)?;

        match field.data_type() {
            DataType::Boolean => columns[index] = from_jobject::<bool>(env, &value)?,
            DataType::Int8 => columns[index] = from_jobject::<i8>(env, &value)?,
            DataType::Int16 => columns[index] = from_jobject::<i16>(env, &value)?,
            DataType::Int32 => columns[index] = from_jobject::<i32>(env, &value)?,
            DataType::Int64 => columns[index] = from_jobject::<i64>(env, &value)?,
            DataType::Float32 => columns[index] = from_jobject::<f32>(env, &value)?,
            DataType::Float64 => columns[index] = from_jobject::<f64>(env, &value)?,
            _ => {}
        }
    }

    let batch = RecordBatch::try_new(schema, columns)?;
    writer.writer.write(&batch)?;
    Ok(())
}

#[no_mangle]
pub unsafe extern "system" fn Java_berrysoft_data_ParquetNative_writeRow<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    writer: jlong,
    values: JObject<'local>,
) {
    write_row(&mut env, writer, values)
        .map_err(|e| env.throw(e.to_exception()))
        .ok();
}

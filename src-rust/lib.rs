use jni::{objects::JClass, sys::jstring, JNIEnv};

#[no_mangle]
pub extern "system" fn Java_berrysoft_data_ParquetNative_hello<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let str = env.new_string("Hello world!").unwrap();
    str.into_raw()
}

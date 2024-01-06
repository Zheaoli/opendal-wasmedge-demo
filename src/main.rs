use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use opendal::{Operator, Scheme};
use serde::Deserialize;
use std::mem::drop;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::Mutex;
use url_parse::core::Parser;
use wasmedge_sdk::{
    async_host_function,
    config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions},
    error::HostFuncError,
    types::ExternRef,
    wasi::r#async::{AsyncState, WasiContext},
    Caller, ImportObjectBuilder, Module, NeverType, VmBuilder, WasmValue,
};

lazy_static! {
    static ref PROFILE_HASH: Mutex<HashMap<String, Operator>> = { Mutex::new(HashMap::new()) };
}
static PARSER: OnceCell<Parser> = OnceCell::new();

pub fn mutu8sclice<'a>(caller: &Caller, ptr: i32, len: i32) -> Option<&'a mut [u8]> {
    if let Ok(mem) = caller
        .memory(0)
        .unwrap()
        .data_pointer_mut(ptr as u32, len as u32)
    {
        Some(unsafe { std::slice::from_raw_parts_mut(mem, len as usize) })
    } else {
        None
    }
}

pub fn u8slice<'a>(caller: &Caller, ptr: i32, len: i32) -> &'a [u8] {
    // tracing::info!("getting u8 slice");
    let mem = caller
        .memory(0)
        .unwrap()
        .data_pointer(ptr as u32, len as u32)
        .unwrap();
    let res = unsafe { std::slice::from_raw_parts(mem, len as usize) };
    // tracing::info!("got u8 slice");
    res
}

pub fn mutref<'a, T: Sized>(caller: &Caller, ptr: i32) -> &'a mut T {
    unsafe {
        &mut *(caller
            .memory(0)
            .unwrap()
            .data_pointer_mut(ptr as u32, std::mem::size_of::<T>() as u32)
            .unwrap() as *mut T)
    }
}

// profile_name, profile_name_length, file_name, filename_len
type StatArgs = (i32, i32, i32, i32);
#[async_host_function]
async fn stat(_caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let profile_name =
        std::str::from_utf8(u8slice(&_caller, args[0].to_i32(), args[1].to_i32())).unwrap();
    let file_name =
        std::str::from_utf8(u8slice(&_caller, args[2].to_i32(), args[3].to_i32())).unwrap();
    let map = PROFILE_HASH.lock().await;
    if map.contains_key(profile_name) {
        let metadata = map
            .get(profile_name)
            .unwrap()
            .stat(file_name)
            .await
            .unwrap();
        drop(map);
        Ok(vec![WasmValue::from_i32(metadata.content_length() as i32)])
    } else {
        drop(map);
        Ok(vec![WasmValue::from_i32(-1)])
    }
}

// profile_name, profile_name_length, file_name, filename_len, result, result_len, readlen
type ReadFileArgs = (i32, i32, i32, i32, i32, i32, i32);
#[async_host_function]
async fn read(_caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let profile_name =
        std::str::from_utf8(u8slice(&_caller, args[0].to_i32(), args[1].to_i32())).unwrap();
    let file_name =
        std::str::from_utf8(u8slice(&_caller, args[2].to_i32(), args[3].to_i32())).unwrap();
    let retlen = mutref::<i32>(&_caller, args[6].to_i32());
    let map = PROFILE_HASH.lock().await;
    let data = mutu8sclice(&_caller, args[4].to_i32(), args[5].to_i32()).unwrap();
    if map.contains_key(profile_name) {
        let result = map
            .get(profile_name)
            .unwrap()
            .read(file_name)
            .await
            .unwrap();
        *retlen = result.len() as i32;
        data[..result.len()].copy_from_slice(&result);
        drop(map);
        Ok(vec![WasmValue::from_i32(0)])
    } else {
        drop(map);
        Ok(vec![WasmValue::from_i32(-1)])
    }
}
// profile_name, profile_name_length,file_name, filename_len, result, result_len
type WriteFileArgs = (i32, i32, i32, i32, i32, i32);
#[async_host_function]
async fn write(_caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let profile_name =
        std::str::from_utf8(u8slice(&_caller, args[0].to_i32(), args[1].to_i32())).unwrap();
    let file_name =
        std::str::from_utf8(u8slice(&_caller, args[2].to_i32(), args[3].to_i32())).unwrap();
    let map = PROFILE_HASH.lock().await;
    let data = mutu8sclice(&_caller, args[4].to_i32(), args[5].to_i32()).unwrap();
    if map.contains_key(profile_name) {
        let _ = map
            .get(profile_name)
            .unwrap()
            .write(file_name, data.to_vec())
            .await
            .unwrap();
        drop(map);
        Ok(vec![WasmValue::from_i32(0)])
    } else {
        drop(map);
        Ok(vec![WasmValue::from_i32(-1)])
    }
}

#[derive(Deserialize, Debug)]
struct OpenDALConfig {
    #[serde(flatten)]
    tables: HashMap<String, HashMap<String, String>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut open_dal_config =
        toml::from_str::<OpenDALConfig>(&std::fs::read_to_string("opendal.toml")?)?;
    let mut map = PROFILE_HASH.lock().await;
    let mut scheme_mappings = HashMap::new();
    let mut index = 8888;
    for (table_name, table_config) in open_dal_config.tables.iter_mut() {
        let mut table_config = table_config.clone();
        let origin_scheme = table_config.remove("scheme").unwrap();
        let scheme = Scheme::from_str(&origin_scheme).unwrap();
        let mut operator = Operator::via_map(scheme, table_config).unwrap();
        operator = operator.layer(
            opendal::layers::BlockingLayer::create().expect("blocking layer must be created"),
        );
        map.insert(table_name.clone(), operator);
        unsafe {
            scheme_mappings.insert(
                &*Box::leak(table_name.clone().into_boxed_str()),
                (index as u32, &*Box::leak(origin_scheme.into_boxed_str())),
            );
        }
        index += 1;
    }
    drop(map);
    PARSER.get_or_init(|| Parser::new(Some(scheme_mappings)));
    let import = ImportObjectBuilder::new()
        .with_async_func::<StatArgs, i32, NeverType>("custom_stat", stat, None)?
        .with_async_func::<ReadFileArgs, i32, NeverType>("custom_read", read, None)?
        .with_async_func::<WriteFileArgs, i32, NeverType>("custom_write", write, None)?
        .build::<NeverType>("env", None)?;
    let config = ConfigBuilder::new(CommonConfigOptions::default())
        .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
        .build()
        .expect("failed to create config");
    let module =
        Module::from_file(Some(&config), "wasm/target/wasm32-wasi/debug/demo.wasm").unwrap();
    let wasi_ctx = WasiContext::default();
    let mut vm = VmBuilder::new()
        .with_config(config)
        .with_wasi_context(wasi_ctx)
        .build()?;
    let async_state = AsyncState::new();

    vm.register_import_module(&import)?;
    vm = vm.register_module(Some("demo"), module)?;
    vm.run_func_async(&async_state, Some("demo"), "_start", vec![])
        .await?;
    Ok(())
}

use rocket::{get, routes, State};
use crate::Result;
use crate::server::Server;

pub async fn run_http_server(server: Server) -> Result<()> {
    rocket::build()
        .mount("/", routes![search, add_test_data])
        .manage(server)
        .launch()
        .await?;

    Ok(())
}

#[get("/search")]
fn search(server: &State<Server>) {
    log::info!("Accept search request");
}

#[get("/test_data")]
fn add_test_data(server: &State<Server>) {
    use serde_json::Value;

    let json_str = "{\
  \"string\": \"这是一个字符串\",\
  \"u64\": 18446744073709551615,\
  \"i64\": -123,\
  \"f64\": -1.23,\
  \"boolean\": true,\
  \"null\": null,\
  \"array\": [1, 2, 3],\
  \"object\": {\
    \"key1\": \"value1\"\
  }\
}";
    let json = serde_json::from_str::<Value>(json_str).unwrap();
    let mut v = Vec::new();

    for i in 0..100 {
        v.push(json.clone());
    }

    let value = Value::Array(v);

    match server.ingest(&"test1".to_string(), value) {
        Ok(_) => {
            log::info!("add test data for stream test1");
        }
        Err(e) => {
            log::error!("add test data failed: {}", e)
        }
    }
}
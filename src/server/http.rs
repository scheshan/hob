use rocket::{get, routes, State};
use crate::Result;
use crate::server::Server;

pub async fn run_http_server(server: Server) -> Result<()> {
    rocket::build()
        .mount("/", routes![search])
        .manage(server)
        .launch()
        .await?;

    Ok(())
}

#[get("/search")]
fn search(server: &State<Server>) {
    log::info!("Accept search request");
}
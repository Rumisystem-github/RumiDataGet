use sqlx::{mysql::MySqlPool};
use axum::{
	extract::Path,
	body::{Body},
	http::{header, HeaderMap, StatusCode},
	response::{IntoResponse, Response},
	routing::get,
	Router
};
use once_cell::sync::OnceCell;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
struct FileInfo {
	FILE: Option<i64>
}

#[derive(Debug, serde::Deserialize)]
struct FilePath {
	bucket: String,
	name: String
}

static SQL_POOL: OnceCell<MySqlPool> = OnceCell::new();

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
	let database_url = "mysql://data_server:aya442377@192.168.0.130:3306/DataServer";

	let pool = MySqlPool::connect(database_url).await?;
	SQL_POOL.set(pool).map_err(|_| {
		eprintln!("エラー");

		sqlx::Error::Io(std::io::ErrorKind::Other.into())
	})?;

	let app = Router::new();
	let app = app.route("/{bucket}/{*name}", get(root));

	let listener = tokio::net::TcpListener::bind("0.0.0.0:3007").await.unwrap();
	axum::serve(listener, app).await.unwrap();

	Ok(())
}

fn get_sql_pool() -> &'static MySqlPool {
	SQL_POOL.get().expect("SQL_POOLが初期化されてない")
}

async fn get_file_id(bucket: &str, file_name: &str) -> Option<String> {
	let pool = get_sql_pool();

	let script = "SELECT `FILE` FROM `DATA` WHERE `NAME` = ? AND `BUCKET` = ? AND `PUBLIC` = 1;";
	let query_result:anyhow::Result<Vec<FileInfo>, sqlx::Error> = sqlx::query_as(script)
		.bind(file_name)
		.bind(bucket)
		.fetch_all(pool).await;

	if let Ok(query) = query_result {
		if let Some(row) = query.first() {
			if let Some(file_id) = row.FILE.as_ref() {
				return Some(file_id.to_string());
			}
		}
	}

	None
}

async fn root(Path(FilePath{bucket, name}): Path<FilePath>) -> Response {
	let query = get_file_id(&bucket, &name).await;

	if let Some(file_id) = query {
		let file_path = format!("/home/rumisan/Documents/RDS/{file_id}");
		match File::open(&file_path).await {
			Ok(file) => {
				let stream = ReaderStream::new(file);
				let body = Body::from_stream(stream);
				let header_list = [
					(header::CONTENT_TYPE, "application/octet-stream")
				];

				(StatusCode::OK, header_list, body).into_response()
			},
			Err(_) => {
				let mut header_list = HeaderMap::new();
				header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
				(StatusCode::NOT_FOUND, header_list, format!("ファイル本体が見つかりませんでした（泣）\nバケット名:{bucket}\nファイル名:{name}")).into_response()
			}
		}
	} else {
		let mut header_list = HeaderMap::new();
		header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
		(StatusCode::NOT_FOUND, header_list, format!("ファイルが見つかりませんでした（泣）\nバケット名:{bucket}\nファイル名:{name}")).into_response()
	}
}


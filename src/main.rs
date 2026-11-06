use anyhow::Context;
use chrono::Local;
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
use tokio::{fs::File, io::AsyncReadExt};
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
static DATA_DIR: OnceCell<String> = OnceCell::new();

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
	//.envがあれば読み込む（既に環境変数で設定されてる値は上書きしない）
	let _ = dotenvy::dotenv();

	let database_url = std::env::var("DATABASE_URL")
		.context("DATABASE_URLが設定されてない（.envか環境変数で設定して）")?;

	//ファイル実体の置き場所（.envのDATA_DIR、無ければ従来のパス）
	let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/mnt/DATA/RDS".to_string());
	let _ = DATA_DIR.set(data_dir);

	let pool = MySqlPool::connect(&database_url).await?;
	SQL_POOL.set(pool).map_err(|_| {
		eprintln!("エラー");

		sqlx::Error::Io(std::io::ErrorKind::Other.into())
	})?;

	let app = Router::new();
	let app = app.route("/{bucket}/{*name}", get(root));

	//待ち受けアドレス（.envのBIND_ADDR、無ければ従来の0.0.0.0:3007）
	let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3007".to_string());
	let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
	axum::serve(listener, app).await.unwrap();

	Ok(())
}

//ファイル実体の置き場所を拾ってくる
fn get_data_dir() -> &'static str {
	DATA_DIR.get().map(|s| s.as_str()).unwrap_or("/mnt/DATA/RDS")
}

//MySqlPoolを拾ってくる
fn get_sql_pool() -> &'static MySqlPool {
	SQL_POOL.get().expect("SQL_POOLが初期化されてない")
}

//ファイル名→ID
async fn get_file_id(bucket: &str, file_name: &str) -> Option<String> {
	let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
	let systemd_ok = "[  \x1b[32mOK  \x1b[0m] ";
	let space = "         ";

	let pool = get_sql_pool();
	let script = "SELECT `FILE` FROM `DATA` WHERE `NAME` = ? AND `BUCKET` = ? AND `PUBLIC` = 1;";
	let query_result:anyhow::Result<Vec<FileInfo>, sqlx::Error> = sqlx::query_as(script)
		.bind(file_name)
		.bind(bucket)
		.fetch_all(pool).await;

	if let Ok(query) = query_result {
		if let Some(row) = query.first() {
			if let Some(file_id) = row.FILE.as_ref() {
				let file_id_str = file_id.to_string();

				println!("{systemd_ok}┬{timestamp}");
				println!("{space}├BUCKET:{bucket}");
				println!("{space}├NAME:{file_name}");
				println!("{space}├SOURCE:SQL");
				println!("{space}└ID:{file_id}");

				return Some(file_id_str);
			}
		}
	}

	None
}

//マジックナンバーからMIMEタイプを引っ張ってくるのです
async fn get_mimetype(file_path: &str) -> &'static str {
	let mut file = match File::open(file_path).await {
		Ok(f) => f,
		Err(_) => return "application/octet-stream"
	};

	let mut buffer = [0u8; 12];
	let bytes_read = match file.read(&mut buffer).await {
		Ok(n) => n,
		Err(_) => return "application/octet-stream"
	};

	let slice = &buffer[..bytes_read];

	if slice.len() >= 8 && &slice[0..8] == &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A] {
		//PNG
		return "image/png";
	} else if slice.len() >= 3 && &slice[0..3] == &[0xFF, 0xD8, 0xFF] {
		//JPG
		return "image/jpeg";
	} else if slice.len() >= 12 && &slice[0..4] == b"RIFF" && &slice[8..12] == b"WEBP" {
		//WebP
		return "image/webp";
	} else if slice.len() >= 6 && (&slice[0..6] == b"GIF87a" || &slice[0..6] == b"GIF89a") {
		//GIF
		"image/gif"
	} else if slice.len() >= 5 && &slice[0..5] == b"%PDF-" {
		//PDF
		"application/pdf"
	} else if slice.len() >= 12 && &slice[4..8] == b"ftyp" {
		//MP4
		"video/mp4"
	} else if slice.len() >= 4 && &slice[0..4] == [0x1A, 0x45, 0xDF, 0xA3] {
		//簡易判定です。
		//中に「webm」があればWebM、なければMKV
		if slice.windows(4).any(|w| w == b"webm") {
			"video/webm"
		} else {
			"video/x-matroska"
		}
	} else {
		"application/octet-stream"
	}
}

//HTTPリクエストでアクセスするばしょ
async fn root(Path(FilePath{bucket, name}): Path<FilePath>) -> Response {
	let query = get_file_id(&bucket, &name).await;

	if let Some(file_id) = query {
		//println!("[  \x1b[32mOK  \x1b[0m]BUCKET:{bucket} NAME:{name} -> {file_id}");

		let file_path = format!("{}/{file_id}", get_data_dir());
		match File::open(&file_path).await {
			Ok(file) => {
				//メモリ破壊マン回避
				let stream = ReaderStream::new(file);
				let body = Body::from_stream(stream);
				let header_list = [
					(header::CONTENT_TYPE, get_mimetype(&file_path).await)
				];

				(StatusCode::OK, header_list, body).into_response()
			},
			Err(_) => {
				//ファイルを開けなかった
				let mut header_list = HeaderMap::new();
				header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
				(StatusCode::NOT_FOUND, header_list, format!("ファイル本体が見つかりませんでした（泣）\nバケット名:{bucket}\nファイル名:{name}\nパス:{file_path}")).into_response()
			}
		}
	} else {
		//そんなファイルねーよばーーーーーーーーーか
		let mut header_list = HeaderMap::new();
		header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
		(StatusCode::NOT_FOUND, header_list, format!("ファイルが見つかりませんでした（泣）\nバケット名:{bucket}\nファイル名:{name}")).into_response()
	}
}

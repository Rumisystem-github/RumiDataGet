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
use lru::LruCache;
use std::{num::NonZeroUsize, sync::{LazyLock, Mutex}};

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
static LRU_CACHE: LazyLock<Mutex<LruCache<String, String>>> = LazyLock::new(|| {
	Mutex::new(LruCache::new(NonZeroUsize::new(100).unwrap()))
});

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

//MySqlPoolを拾ってくる
fn get_sql_pool() -> &'static MySqlPool {
	SQL_POOL.get().expect("SQL_POOLが初期化されてない")
}

//ファイル名→ID
async fn get_file_id(bucket: &str, file_name: &str) -> Option<String> {
	let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
	let systemd_ok = "[  \x1b[32mOK  \x1b[0m] ";
	let space = "         ";

	if let Some(file_id_str) = LRU_CACHE.lock().unwrap().get(&format!("{bucket}+{file_name}")) {
		println!("{systemd_ok}┬{timestamp}");
		println!("{space}├BUCKET:{bucket}");
		println!("{space}├NAME:{file_name}");
		println!("{space}├SOURCE:CACHE");
		println!("{space}└ID:{file_id_str}");

		return Some(file_id_str.to_string());
	} else {
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

					let mut cache = LRU_CACHE.lock().unwrap();
					cache.put(format!("{bucket}+{file_name}"), file_id_str.clone());

					println!("{systemd_ok}┬{timestamp}");
					println!("{space}├BUCKET:{bucket}");
					println!("{space}├NAME:{file_name}");
					println!("{space}├SOURCE:SQL");
					println!("{space}└ID:{file_id}");

					return Some(file_id_str);
				}
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

		let file_path = format!("/home/rumisan/Documents/RDS/{file_id}");
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
				(StatusCode::NOT_FOUND, header_list, format!("ファイル本体が見つかりませんでした（泣）\nバケット名:{bucket}\nファイル名:{name}")).into_response()
			}
		}
	} else {
		//そんなファイルねーよばーーーーーーーーーか
		let mut header_list = HeaderMap::new();
		header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
		(StatusCode::NOT_FOUND, header_list, format!("ファイルが見つかりませんでした（泣）\nバケット名:{bucket}\nファイル名:{name}")).into_response()
	}
}


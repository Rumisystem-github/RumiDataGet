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
use tokio::{fs::File, io::{AsyncReadExt, AsyncSeekExt}};
use tokio_util::io::ReaderStream;
use moka::future::Cache;
use std::time::Duration;

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
struct FileInfo {
	#[sqlx(rename = "FILE")]
	file: Option<i64>
}

#[derive(Debug, serde::Deserialize)]
struct FilePath {
	bucket: String,
	name: String
}

static SQL_POOL: OnceCell<MySqlPool> = OnceCell::new();
static DATA_DIR: OnceCell<String> = OnceCell::new();
static FILE_CACHE: OnceCell<Option<Cache<(String, String), Option<String>>>> = OnceCell::new();

//ファイル読み取りのバッファサイズ（読み取り回数を減らす）
const STREAM_BUFFER_SIZE: usize = 128 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	//.envがあれば読み込む（既に環境変数で設定されてる値は上書きしない）
	let _ = dotenvy::dotenv();

	let database_url = std::env::var("DATABASE_URL")
		.context("DATABASE_URLが設定されてない（.envか環境変数で設定して）")?;

	//ファイル実体の置き場所（.envのDATA_DIR、無ければ従来のパス）
	let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "/mnt/DATA/RDS".to_string());
	let _ = DATA_DIR.set(data_dir);

	//ファイルID解決のキャッシュ保持秒数（.envのCACHE_TTL_SEC、無ければ30。0で無効）
	let cache_ttl: u64 = std::env::var("CACHE_TTL_SEC")
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(30);
	let cache = if cache_ttl > 0 {
		Some(
			Cache::builder()
				.max_capacity(10_000)
				.time_to_live(Duration::from_secs(cache_ttl))
				.build(),
		)
	} else {
		None
	};
	let _ = FILE_CACHE.set(cache);

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

//ログ出力用に制御文字を取り除く
fn log_safe(text: &str) -> String {
	text.chars().filter(|c| !c.is_control()).collect()
}

//MySqlPoolを拾ってくる
fn get_sql_pool() -> &'static MySqlPool {
	SQL_POOL.get().expect("SQL_POOLが初期化されてない")
}

//ファイルID解決のキャッシュを拾ってくる（無効な場合はNone）
fn get_file_cache() -> Option<&'static Cache<(String, String), Option<String>>> {
	FILE_CACHE.get().and_then(|cache| cache.as_ref())
}

//アクセスログを1回でまとめて書く（並行リクエストでログ行が混ざる対策も兼ねる）
fn log_access(bucket: &str, file_name: &str, source: &str, file_id: &str) {
	let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
	let systemd_ok = "[  \x1b[32mOK  \x1b[0m] ";
	let space = "         ";
	let bucket = log_safe(bucket);
	let file_name = log_safe(file_name);

	println!("{systemd_ok}┬{timestamp}\n{space}├BUCKET:{bucket}\n{space}├NAME:{file_name}\n{space}├SOURCE:{source}\n{space}└ID:{file_id}");
}

//ファイル名→ID
async fn get_file_id(bucket: &str, file_name: &str) -> Option<String> {
	//キャッシュにあればDBを引かずに返す（見つからなかった結果もキャッシュする）
	if let Some(cache) = get_file_cache() {
		if let Some(cached) = cache.get(&(bucket.to_string(), file_name.to_string())).await {
			if let Some(file_id_str) = &cached {
				log_access(bucket, file_name, "CACHE", file_id_str);
			}
			return cached;
		}
	}

	let pool = get_sql_pool();
	let script = "SELECT `FILE` FROM `DATA` WHERE `NAME` = ? AND `BUCKET` = ? AND `PUBLIC` = 1 LIMIT 1;";
	let query_result:anyhow::Result<Vec<FileInfo>, sqlx::Error> = sqlx::query_as(script)
		.bind(file_name)
		.bind(bucket)
		.fetch_all(pool).await;

	let mut result = None;

	if let Ok(query) = query_result {
		if let Some(row) = query.first() {
			if let Some(file_id) = row.file.as_ref() {
				let file_id_str = file_id.to_string();

				log_access(bucket, file_name, "SQL", &file_id_str);

				result = Some(file_id_str);
			}
		}
	}

	//DBを引いた結果はキャッシュに入れる（None＝見つからなかった、も入れる）
	if let Some(cache) = get_file_cache() {
		cache.insert((bucket.to_string(), file_name.to_string()), result.clone()).await;
	}

	result
}

//マジックナンバーからMIMEタイプを引っ張ってくるのです（開いてあるファイルの先頭を読む）
async fn get_mimetype(file: &mut File) -> &'static str {
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
async fn root(Path(FilePath{bucket, name}): Path<FilePath>, headers: HeaderMap) -> Response {
	let query = get_file_id(&bucket, &name).await;

	if let Some(file_id) = query {
		//println!("[  \x1b[32mOK  \x1b[0m]BUCKET:{bucket} NAME:{name} -> {file_id}");

		let file_path = format!("{}/{file_id}", get_data_dir());
		match File::open(&file_path).await {
			Ok(mut file) => {
				let file_size = file.metadata().await.map(|m| m.len()).unwrap_or(0);
				let mime = get_mimetype(&mut file).await;
				let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());

				match parse_range(range_header, file_size) {
					RangeResult::Partial(start, end) => {
						//範囲指定あり（206 Partial Content）
						let len = end - start + 1;
						match partial_body(file, start, len).await {
							Ok(body) => {
								let mut header_list = HeaderMap::new();
								header_list.insert(header::CONTENT_TYPE, mime.parse().unwrap());
								header_list.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
								header_list.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
								header_list.insert(header::CONTENT_RANGE, format!("bytes {start}-{end}/{file_size}").parse().unwrap());
								header_list.insert(header::CONTENT_LENGTH, len.to_string().parse().unwrap());
								(StatusCode::PARTIAL_CONTENT, header_list, body).into_response()
							},
							Err(_) => {
								let mut header_list = HeaderMap::new();
								header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
								header_list.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
								(StatusCode::INTERNAL_SERVER_ERROR, header_list, "ファイルの読み込みに失敗しました\n").into_response()
							}
						}
					},
					RangeResult::NotSatisfiable => {
						//範囲がファイルの範囲外（416 Range Not Satisfiable）
						let mut header_list = HeaderMap::new();
						header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
						header_list.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
						header_list.insert(header::CONTENT_RANGE, format!("bytes */{file_size}").parse().unwrap());
						(StatusCode::RANGE_NOT_SATISFIABLE, header_list, format!("指定された範囲がファイルサイズを超えています\nバケット名:{bucket}\nファイル名:{name}")).into_response()
					},
					RangeResult::Full => {
						//マジックナンバー読みで進んだ読み取り位置を先頭に戻す
						let _ = file.seek(std::io::SeekFrom::Start(0)).await;
						//メモリ破壊マン回避（バッファを大きくして読み取り回数を減らす）
						let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
						let body = Body::from_stream(stream);
						let mut header_list = HeaderMap::new();
						header_list.insert(header::CONTENT_TYPE, mime.parse().unwrap());
						header_list.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
						header_list.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
						header_list.insert(header::CONTENT_LENGTH, file_size.to_string().parse().unwrap());

						(StatusCode::OK, header_list, body).into_response()
					}
				}
			},
			Err(_) => {
				//ファイルを開けなかった
				let mut header_list = HeaderMap::new();
				header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
				header_list.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
				(StatusCode::NOT_FOUND, header_list, format!("ファイル本体が見つかりませんでした（泣）\nバケット名:{bucket}\nファイル名:{name}")).into_response()
			}
		}
	} else {
		//そんなファイルねーよばーーーーーーーーーか
		let mut header_list = HeaderMap::new();
		header_list.insert(header::CONTENT_TYPE, "text/plain; charset=UTF-8".parse().unwrap());
		header_list.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
		(StatusCode::NOT_FOUND, header_list, format!("ファイルが見つかりませんでした（泣）\nバケット名:{bucket}\nファイル名:{name}")).into_response()
	}
}

//Rangeヘッダーの解釈結果
#[derive(Debug, PartialEq, Eq)]
enum RangeResult {
	//範囲指定なし（ファイル全体を返す）
	Full,
	//範囲あり（開始位置, 終了位置）※終了位置を含む
	Partial(u64, u64),
	//範囲がファイルの範囲外で返せない（416）
	NotSatisfiable
}

//Rangeヘッダーを解釈する
//対応するのは単一範囲のみ。複数範囲や不正な書式の場合はヘッダー自体を無視して全体を返す（RFC 9110で許可された挙動）
fn parse_range(range_header: Option<&str>, file_size: u64) -> RangeResult {
	let Some(range_header) = range_header else {
		return RangeResult::Full;
	};
	let Some(spec) = range_header.strip_prefix("bytes=") else {
		return RangeResult::Full;
	};

	//複数範囲（bytes=0-99,200-299）は未対応なので無視
	if spec.contains(',') {
		return RangeResult::Full;
	}

	let Some((start_str, end_str)) = spec.split_once('-') else {
		return RangeResult::Full;
	};

	if start_str.is_empty() {
		//末尾Nバイト指定（bytes=-N）
		let Ok(suffix_len) = end_str.parse::<u64>() else {
			return RangeResult::Full;
		};
		if suffix_len == 0 || file_size == 0 {
			return RangeResult::NotSatisfiable;
		}
		let len = suffix_len.min(file_size);
		RangeResult::Partial(file_size - len, file_size - 1)
	} else {
		let Ok(start) = start_str.parse::<u64>() else {
			return RangeResult::Full;
		};
		if start >= file_size {
			return RangeResult::NotSatisfiable;
		}
		if end_str.is_empty() {
			//開始位置から最後まで（bytes=N-）
			RangeResult::Partial(start, file_size - 1)
		} else {
			let Ok(end) = end_str.parse::<u64>() else {
				return RangeResult::Full;
			};
			if end < start {
				//不正な範囲は無視
				return RangeResult::Full;
			}
			RangeResult::Partial(start, end.min(file_size - 1))
		}
	}
}

//ファイルの途中から指定バイト数だけを返すBodyを作る
async fn partial_body(mut file: File, start: u64, len: u64) -> std::io::Result<Body> {
	file.seek(std::io::SeekFrom::Start(start)).await?;
	Ok(Body::from_stream(ReaderStream::with_capacity(file.take(len), STREAM_BUFFER_SIZE)))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn range_none_is_full() {
		assert_eq!(parse_range(None, 1000), RangeResult::Full);
	}

	#[test]
	fn range_normal() {
		assert_eq!(parse_range(Some("bytes=0-99"), 1000), RangeResult::Partial(0, 99));
		assert_eq!(parse_range(Some("bytes=100-199"), 1000), RangeResult::Partial(100, 199));
	}

	#[test]
	fn range_open_end() {
		assert_eq!(parse_range(Some("bytes=500-"), 1000), RangeResult::Partial(500, 999));
	}

	#[test]
	fn range_suffix() {
		assert_eq!(parse_range(Some("bytes=-100"), 1000), RangeResult::Partial(900, 999));
		//ファイルサイズより大きい末尾指定は全体になる
		assert_eq!(parse_range(Some("bytes=-2000"), 1000), RangeResult::Partial(0, 999));
	}

	#[test]
	fn range_end_clamped() {
		assert_eq!(parse_range(Some("bytes=0-9999"), 1000), RangeResult::Partial(0, 999));
	}

	#[test]
	fn range_not_satisfiable() {
		assert_eq!(parse_range(Some("bytes=1000-"), 1000), RangeResult::NotSatisfiable);
		assert_eq!(parse_range(Some("bytes=-0"), 1000), RangeResult::NotSatisfiable);
		assert_eq!(parse_range(Some("bytes=0-"), 0), RangeResult::NotSatisfiable);
	}

	#[test]
	fn range_invalid_is_ignored() {
		assert_eq!(parse_range(Some("bytes=abc-def"), 1000), RangeResult::Full);
		assert_eq!(parse_range(Some("bytes=200-100"), 1000), RangeResult::Full);
		assert_eq!(parse_range(Some("items=0-100"), 1000), RangeResult::Full);
		//複数範囲は無視して全体
		assert_eq!(parse_range(Some("bytes=0-99,200-299"), 1000), RangeResult::Full);
	}

	#[tokio::test]
	async fn partial_body_reads_middle() {
		let path = std::env::temp_dir().join(format!("rumi_range_test_{}.bin", std::process::id()));
		std::fs::write(&path, b"0123456789").unwrap();

		let file = File::open(&path).await.unwrap();
		let body = partial_body(file, 2, 3).await.unwrap();
		let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();

		assert_eq!(&bytes[..], b"234");
		let _ = std::fs::remove_file(&path);
	}

	#[tokio::test]
	async fn mimetype_reads_open_file() {
		let path = std::env::temp_dir().join(format!("rumi_mime_test_{}.bin", std::process::id()));
		std::fs::write(&path, [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00]).unwrap();

		let mut file = File::open(&path).await.unwrap();
		assert_eq!(get_mimetype(&mut file).await, "image/png");
		let _ = std::fs::remove_file(&path);
	}
}

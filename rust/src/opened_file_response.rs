use std::{
    fs::File,
    io::{self, Seek, SeekFrom},
    time::SystemTime,
};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, Method, StatusCode, header},
    response::Response,
};
use file_identity::{FileVersion, Identity};
use futures_util::stream;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::errors::ApiError;

const FILE_CHUNK_BYTES: usize = 64 * 1024;

pub(super) struct OpenedFile {
    file: File,
    validated_identity: Identity,
    max_bytes: u64,
}

impl OpenedFile {
    pub(super) fn new(file: File, version: FileVersion, max_bytes: u64) -> Result<Self, ApiError> {
        if version.size > max_bytes || file_identity::version(&file).ok() != Some(version) {
            return Err(ApiError::unavailable());
        }
        Ok(Self {
            file,
            validated_identity: version.identity,
            max_bytes,
        })
    }

    fn current_metadata(&self) -> Result<CurrentFileMetadata, ApiError> {
        let first = file_identity::version(&self.file).map_err(|_| ApiError::unavailable())?;
        if first.identity != self.validated_identity || first.size > self.max_bytes {
            return Err(ApiError::unavailable());
        }
        let modified = self
            .file
            .metadata()
            .and_then(|metadata| metadata.modified())
            .map_err(|_| ApiError::unavailable())?;
        let second = file_identity::version(&self.file).map_err(|_| ApiError::unavailable())?;
        if second != first {
            return Err(ApiError::unavailable());
        }
        Ok(CurrentFileMetadata {
            size: first.size,
            modified,
            etag: file_etag(first),
        })
    }
}

struct CurrentFileMetadata {
    size: u64,
    modified: SystemTime,
    etag: String,
}

fn file_etag(version: FileVersion) -> String {
    let mut digest = Sha256::new();
    digest.update(version.identity.first.to_le_bytes());
    digest.update(version.identity.second.to_le_bytes());
    digest.update(version.size.to_le_bytes());
    digest.update(version.modified.to_le_bytes());
    digest.update(version.changed.to_le_bytes());
    format!(
        "\"{}\"",
        digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

#[derive(Debug, Eq, PartialEq)]
enum RangeError {
    Malformed(&'static str),
    Unsatisfiable,
}

fn parse_ranges(value: &str, file_size: u64) -> Result<Vec<(u64, u64)>, RangeError> {
    let Some((units, requested)) = value.split_once('=') else {
        return Err(RangeError::Malformed("Malformed range header."));
    };
    if !units.trim().eq_ignore_ascii_case("bytes") {
        return Err(RangeError::Malformed("Only support bytes range"));
    }

    let file_size = i128::from(file_size);
    let mut ranges = Vec::new();
    for part in requested.split(',').map(str::trim) {
        if part.is_empty() || part == "-" {
            continue;
        }
        let Some((start_text, end_text)) = part.split_once('-') else {
            continue;
        };
        let start_text = start_text.trim();
        let end_text = end_text.trim();
        let parsed = if start_text.is_empty() {
            end_text
                .parse::<i128>()
                .ok()
                .map(|suffix| ((file_size - suffix).max(0), file_size))
        } else {
            let Some(start) = start_text.parse::<i128>().ok() else {
                continue;
            };
            if end_text.is_empty() {
                Some((start, file_size))
            } else {
                end_text.parse::<i128>().ok().map(|raw_end| {
                    let end = if raw_end < file_size {
                        raw_end.saturating_add(1)
                    } else {
                        file_size
                    };
                    (start, end)
                })
            }
        };
        if let Some(range) = parsed {
            ranges.push(range);
        }
    }
    if ranges.is_empty() {
        return Err(RangeError::Malformed(
            "Range header: range must be requested",
        ));
    }
    if ranges
        .iter()
        .any(|(start, _)| *start < 0 || *start >= file_size)
    {
        return Err(RangeError::Unsatisfiable);
    }
    if ranges.iter().any(|(start, end)| start > end) {
        return Err(RangeError::Malformed(
            "Range header: start must be less than end",
        ));
    }

    let mut ranges = ranges
        .into_iter()
        .map(|(start, end)| {
            (
                u64::try_from(start).expect("validated range start"),
                u64::try_from(end).expect("validated range end"),
            )
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    let mut merged = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some((_, previous_end)) = merged.last_mut()
            && start <= *previous_end
        {
            *previous_end = (*previous_end).max(end);
        } else {
            merged.push((start, end));
        }
    }
    Ok(merged)
}

struct FileStreamState {
    file: tokio::fs::File,
    remaining: u64,
}

fn file_body(mut file: File, start: u64, remaining: u64) -> Result<Body, ApiError> {
    file.seek(SeekFrom::Start(start))
        .map_err(|_| ApiError::unavailable())?;
    let chunks = stream::unfold(
        Some(FileStreamState {
            file: tokio::fs::File::from_std(file),
            remaining,
        }),
        |state| async move {
            let mut state = state?;
            if state.remaining == 0 {
                return None;
            }
            let requested = usize::try_from(state.remaining.min(FILE_CHUNK_BYTES as u64))
                .expect("bounded file chunk");
            let mut chunk = vec![0; requested];
            match state.file.read(&mut chunk).await {
                Ok(0) => Some((
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "file changed while streaming",
                    )),
                    None,
                )),
                Ok(read) => {
                    chunk.truncate(read);
                    state.remaining -= read as u64;
                    Some((Ok(Bytes::from(chunk)), Some(state)))
                }
                Err(error) => Some((Err(error), None)),
            }
        },
    );
    Ok(Body::from_stream(chunks))
}

fn part_header(
    boundary: &str,
    content_type: &str,
    start: u64,
    end: u64,
    file_size: u64,
) -> Vec<u8> {
    format!(
        "--{boundary}\r\nContent-Type: {content_type}\r\nContent-Range: bytes {start}-{}/{file_size}\r\n\r\n",
        end.saturating_sub(1)
    )
    .into_bytes()
}

#[derive(Clone, Copy)]
enum MultipartPhase {
    Header,
    Data(u64),
    Separator,
    Closing,
}

struct MultipartStreamState {
    file: tokio::fs::File,
    ranges: Vec<(u64, u64)>,
    boundary: String,
    content_type: String,
    file_size: u64,
    index: usize,
    phase: MultipartPhase,
}

fn multipart_body(
    file: File,
    ranges: Vec<(u64, u64)>,
    boundary: String,
    content_type: String,
    file_size: u64,
) -> Body {
    let chunks = stream::unfold(
        Some(MultipartStreamState {
            file: tokio::fs::File::from_std(file),
            ranges,
            boundary,
            content_type,
            file_size,
            index: 0,
            phase: MultipartPhase::Header,
        }),
        |state| async move {
            let mut state = state?;
            loop {
                match state.phase {
                    MultipartPhase::Header => {
                        if state.index >= state.ranges.len() {
                            state.phase = MultipartPhase::Closing;
                            continue;
                        }
                        let (start, end) = state.ranges[state.index];
                        if let Err(error) = state.file.seek(SeekFrom::Start(start)).await {
                            return Some((Err(error), None));
                        }
                        state.phase = MultipartPhase::Data(end - start);
                        let header = part_header(
                            &state.boundary,
                            &state.content_type,
                            start,
                            end,
                            state.file_size,
                        );
                        return Some((Ok(Bytes::from(header)), Some(state)));
                    }
                    MultipartPhase::Data(remaining) => {
                        if remaining == 0 {
                            state.phase = MultipartPhase::Separator;
                            continue;
                        }
                        let requested = usize::try_from(remaining.min(FILE_CHUNK_BYTES as u64))
                            .expect("bounded multipart chunk");
                        let mut chunk = vec![0; requested];
                        match state.file.read(&mut chunk).await {
                            Ok(0) => {
                                return Some((
                                    Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "file changed while streaming",
                                    )),
                                    None,
                                ));
                            }
                            Ok(read) => {
                                chunk.truncate(read);
                                state.phase = MultipartPhase::Data(remaining - read as u64);
                                return Some((Ok(Bytes::from(chunk)), Some(state)));
                            }
                            Err(error) => return Some((Err(error), None)),
                        }
                    }
                    MultipartPhase::Separator => {
                        state.index += 1;
                        state.phase = if state.index < state.ranges.len() {
                            MultipartPhase::Header
                        } else {
                            MultipartPhase::Closing
                        };
                        return Some((Ok(Bytes::from_static(b"\r\n")), Some(state)));
                    }
                    MultipartPhase::Closing => {
                        let closing = format!("--{}--", state.boundary);
                        return Some((Ok(Bytes::from(closing)), None));
                    }
                }
            }
        },
    );
    Body::from_stream(chunks)
}

fn multipart_content_length(
    ranges: &[(u64, u64)],
    boundary: &str,
    content_type: &str,
    file_size: u64,
) -> Result<u64, ApiError> {
    let mut total = u64::try_from(boundary.len() + 4).map_err(|_| ApiError::unavailable())?;
    for &(start, end) in ranges {
        let header = part_header(boundary, content_type, start, end, file_size);
        total = total
            .checked_add(u64::try_from(header.len()).map_err(|_| ApiError::unavailable())?)
            .and_then(|value| value.checked_add(end - start))
            .and_then(|value| value.checked_add(2))
            .ok_or_else(ApiError::unavailable)?;
    }
    Ok(total)
}

fn boundary() -> Result<String, ApiError> {
    let mut entropy = [0_u8; 13];
    getrandom::getrandom(&mut entropy).map_err(|_| ApiError::unavailable())?;
    Ok(entropy.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn malformed_range(message: &'static str) -> Result<Response, ApiError> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CONTENT_LENGTH, message.len().to_string())
        .body(Body::from(message))
        .map_err(|_| ApiError::unavailable())
}

fn unsatisfiable_range(file_size: u64) -> Result<Response, ApiError> {
    Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header(header::CONTENT_RANGE, format!("bytes */{file_size}"))
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CONTENT_LENGTH, "0")
        .body(Body::empty())
        .map_err(|_| ApiError::unavailable())
}

struct DownloadHeaders<'a> {
    content_type: &'a str,
    content_disposition: &'a str,
    content_length: u64,
    content_range: Option<String>,
    last_modified: &'a str,
    etag: &'a str,
}

fn download_response(
    status: StatusCode,
    headers: DownloadHeaders<'_>,
    body: Body,
) -> Result<Response, ApiError> {
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, headers.content_type)
        .header(header::CONTENT_DISPOSITION, headers.content_disposition)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, headers.content_length.to_string())
        .header(header::LAST_MODIFIED, headers.last_modified)
        .header(header::ETAG, headers.etag);
    if let Some(content_range) = headers.content_range {
        builder = builder.header(header::CONTENT_RANGE, content_range);
    }
    builder.body(body).map_err(|_| ApiError::unavailable())
}

pub(super) fn respond(
    opened: OpenedFile,
    method: &Method,
    request_headers: &HeaderMap,
    content_type: &str,
    content_disposition: &str,
) -> Result<Response, ApiError> {
    let current = opened.current_metadata()?;
    let last_modified = httpdate::fmt_http_date(current.modified);
    let range = request_headers.get(header::RANGE);
    let if_range_matches = request_headers
        .get(header::IF_RANGE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value == current.etag || value == last_modified);

    let Some(range) = range.filter(|_| if_range_matches) else {
        let body = if *method == Method::HEAD {
            Body::empty()
        } else {
            file_body(opened.file, 0, current.size)?
        };
        return download_response(
            StatusCode::OK,
            DownloadHeaders {
                content_type,
                content_disposition,
                content_length: current.size,
                content_range: None,
                last_modified: &last_modified,
                etag: &current.etag,
            },
            body,
        );
    };
    let range = range
        .to_str()
        .map_err(|_| RangeError::Malformed("Malformed range header."));
    let ranges = match range.and_then(|value| parse_ranges(value, current.size)) {
        Ok(ranges) => ranges,
        Err(RangeError::Malformed(message)) => return malformed_range(message),
        Err(RangeError::Unsatisfiable) => return unsatisfiable_range(current.size),
    };

    if ranges.len() == 1 {
        let (start, end) = ranges[0];
        let body = if *method == Method::HEAD {
            Body::empty()
        } else {
            file_body(opened.file, start, end - start)?
        };
        return download_response(
            StatusCode::PARTIAL_CONTENT,
            DownloadHeaders {
                content_type,
                content_disposition,
                content_length: end - start,
                content_range: Some(format!(
                    "bytes {start}-{}/{}",
                    end.saturating_sub(1),
                    current.size
                )),
                last_modified: &last_modified,
                etag: &current.etag,
            },
            body,
        );
    }

    let boundary = boundary()?;
    let multipart_type = format!("multipart/byteranges; boundary={boundary}");
    let content_length = multipart_content_length(&ranges, &boundary, content_type, current.size)?;
    let body = if *method == Method::HEAD {
        Body::empty()
    } else {
        multipart_body(
            opened.file,
            ranges,
            boundary,
            content_type.to_owned(),
            current.size,
        )
    };
    download_response(
        StatusCode::PARTIAL_CONTENT,
        DownloadHeaders {
            content_type: &multipart_type,
            content_disposition,
            content_length,
            content_range: None,
            last_modified: &last_modified,
            etag: &current.etag,
        },
        body,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use axum::http::HeaderValue;
    use http_body_util::BodyExt;

    use super::*;

    static NEXT_TEST_FILE: AtomicUsize = AtomicUsize::new(0);

    struct TestFile {
        path: PathBuf,
    }

    impl TestFile {
        fn create() -> Self {
            let project_root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("project root");
            let temp_root = project_root.join(".local/codex/tmp");
            fs::create_dir_all(&temp_root).expect("project-local response test root");
            let path = temp_root.join(format!(
                "chatgpt2api-opened-response-{}-{}",
                std::process::id(),
                NEXT_TEST_FILE.fetch_add(1, Ordering::Relaxed)
            ));
            assert!(path.starts_with(project_root.join(".local/codex")));
            Self { path }
        }

        fn with_bytes(bytes: &[u8]) -> Self {
            let file = Self::create();
            let mut output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&file.path)
                .expect("create response test file");
            output.write_all(bytes).expect("write response test file");
            output.sync_all().expect("sync response test file");
            file
        }

        fn sparse(size: u64) -> Self {
            let file = Self::create();
            let output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&file.path)
                .expect("create sparse response test file");
            output
                .set_len(size)
                .expect("size sparse response test file");
            output.sync_all().expect("sync sparse response test file");
            file
        }

        fn opened(&self) -> OpenedFile {
            opened(&self.path)
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn opened(path: &Path) -> OpenedFile {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open response test file");
        let version = file_identity::version(&file).expect("response test file version");
        OpenedFile::new(file, version, 512 * 1024 * 1024).expect("validated opened response file")
    }

    fn response(file: &TestFile, method: &Method, headers: &HeaderMap) -> Response {
        respond(
            file.opened(),
            method,
            headers,
            "application/octet-stream",
            "attachment; filename=\"asset.bin\"",
        )
        .expect("opened file response")
    }

    fn range_headers(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static(value));
        headers
    }

    async fn response_bytes(response: Response) -> Vec<u8> {
        response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes()
            .to_vec()
    }

    #[test]
    fn range_parser_matches_public_single_suffix_and_merge_contract() {
        assert_eq!(parse_ranges("bytes=0-5", 11), Ok(vec![(0, 6)]));
        assert_eq!(parse_ranges("bytes=-3", 11), Ok(vec![(8, 11)]));
        assert_eq!(parse_ranges("bytes=-999", 11), Ok(vec![(0, 11)]));
        assert_eq!(parse_ranges("bytes=6-", 11), Ok(vec![(6, 11)]));
        assert_eq!(
            parse_ranges("bytes=0-2,2-5,8-10", 11),
            Ok(vec![(0, 6), (8, 11)])
        );
        assert_eq!(
            parse_ranges("bytes=99-100", 11),
            Err(RangeError::Unsatisfiable)
        );
        assert!(matches!(
            parse_ranges("items=0-1", 11),
            Err(RangeError::Malformed("Only support bytes range"))
        ));
    }

    #[tokio::test]
    async fn opened_file_refreshes_truncated_size_from_the_same_handle() {
        let file = TestFile::with_bytes(b"0123456789A");
        let before = response(&file, &Method::HEAD, &HeaderMap::new());
        let old_etag = before.headers()[header::ETAG].clone();
        assert!(response_bytes(before).await.is_empty());
        let opened = file.opened();
        opened.file.set_len(3).expect("truncate opened handle");

        let mutated_response = respond(
            opened,
            &Method::GET,
            &HeaderMap::new(),
            "application/octet-stream",
            "attachment; filename=\"asset.bin\"",
        )
        .expect("truncated response");
        assert_eq!(mutated_response.headers()[header::CONTENT_LENGTH], "3");
        let new_etag = mutated_response.headers()[header::ETAG].clone();
        assert_ne!(new_etag, old_etag);
        assert_eq!(response_bytes(mutated_response).await, b"012");

        let mut new_validator = range_headers("bytes=0-1");
        new_validator.insert(header::IF_RANGE, new_etag);
        let new_match = response(&file, &Method::GET, &new_validator);
        assert_eq!(new_match.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response_bytes(new_match).await, b"01");

        let mut old_validator = range_headers("bytes=0-1");
        old_validator.insert(header::IF_RANGE, old_etag);
        let old_mismatch = response(&file, &Method::GET, &old_validator);
        assert_eq!(old_mismatch.status(), StatusCode::OK);
        assert_eq!(response_bytes(old_mismatch).await, b"012");
    }

    #[tokio::test]
    async fn opened_file_refreshes_grown_size_from_the_same_handle() {
        let file = TestFile::with_bytes(b"0123456789A");
        let before = response(&file, &Method::HEAD, &HeaderMap::new());
        let old_etag = before.headers()[header::ETAG].clone();
        assert!(response_bytes(before).await.is_empty());
        let mut opened = file.opened();
        opened
            .file
            .seek(SeekFrom::End(0))
            .expect("seek opened handle");
        opened.file.write_all(b"BC").expect("grow opened handle");
        opened.file.sync_all().expect("sync grown opened handle");

        let mutated_response = respond(
            opened,
            &Method::GET,
            &HeaderMap::new(),
            "application/octet-stream",
            "attachment; filename=\"asset.bin\"",
        )
        .expect("grown response");
        assert_eq!(mutated_response.headers()[header::CONTENT_LENGTH], "13");
        let new_etag = mutated_response.headers()[header::ETAG].clone();
        assert_ne!(new_etag, old_etag);
        assert_eq!(response_bytes(mutated_response).await, b"0123456789ABC");

        let mut new_validator = range_headers("bytes=11-12");
        new_validator.insert(header::IF_RANGE, new_etag);
        let new_match = response(&file, &Method::GET, &new_validator);
        assert_eq!(new_match.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response_bytes(new_match).await, b"BC");

        let mut old_validator = range_headers("bytes=11-12");
        old_validator.insert(header::IF_RANGE, old_etag);
        let old_mismatch = response(&file, &Method::GET, &old_validator);
        assert_eq!(old_mismatch.status(), StatusCode::OK);
        assert_eq!(response_bytes(old_mismatch).await, b"0123456789ABC");
    }

    #[tokio::test]
    async fn head_range_if_range_and_public_error_matrix_matches_python() {
        let file = TestFile::with_bytes(b"0123456789A");

        let head_range = response(&file, &Method::HEAD, &range_headers("bytes=0-5"));
        assert_eq!(head_range.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(head_range.headers()[header::CONTENT_LENGTH], "6");
        assert_eq!(head_range.headers()[header::CONTENT_RANGE], "bytes 0-5/11");
        assert!(response_bytes(head_range).await.is_empty());

        let open_ended = response(&file, &Method::GET, &range_headers("bytes=6-"));
        assert_eq!(open_ended.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response_bytes(open_ended).await, b"6789A");

        let suffix = response(&file, &Method::GET, &range_headers("bytes=-3"));
        assert_eq!(suffix.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response_bytes(suffix).await, b"89A");

        let oversized_suffix = response(&file, &Method::GET, &range_headers("bytes=-999"));
        assert_eq!(oversized_suffix.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(oversized_suffix.headers()[header::CONTENT_LENGTH], "11");
        assert_eq!(
            oversized_suffix.headers()[header::CONTENT_RANGE],
            "bytes 0-10/11"
        );
        assert_eq!(response_bytes(oversized_suffix).await, b"0123456789A");

        let metadata = response(&file, &Method::HEAD, &HeaderMap::new());
        let etag = metadata.headers()[header::ETAG].clone();
        let modified = metadata.headers()[header::LAST_MODIFIED].clone();
        assert!(response_bytes(metadata).await.is_empty());

        for validator in [etag, modified] {
            let mut headers = range_headers("bytes=0-2");
            headers.insert(header::IF_RANGE, validator);
            let matched = response(&file, &Method::GET, &headers);
            assert_eq!(matched.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(response_bytes(matched).await, b"012");
        }

        let mut mismatch_headers = range_headers("bytes=0-2");
        mismatch_headers.insert(
            header::IF_RANGE,
            HeaderValue::from_static("\"different-version\""),
        );
        let mismatch = response(&file, &Method::GET, &mismatch_headers);
        assert_eq!(mismatch.status(), StatusCode::OK);
        assert_eq!(response_bytes(mismatch).await, b"0123456789A");

        let malformed = response(&file, &Method::GET, &range_headers("items=0-2"));
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response_bytes(malformed).await, b"Only support bytes range");

        let unsatisfiable = response(&file, &Method::GET, &range_headers("bytes=99-100"));
        assert_eq!(unsatisfiable.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(unsatisfiable.headers()[header::CONTENT_RANGE], "bytes */11");
        assert_eq!(
            unsatisfiable.headers()[header::CONTENT_TYPE],
            "text/plain; charset=utf-8"
        );
        assert!(response_bytes(unsatisfiable).await.is_empty());
    }

    #[tokio::test]
    async fn overlapping_and_multiple_ranges_stream_exact_declared_length() {
        let file = TestFile::with_bytes(b"0123456789A");
        let response = response(&file, &Method::GET, &range_headers("bytes=0-2,2-5,8-10"));
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .expect("multipart content type")
                .starts_with("multipart/byteranges; boundary=")
        );
        let declared = response.headers()[header::CONTENT_LENGTH]
            .to_str()
            .expect("multipart content length")
            .parse::<usize>()
            .expect("numeric multipart content length");
        let body = response_bytes(response).await;
        assert_eq!(body.len(), declared);
        assert!(body.windows(6).any(|window| window == b"012345"));
        assert!(body.windows(3).any(|window| window == b"89A"));
        assert!(body.ends_with(b"--"));
    }

    #[tokio::test]
    async fn truncation_after_response_creation_never_has_a_successful_terminal_body() {
        let file = TestFile::with_bytes(&vec![b'x'; FILE_CHUNK_BYTES * 2]);
        let response = response(&file, &Method::GET, &HeaderMap::new());
        OpenOptions::new()
            .write(true)
            .open(&file.path)
            .expect("open truncation mutator")
            .set_len(FILE_CHUNK_BYTES as u64 + 3)
            .expect("truncate after response creation");

        let result = response.into_body().collect().await;
        assert!(result.is_err(), "early EOF must fail the response body");
    }

    #[tokio::test]
    async fn maximum_editable_file_streams_one_bounded_first_chunk() {
        let file = TestFile::sparse(512 * 1024 * 1024);
        let mut response = response(&file, &Method::GET, &HeaderMap::new());
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            (512_u64 * 1024 * 1024).to_string()
        );
        let frame = response
            .body_mut()
            .frame()
            .await
            .expect("first sparse file frame")
            .expect("successful sparse file frame");
        let bytes = frame.into_data().expect("data frame");
        assert!(!bytes.is_empty());
        assert!(bytes.len() <= FILE_CHUNK_BYTES);
        drop(response);
    }

    #[tokio::test]
    async fn growth_beyond_maximum_is_rejected_before_the_first_body_byte() {
        const MAX_BYTES: u64 = 512 * 1024 * 1024;
        let file = TestFile::sparse(MAX_BYTES);
        let opened = file.opened();
        opened
            .file
            .set_len(MAX_BYTES + 1)
            .expect("grow opened handle over the limit");
        assert!(
            respond(
                opened,
                &Method::GET,
                &HeaderMap::new(),
                "application/octet-stream",
                "attachment; filename=\"asset.bin\"",
            )
            .is_err()
        );

        let oversized = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file.path)
            .expect("open oversized response test file");
        let version = file_identity::version(&oversized).expect("oversized file version");
        assert!(OpenedFile::new(oversized, version, MAX_BYTES).is_err());
    }
}

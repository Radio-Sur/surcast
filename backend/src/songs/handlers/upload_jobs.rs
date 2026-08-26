use axum::extract::{Multipart, Path, State};
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::Serialize;
use sqlx::PgPool;
use std::io::Read;
use uuid::Uuid;

use super::assign::assign_song_to_stations;
use super::upload_helper::is_audio_file;
use crate::auth::middleware::AuthUser;
use crate::config::Config;
use crate::errors::{AppError, DbResult};
use crate::songs::upload::process_song_upload;

#[derive(Serialize)]
pub struct UploadJobCreated {
    pub job_id: Uuid,
}

#[derive(Serialize)]
pub struct UploadJobStatus {
    pub id: Uuid,
    pub status: String,
    pub total: i32,
    pub processed: i32,
    pub failed: i32,
    pub current_file: Option<String>,
    pub error: Option<String>,
    pub song_ids: Vec<Uuid>,
}

pub async fn start_upload(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    State(config): State<Config>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<UploadJobCreated>), AppError> {
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    let mut assign_to_all = false;
    let mut station_ids: Vec<Uuid> = Vec::new();
    let mut title_override: Option<String> = None;
    let mut artist_override: Option<String> = None;
    let mut album_override: Option<String> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::BadRequest(format!("Invalid multipart: {e}")))?
    {
        match field.name().unwrap_or("") {
            "file" => {
                let name = field.file_name().unwrap_or("unknown").to_string();
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| AppError::BadRequest(format!("Failed to read file: {e}")))?
                    .to_vec();
                files.push((name, bytes));
            }
            "title" => title_override = Some(field.text().await.unwrap_or_default()),
            "artist" => artist_override = Some(field.text().await.unwrap_or_default()),
            "album" => album_override = Some(field.text().await.unwrap_or_default()),
            "assign_to_all" => assign_to_all = field.text().await.unwrap_or_default() == "true",
            "station_ids" => {
                let text = field.text().await.unwrap_or_default();
                if !text.is_empty() {
                    station_ids =
                        serde_json::from_str(&text).map_err(|e| AppError::BadRequest(format!("Invalid station_ids JSON: {e}")))?;
                }
            }
            _ => {}
        }
    }

    if files.is_empty() {
        return Err(AppError::BadRequest("No files provided".into()));
    }

    let entries = expand_files(files)?;

    if entries.is_empty() {
        return Err(AppError::BadRequest("No audio files found".into()));
    }

    if assign_to_all {
        station_ids = crate::stations::repository::find_all_station_ids(&db)
            .await?
            .into_iter()
            .map(|r| r.0)
            .collect();
    }

    let single_meta = if single_override(entries.len(), &title_override, &artist_override, &album_override) {
        Some((title_override, artist_override, album_override))
    } else {
        None
    };

    let job_id = create_job(&db, auth_user.id, entries.len() as i32).await?;

    tokio::spawn(run_job(
        db.clone(),
        config.clone(),
        job_id,
        auth_user.id,
        entries,
        station_ids,
        single_meta,
    ));

    Ok((StatusCode::ACCEPTED, Json(UploadJobCreated { job_id })))
}

/// Manual title/artist/album overrides only apply when a single entry is
/// uploaded; for a batch each file keeps its own tag-derived metadata.
fn single_override(count: usize, title: &Option<String>, artist: &Option<String>, album: &Option<String>) -> bool {
    count == 1 && (title.is_some() || artist.is_some() || album.is_some())
}

pub async fn get_upload_job(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<UploadJobStatus>, AppError> {
    let row = sqlx::query_as::<
        _,
        (
            Uuid,
            Uuid,
            String,
            i32,
            i32,
            i32,
            Option<String>,
            Option<String>,
            Option<serde_json::Value>,
        ),
    >(
        "SELECT id, user_id, status, total, processed, failed, current_file, error, song_ids
         FROM upload_jobs WHERE id = $1",
    )
    .bind(job_id)
    .fetch_optional(&db)
    .await
    .db_error("failed to query upload job")?;

    let Some((id, owner_id, status, total, processed, failed, current_file, error, song_ids)) = row else {
        return Err(AppError::NotFound("Upload job not found".into()));
    };

    if owner_id != auth_user.id {
        return Err(AppError::NotFound("Upload job not found".into()));
    }

    let song_ids: Vec<Uuid> = song_ids.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();

    Ok(Json(UploadJobStatus {
        id,
        status,
        total,
        processed,
        failed,
        current_file,
        error,
        song_ids,
    }))
}

fn expand_files(files: Vec<(String, Vec<u8>)>) -> Result<Vec<(String, Vec<u8>)>, AppError> {
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();

    for (name, bytes) in files {
        let is_zip = name.to_lowercase().ends_with(".zip") || (bytes.len() >= 4 && &bytes[..4] == b"PK\x03\x04");

        if is_zip {
            let cursor = std::io::Cursor::new(bytes);
            let mut archive = zip::ZipArchive::new(cursor).map_err(|e| AppError::BadRequest(format!("Invalid zip archive: {e}")))?;

            for i in 0..archive.len() {
                let mut entry = archive.by_index(i).db_error("failed to process uploaded file")?;
                if entry.is_dir() || !is_audio_file(entry.name()) {
                    continue;
                }
                let original_name = entry.name().to_string();
                let mut entry_bytes = Vec::new();
                entry.read_to_end(&mut entry_bytes).db_error("failed to process uploaded file")?;
                entries.push((original_name, entry_bytes));
            }
        } else if is_audio_file(&name) {
            entries.push((name, bytes));
        }
    }

    Ok(entries)
}

async fn create_job(db: &PgPool, user_id: Uuid, total: i32) -> Result<Uuid, AppError> {
    sqlx::query_scalar::<_, Uuid>("INSERT INTO upload_jobs (user_id, total) VALUES ($1, $2) RETURNING id")
        .bind(user_id)
        .bind(total)
        .fetch_one(db)
        .await
        .db_error("failed to create upload job")
}

async fn set_current_file(db: &PgPool, job_id: Uuid, name: &str) {
    let _ = sqlx::query("UPDATE upload_jobs SET current_file = $1, updated_at = now() WHERE id = $2")
        .bind(name)
        .bind(job_id)
        .execute(db)
        .await;
}

async fn update_progress(db: &PgPool, job_id: Uuid, processed: i32, failed: i32) {
    let _ = sqlx::query("UPDATE upload_jobs SET processed = $1, failed = $2, updated_at = now() WHERE id = $3")
        .bind(processed)
        .bind(failed)
        .bind(job_id)
        .execute(db)
        .await;
}

async fn finish_job(db: &PgPool, job_id: Uuid, status: &str, error: Option<&str>, song_ids: &[Uuid], processed: i32, failed: i32) {
    let ids = serde_json::to_string(song_ids).unwrap_or_else(|_| "[]".to_string());
    let _ = sqlx::query(
        "UPDATE upload_jobs SET status = $1, error = $2, song_ids = $3::jsonb, processed = $4, failed = $5, current_file = NULL, updated_at = now() WHERE id = $6",
    )
    .bind(status)
    .bind(error)
    .bind(ids)
    .bind(processed)
    .bind(failed)
    .bind(job_id)
    .execute(db)
    .await;
}

async fn run_job(
    db: PgPool,
    config: Config,
    job_id: Uuid,
    user_id: Uuid,
    entries: Vec<(String, Vec<u8>)>,
    station_ids: Vec<Uuid>,
    single_meta: Option<(Option<String>, Option<String>, Option<String>)>,
) {
    let mut processed = 0i32;
    let mut failed = 0i32;
    let mut song_ids: Vec<Uuid> = Vec::new();
    let mut last_error: Option<String> = None;

    let (title_override, artist_override, album_override) = single_meta.unwrap_or((None, None, None));

    for (original_name, bytes) in entries {
        set_current_file(&db, job_id, &original_name).await;

        match process_song_upload(
            &db,
            &original_name,
            &bytes,
            &config.upload_dir,
            user_id,
            config.lastfm_api_key.as_deref(),
            title_override.as_deref(),
            artist_override.as_deref(),
            album_override.as_deref(),
        )
        .await
        {
            Ok(song) => {
                if let Err(e) = assign_song_to_stations(&db, song.id, &station_ids).await {
                    tracing::warn!(song = %song.id, "failed to assign song after upload: {e}");
                }
                song_ids.push(song.id);
                processed += 1;
            }
            Err(e) => {
                tracing::warn!(file = %original_name, "Song upload/analysis failed: {e}");
                failed += 1;
                if last_error.is_none() {
                    last_error = Some(e.to_string());
                }
            }
        }

        update_progress(&db, job_id, processed, failed).await;
    }

    finish_job(&db, job_id, "done", last_error.as_deref(), &song_ids, processed, failed).await;
}

#[cfg(test)]
mod tests {
    use super::expand_files;
    use super::single_override;
    use std::io::Write;

    fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let cursor = std::io::Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default();
        for (name, data) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(data).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn expand_files_keeps_multiple_single_tracks() {
        let files = vec![
            ("a.mp3".to_string(), b"a".to_vec()),
            ("b.wav".to_string(), b"b".to_vec()),
            ("c.flac".to_string(), b"c".to_vec()),
            ("d.opus".to_string(), b"d".to_vec()),
        ];
        let entries = expand_files(files).unwrap();
        let names: Vec<&str> = entries.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["a.mp3", "b.wav", "c.flac", "d.opus"]);
    }

    #[test]
    fn expand_files_skips_non_audio() {
        let files = vec![
            ("a.mp3".to_string(), b"a".to_vec()),
            ("notes.txt".to_string(), b"n".to_vec()),
            ("cover.jpg".to_string(), b"j".to_vec()),
            ("noext".to_string(), b"x".to_vec()),
        ];
        let entries = expand_files(files).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "a.mp3");
    }

    #[test]
    fn expand_files_mixes_zip_with_single_tracks() {
        let zip_bytes = make_zip(&[("inside/one.mp3", b"1"), ("two.flac", b"2"), ("doc.txt", b"d")]);
        let files = vec![("batch.zip".to_string(), zip_bytes), ("standalone.wav".to_string(), b"w".to_vec())];
        let entries = expand_files(files).unwrap();
        let names: Vec<&str> = entries.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["inside/one.mp3", "two.flac", "standalone.wav"]);
    }

    #[test]
    fn expand_files_empty_zip_yields_empty() {
        let zip_bytes = make_zip(&[("doc.txt", b"d")]);
        let files = vec![("empty.zip".to_string(), zip_bytes)];
        assert!(expand_files(files).unwrap().is_empty());
    }

    #[test]
    fn expand_files_rejects_broken_zip() {
        let files = vec![("broken.zip".to_string(), b"PK\x03\x04garbage".to_vec())];
        assert!(expand_files(files).is_err());
    }

    #[test]
    fn single_override_only_for_one_entry() {
        assert!(single_override(1, &Some("t".into()), &None, &None));
        assert!(single_override(1, &None, &Some("a".into()), &None));
        assert!(single_override(1, &None, &None, &Some("l".into())));
        assert!(!single_override(2, &Some("t".into()), &None, &None));
        assert!(!single_override(1, &None, &None, &None));
        assert!(!single_override(0, &Some("t".into()), &None, &None));
    }
}

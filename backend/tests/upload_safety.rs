use surcast_backend::songs::import::{sanitize_relative_path, store_upload_bytes};

#[test]
fn long_unicode_name_is_truncated_at_a_character_boundary() {
    let name = format!("{}é.mp3", "a".repeat(199));
    assert_eq!(sanitize_relative_path(&name).unwrap(), "a".repeat(199));
}

#[tokio::test]
async fn concurrent_uploads_never_overwrite_each_other() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let a = vec![1; 100_000];
    let b = vec![2; 100_000];
    let (first, second) = tokio::join!(store_upload_bytes(path, "same.mp3", &a), store_upload_bytes(path, "same.mp3", &b));
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_ne!(first, second);
    assert_eq!(std::fs::read(root.path().join(first)).unwrap(), a);
    assert_eq!(std::fs::read(root.path().join(second)).unwrap(), b);
}

#[cfg(unix)]
#[tokio::test]
async fn uploads_cannot_follow_parent_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("link")).unwrap();
    assert!(store_upload_bytes(root.path().to_str().unwrap(), "link/song.mp3", b"audio")
        .await
        .is_err());
    assert!(!outside.path().join("song.mp3").exists());
}

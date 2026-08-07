use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;
use sqlx::postgres::{PgTypeInfo, PgValueRef};
use sqlx::{Decode, Encode, Postgres, Type};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceType {
    #[serde(rename = "playlist")]
    Playlist,
    #[serde(rename = "station_library")]
    StationLibrary,
    #[serde(rename = "global_library")]
    GlobalLibrary,
    #[serde(rename = "weighted_playlists")]
    WeightedPlaylists,
}

impl Type<Postgres> for SourceType {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("varchar")
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        matches!(
            ty.oid(),
            Some(Oid(25)) | Some(Oid(1043)) | Some(Oid(1042)) | Some(Oid(19)) | Some(Oid(705))
        )
    }
}

impl Encode<'_, Postgres> for SourceType {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        let s = self.to_string();
        <String as Encode<Postgres>>::encode(s, buf)
    }
}

impl<'r> Decode<'r, Postgres> for SourceType {
    fn decode(value: PgValueRef<'r>) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let s = <&str as Decode<'r, Postgres>>::decode(value)?;
        s.parse::<SourceType>().map_err(|_| {
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid source type"))
                as Box<dyn std::error::Error + Send + Sync + 'static>
        })
    }
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::Playlist => write!(f, "playlist"),
            SourceType::StationLibrary => write!(f, "station_library"),
            SourceType::GlobalLibrary => write!(f, "global_library"),
            SourceType::WeightedPlaylists => write!(f, "weighted_playlists"),
        }
    }
}

impl std::str::FromStr for SourceType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "playlist" => Ok(SourceType::Playlist),
            "station_library" => Ok(SourceType::StationLibrary),
            "global_library" => Ok(SourceType::GlobalLibrary),
            "weighted_playlists" => Ok(SourceType::WeightedPlaylists),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecurrenceType {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "daily")]
    Daily,
    #[serde(rename = "every_n_days")]
    EveryNDays,
    #[serde(rename = "weekly")]
    Weekly,
    #[serde(rename = "biweekly")]
    Biweekly,
    #[serde(rename = "monthly")]
    Monthly,
    #[serde(rename = "custom_days")]
    CustomDays,
}

impl Type<Postgres> for RecurrenceType {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("varchar")
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        matches!(
            ty.oid(),
            Some(Oid(25)) | Some(Oid(1043)) | Some(Oid(1042)) | Some(Oid(19)) | Some(Oid(705))
        )
    }
}

impl Encode<'_, Postgres> for RecurrenceType {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        let s = self.to_string();
        <String as Encode<Postgres>>::encode(s, buf)
    }
}

impl<'r> Decode<'r, Postgres> for RecurrenceType {
    fn decode(value: PgValueRef<'r>) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let s = <&str as Decode<'r, Postgres>>::decode(value)?;
        s.parse::<RecurrenceType>().map_err(|_| {
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid recurrence type"))
                as Box<dyn std::error::Error + Send + Sync + 'static>
        })
    }
}

impl std::fmt::Display for RecurrenceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecurrenceType::None => write!(f, "none"),
            RecurrenceType::Daily => write!(f, "daily"),
            RecurrenceType::EveryNDays => write!(f, "every_n_days"),
            RecurrenceType::Weekly => write!(f, "weekly"),
            RecurrenceType::Biweekly => write!(f, "biweekly"),
            RecurrenceType::Monthly => write!(f, "monthly"),
            RecurrenceType::CustomDays => write!(f, "custom_days"),
        }
    }
}

impl std::str::FromStr for RecurrenceType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(RecurrenceType::None),
            "daily" => Ok(RecurrenceType::Daily),
            "every_n_days" => Ok(RecurrenceType::EveryNDays),
            "weekly" => Ok(RecurrenceType::Weekly),
            "biweekly" => Ok(RecurrenceType::Biweekly),
            "monthly" => Ok(RecurrenceType::Monthly),
            "custom_days" => Ok(RecurrenceType::CustomDays),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutoDjMode {
    #[default]
    #[serde(rename = "random")]
    Random,
    #[serde(rename = "sequential")]
    Sequential,
    #[serde(rename = "reverse")]
    Reverse,
}

impl Type<Postgres> for AutoDjMode {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("varchar")
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        matches!(
            ty.oid(),
            Some(Oid(25)) | Some(Oid(1043)) | Some(Oid(1042)) | Some(Oid(19)) | Some(Oid(705))
        )
    }
}

impl Encode<'_, Postgres> for AutoDjMode {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        let s = self.to_string();
        <String as Encode<Postgres>>::encode(s, buf)
    }
}

impl<'r> Decode<'r, Postgres> for AutoDjMode {
    fn decode(value: PgValueRef<'r>) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let s = <&str as Decode<'r, Postgres>>::decode(value)?;
        s.parse::<AutoDjMode>().map_err(|_| {
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid auto-dj mode"))
                as Box<dyn std::error::Error + Send + Sync + 'static>
        })
    }
}

impl std::fmt::Display for AutoDjMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AutoDjMode::Random => write!(f, "random"),
            AutoDjMode::Sequential => write!(f, "sequential"),
            AutoDjMode::Reverse => write!(f, "reverse"),
        }
    }
}

impl std::str::FromStr for AutoDjMode {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "random" => Ok(AutoDjMode::Random),
            "sequential" => Ok(AutoDjMode::Sequential),
            "reverse" => Ok(AutoDjMode::Reverse),
            _ => Err(()),
        }
    }
}

#[derive(Debug, sqlx::FromRow, Serialize)]
pub struct StationSchedule {
    pub id: Uuid,
    pub station_id: Uuid,
    pub day_of_week: i16,
    pub start_time: NaiveTime,
    pub end_time: NaiveTime,
    pub source_type: SourceType,
    pub playlist_id: Option<Uuid>,
    pub auto_dj_mode: Option<AutoDjMode>,
    pub auto_dj_avoid_repeat: Option<bool>,
    pub auto_dj_min_gap: Option<i32>,
    pub auto_dj_songs_ahead: Option<i32>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateScheduleRequest {
    pub day_of_week: i16,
    pub start_time: String,
    pub end_time: String,
    pub source_type: Option<SourceType>,
    pub playlist_id: Option<Uuid>,
    pub auto_dj_mode: Option<AutoDjMode>,
    pub auto_dj_avoid_repeat: Option<bool>,
    pub auto_dj_min_gap: Option<i32>,
    pub auto_dj_songs_ahead: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateScheduleRequest {
    pub day_of_week: Option<i16>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub source_type: Option<SourceType>,
    pub playlist_id: Option<Option<Uuid>>,
    pub auto_dj_mode: Option<Option<AutoDjMode>>,
    pub auto_dj_avoid_repeat: Option<Option<bool>>,
    pub auto_dj_min_gap: Option<Option<i32>>,
    pub auto_dj_songs_ahead: Option<Option<i32>>,
}

#[derive(Debug, Serialize)]
pub struct ScheduleResponse {
    pub id: Uuid,
    pub station_id: Uuid,
    pub day_of_week: i16,
    pub start_time: String,
    pub end_time: String,
    pub source_type: SourceType,
    pub playlist_id: Option<Uuid>,
    pub playlist_name: Option<String>,
    pub auto_dj_mode: Option<AutoDjMode>,
    pub auto_dj_avoid_repeat: Option<bool>,
    pub auto_dj_min_gap: Option<i32>,
    pub auto_dj_songs_ahead: Option<i32>,
}

#[derive(Debug, sqlx::FromRow, Serialize)]
pub struct StationScheduleEvent {
    pub id: Uuid,
    pub station_id: Uuid,
    pub title: Option<String>,
    pub start_date: NaiveDate,
    pub start_time: NaiveTime,
    pub end_time: NaiveTime,
    pub source_type: SourceType,
    pub playlist_id: Option<Uuid>,
    pub auto_dj_mode: Option<AutoDjMode>,
    pub auto_dj_avoid_repeat: Option<bool>,
    pub auto_dj_min_gap: Option<i32>,
    pub auto_dj_songs_ahead: Option<i32>,
    pub recurrence_type: RecurrenceType,
    pub recurrence_interval: Option<i32>,
    pub recurrence_days: Option<String>,
    pub recurrence_end_date: Option<NaiveDate>,
    pub recurrence_count: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateScheduleEventRequest {
    pub title: Option<String>,
    pub start_date: String,
    pub start_time: String,
    pub end_time: String,
    pub source_type: Option<SourceType>,
    pub playlist_id: Option<Uuid>,
    pub auto_dj_mode: Option<AutoDjMode>,
    pub auto_dj_avoid_repeat: Option<bool>,
    pub auto_dj_min_gap: Option<i32>,
    pub auto_dj_songs_ahead: Option<i32>,
    pub recurrence_type: Option<RecurrenceType>,
    pub recurrence_interval: Option<i32>,
    pub recurrence_days: Option<Vec<i32>>,
    pub recurrence_end_date: Option<String>,
    pub recurrence_count: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateScheduleEventRequest {
    pub title: Option<String>,
    pub start_date: Option<String>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub source_type: Option<SourceType>,
    pub playlist_id: Option<Option<Uuid>>,
    pub auto_dj_mode: Option<Option<AutoDjMode>>,
    pub auto_dj_avoid_repeat: Option<Option<bool>>,
    pub auto_dj_min_gap: Option<Option<i32>>,
    pub auto_dj_songs_ahead: Option<Option<i32>>,
    pub recurrence_type: Option<RecurrenceType>,
    pub recurrence_interval: Option<i32>,
    pub recurrence_days: Option<Option<String>>,
    pub recurrence_end_date: Option<Option<String>>,
    pub recurrence_count: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct ScheduleEventResponse {
    pub id: Uuid,
    pub station_id: Uuid,
    pub title: Option<String>,
    pub start_date: String,
    pub start_time: String,
    pub end_time: String,
    pub source_type: SourceType,
    pub playlist_id: Option<Uuid>,
    pub playlist_name: Option<String>,
    pub auto_dj_mode: Option<AutoDjMode>,
    pub auto_dj_avoid_repeat: Option<bool>,
    pub auto_dj_min_gap: Option<i32>,
    pub auto_dj_songs_ahead: Option<i32>,
    pub recurrence_type: RecurrenceType,
    pub recurrence_interval: Option<i32>,
    pub recurrence_days: Option<Vec<i32>>,
    pub recurrence_end_date: Option<String>,
    pub recurrence_count: Option<i32>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow, Serialize)]
pub struct StationAutoFill {
    pub station_id: Uuid,
    pub enabled: bool,
    pub mode: AutoDjMode,
    pub source_type: SourceType,
    pub source_playlist_id: Option<Uuid>,
    pub avoid_artist_repeat: bool,
    pub min_song_gap: i32,
    pub songs_ahead: i32,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAutoFillRequest {
    pub enabled: Option<bool>,
    pub mode: Option<AutoDjMode>,
    pub source_type: Option<SourceType>,
    pub source_playlist_id: Option<Option<Uuid>>,
    pub avoid_artist_repeat: Option<bool>,
    pub min_song_gap: Option<i32>,
    pub songs_ahead: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct AutoFillResponse {
    pub station_id: Uuid,
    pub enabled: bool,
    pub mode: AutoDjMode,
    pub source_type: SourceType,
    pub source_playlist_id: Option<Uuid>,
    pub source_playlist_name: Option<String>,
    pub avoid_artist_repeat: bool,
    pub min_song_gap: i32,
    pub songs_ahead: i32,
    pub weighted_playlists: Vec<AutoFillPlaylistResponse>,
}

#[derive(Debug, sqlx::FromRow, Serialize)]
pub struct StationAutoFillPlaylist {
    pub id: Uuid,
    pub station_id: Uuid,
    pub playlist_id: Uuid,
    pub weight: i32,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAutoFillPlaylistRequest {
    pub weight: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct AddAutoFillPlaylistRequest {
    pub playlist_id: Uuid,
    pub weight: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct AutoFillPlaylistResponse {
    pub id: Uuid,
    pub playlist_id: Uuid,
    pub playlist_name: String,
    pub weight: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_source_type_from_str_playlist() {
        assert_eq!(SourceType::from_str("playlist"), Ok(SourceType::Playlist));
    }

    #[test]
    fn test_source_type_from_str_station_library() {
        assert_eq!(SourceType::from_str("station_library"), Ok(SourceType::StationLibrary));
    }

    #[test]
    fn test_source_type_from_str_global_library() {
        assert_eq!(SourceType::from_str("global_library"), Ok(SourceType::GlobalLibrary));
    }

    #[test]
    fn test_source_type_from_str_weighted_playlists() {
        assert_eq!(SourceType::from_str("weighted_playlists"), Ok(SourceType::WeightedPlaylists));
    }

    #[test]
    fn test_source_type_from_str_invalid() {
        assert_eq!(SourceType::from_str("invalid"), Err(()));
    }

    #[test]
    fn test_recurrence_type_from_str_none() {
        assert_eq!(RecurrenceType::from_str("none"), Ok(RecurrenceType::None));
    }

    #[test]
    fn test_recurrence_type_from_str_daily() {
        assert_eq!(RecurrenceType::from_str("daily"), Ok(RecurrenceType::Daily));
    }

    #[test]
    fn test_recurrence_type_from_str_every_n_days() {
        assert_eq!(RecurrenceType::from_str("every_n_days"), Ok(RecurrenceType::EveryNDays));
    }

    #[test]
    fn test_recurrence_type_from_str_weekly() {
        assert_eq!(RecurrenceType::from_str("weekly"), Ok(RecurrenceType::Weekly));
    }

    #[test]
    fn test_recurrence_type_from_str_biweekly() {
        assert_eq!(RecurrenceType::from_str("biweekly"), Ok(RecurrenceType::Biweekly));
    }

    #[test]
    fn test_recurrence_type_from_str_monthly() {
        assert_eq!(RecurrenceType::from_str("monthly"), Ok(RecurrenceType::Monthly));
    }

    #[test]
    fn test_recurrence_type_from_str_custom_days() {
        assert_eq!(RecurrenceType::from_str("custom_days"), Ok(RecurrenceType::CustomDays));
    }

    #[test]
    fn test_recurrence_type_from_str_invalid() {
        assert_eq!(RecurrenceType::from_str("invalid"), Err(()));
    }

    #[test]
    fn test_auto_dj_mode_from_str_random() {
        assert_eq!(AutoDjMode::from_str("random"), Ok(AutoDjMode::Random));
    }

    #[test]
    fn test_auto_dj_mode_from_str_sequential() {
        assert_eq!(AutoDjMode::from_str("sequential"), Ok(AutoDjMode::Sequential));
    }

    #[test]
    fn test_auto_dj_mode_from_str_reverse() {
        assert_eq!(AutoDjMode::from_str("reverse"), Ok(AutoDjMode::Reverse));
    }

    #[test]
    fn test_auto_dj_mode_from_str_invalid() {
        assert_eq!(AutoDjMode::from_str("invalid"), Err(()));
    }
}

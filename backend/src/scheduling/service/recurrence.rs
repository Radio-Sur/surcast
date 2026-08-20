use chrono::{Datelike, NaiveDate};

use crate::scheduling::models::RecurrenceType;

fn match_daily(event_date: NaiveDate, date: NaiveDate, count: Option<i32>) -> bool {
    if let Some(max) = count {
        (date - event_date).num_days() < max as i64
    } else {
        true
    }
}

fn match_weekly(event_date: NaiveDate, date: NaiveDate, count: Option<i32>) -> bool {
    let diff = (date - event_date).num_days();
    if diff >= 0 && diff % 7 == 0 {
        if let Some(max) = count {
            diff / 7 < max as i64
        } else {
            true
        }
    } else {
        false
    }
}

fn match_every_n_days(event_date: NaiveDate, date: NaiveDate, interval: i32, count: Option<i32>) -> bool {
    let diff = (date - event_date).num_days();
    if diff >= 0 && diff % interval as i64 == 0 {
        if let Some(max) = count {
            diff / (interval as i64) < max as i64
        } else {
            true
        }
    } else {
        false
    }
}

fn match_biweekly(event_date: NaiveDate, date: NaiveDate, count: Option<i32>) -> bool {
    let diff = (date - event_date).num_days();
    if diff >= 0 && diff % 14 == 0 {
        if let Some(max) = count {
            diff / 14 < max as i64
        } else {
            true
        }
    } else {
        false
    }
}

fn match_monthly(event_date: NaiveDate, date: NaiveDate, count: Option<i32>) -> bool {
    if date.day() == event_date.day() {
        if let Some(max) = count {
            let months = (date.year() - event_date.year()) * 12 + (date.month() as i32 - event_date.month() as i32);
            months < max
        } else {
            true
        }
    } else {
        false
    }
}

fn match_custom_days(date: NaiveDate, days: Option<&str>) -> bool {
    if let Some(days_str) = days {
        let target = date.weekday().num_days_from_monday() as i32;
        days_str.split(',').any(|d| d.trim().parse::<i32>().ok() == Some(target))
    } else {
        false
    }
}

pub fn matches_recurrence(
    date: NaiveDate,
    start_date: NaiveDate,
    recurrence_type: &RecurrenceType,
    recurrence_interval: Option<i32>,
    recurrence_days: Option<&str>,
    recurrence_end_date: Option<NaiveDate>,
    recurrence_count: Option<i32>,
) -> bool {
    if date < start_date {
        return false;
    }
    if let Some(end) = recurrence_end_date {
        if date > end {
            return false;
        }
    }

    match recurrence_type {
        RecurrenceType::None => date == start_date,
        RecurrenceType::Daily => match_daily(start_date, date, recurrence_count),
        RecurrenceType::EveryNDays => match_every_n_days(start_date, date, recurrence_interval.unwrap_or(1).max(1), recurrence_count),
        RecurrenceType::Weekly => match_weekly(start_date, date, recurrence_count),
        RecurrenceType::Biweekly => match_biweekly(start_date, date, recurrence_count),
        RecurrenceType::Monthly => match_monthly(start_date, date, recurrence_count),
        RecurrenceType::CustomDays => match_custom_days(date, recurrence_days),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_daily_same_date() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert!(match_daily(d, d, None));
    }

    #[test]
    fn test_match_daily_next_day() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        assert!(match_daily(event, date, None));
    }

    #[test]
    fn test_match_daily_next_year_count_limited_not_exceeded() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        assert!(match_daily(event, date, Some(367)));
    }

    #[test]
    fn test_match_daily_next_year_count_exceeded() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        assert!(!match_daily(event, date, Some(365)));
    }

    #[test]
    fn test_match_weekly_plus_7_days() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        assert!(match_weekly(event, date, None));
    }

    #[test]
    fn test_match_weekly_plus_8_days() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 9).unwrap();
        assert!(!match_weekly(event, date, None));
    }

    #[test]
    fn test_match_weekly_count_limited() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        assert!(match_weekly(event, date, Some(4)));
        assert!(!match_weekly(event, date, Some(3)));
    }

    #[test]
    fn test_match_every_n_days_interval_3_correct() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        assert!(match_every_n_days(event, date, 3, None));
    }

    #[test]
    fn test_match_every_n_days_interval_3_incorrect() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        assert!(!match_every_n_days(event, date, 3, None));
    }

    #[test]
    fn test_match_every_n_days_interval_1() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        assert!(match_every_n_days(event, date, 1, None));
    }

    #[test]
    #[should_panic]
    fn test_match_every_n_days_interval_0() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        match_every_n_days(event, date, 0, None);
    }

    #[test]
    fn test_match_biweekly_plus_14_days() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert!(match_biweekly(event, date, None));
    }

    #[test]
    fn test_match_biweekly_plus_21_days() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 22).unwrap();
        assert!(!match_biweekly(event, date, None));
    }

    #[test]
    fn test_match_biweekly_with_count() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 29).unwrap();
        assert!(match_biweekly(event, date, Some(3)));
        assert!(!match_biweekly(event, date, Some(2)));
    }

    #[test]
    fn test_match_monthly_same_day() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 2, 15).unwrap();
        assert!(match_monthly(event, date, None));
    }

    #[test]
    fn test_match_monthly_different_day() {
        let event = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 2, 14).unwrap();
        assert!(!match_monthly(event, date, None));
    }

    #[test]
    fn test_match_monthly_dec_jan_transition() {
        let event = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let date = NaiveDate::from_ymd_opt(2025, 1, 31).unwrap();
        assert!(match_monthly(event, date, None));
    }

    #[test]
    fn test_match_monthly_feb_to_mar() {
        let event = NaiveDate::from_ymd_opt(2024, 2, 28).unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 3, 28).unwrap();
        assert!(match_monthly(event, date, None));
    }

    #[test]
    fn test_match_custom_days_string() {
        let mon = NaiveDate::from_ymd_opt(2025, 6, 2).unwrap();
        let wed = NaiveDate::from_ymd_opt(2025, 6, 4).unwrap();
        let fri = NaiveDate::from_ymd_opt(2025, 6, 6).unwrap();
        let tue = NaiveDate::from_ymd_opt(2025, 6, 3).unwrap();
        assert!(match_custom_days(mon, Some("0,2,4")));
        assert!(match_custom_days(wed, Some("0,2,4")));
        assert!(match_custom_days(fri, Some("0,2,4")));
        assert!(!match_custom_days(tue, Some("0,2,4")));
    }

    #[test]
    fn test_match_custom_days_empty_string() {
        let d = NaiveDate::from_ymd_opt(2025, 6, 2).unwrap();
        assert!(!match_custom_days(d, Some("")));
    }

    #[test]
    fn test_match_custom_days_none() {
        let d = NaiveDate::from_ymd_opt(2025, 6, 2).unwrap();
        assert!(!match_custom_days(d, None));
    }

    #[test]
    fn test_matches_recurrence_none() {
        let d = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert!(matches_recurrence(d, d, &RecurrenceType::None, None, None, None, None));
        let next = NaiveDate::from_ymd_opt(2024, 6, 16).unwrap();
        assert!(!matches_recurrence(next, d, &RecurrenceType::None, None, None, None, None));
    }

    #[test]
    fn test_matches_recurrence_daily() {
        let start = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 6, 5).unwrap();
        assert!(matches_recurrence(d, start, &RecurrenceType::Daily, None, None, None, None));
    }

    #[test]
    fn test_matches_recurrence_before_start() {
        let start = NaiveDate::from_ymd_opt(2024, 6, 10).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 6, 5).unwrap();
        assert!(!matches_recurrence(d, start, &RecurrenceType::Daily, None, None, None, None));
    }

    #[test]
    fn test_matches_recurrence_after_end_date() {
        let start = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 6, 10).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert!(!matches_recurrence(d, start, &RecurrenceType::Daily, None, None, Some(end), None));
    }

    #[test]
    fn test_matches_recurrence_weekly() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        assert!(matches_recurrence(d, start, &RecurrenceType::Weekly, None, None, None, None));
    }

    #[test]
    fn test_matches_recurrence_biweekly() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert!(matches_recurrence(d, start, &RecurrenceType::Biweekly, None, None, None, None));
    }

    #[test]
    fn test_matches_recurrence_monthly() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        assert!(matches_recurrence(d, start, &RecurrenceType::Monthly, None, None, None, None));
    }

    #[test]
    fn test_matches_recurrence_every_n_days() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        assert!(matches_recurrence(d, start, &RecurrenceType::EveryNDays, Some(3), None, None, None));
        let wrong = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        assert!(!matches_recurrence(
            wrong,
            start,
            &RecurrenceType::EveryNDays,
            Some(3),
            None,
            None,
            None
        ));
    }

    #[test]
    fn test_matches_recurrence_custom_days() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let mon = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert!(matches_recurrence(
            mon,
            start,
            &RecurrenceType::CustomDays,
            None,
            Some("0"),
            None,
            None
        ));
    }
}

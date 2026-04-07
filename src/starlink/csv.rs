use std::collections::HashMap;
use std::io;
use std::path::Path;

pub fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && matches!(chars.peek(), Some('"')) {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => fields.push(std::mem::take(&mut field)),
            _ => field.push(ch),
        }
    }

    fields.push(field);
    fields
}

pub fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

pub fn build_header_map(fields: &[String]) -> HashMap<String, usize> {
    let mut header_map = HashMap::new();
    for (index, field) in fields.iter().enumerate() {
        header_map.insert(field.clone(), index);
    }
    header_map
}

pub fn require_column(header_map: &HashMap<String, usize>, name: &str) -> io::Result<usize> {
    header_map.get(name).copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("required CSV column '{name}' was not found"),
        )
    })
}

pub fn get_field(fields: &[String], index: usize, name: &str) -> io::Result<String> {
    fields.get(index).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CSV row did not include '{name}'"),
        )
    })
}

pub fn path_as_str(path: &Path) -> io::Result<&str> {
    path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains non-UTF-8 characters: {}", path.display()),
        )
    })
}

pub fn validate_date(value: &str, flag: &str) -> io::Result<()> {
    let bytes = value.as_bytes();
    let valid = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit());
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} must be YYYY-MM-DD, got '{}'", flag, value),
        ))
    }
}

pub fn next_date(date: &str) -> io::Result<String> {
    validate_date(date, "date")?;
    let year = date[0..4].parse::<i32>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid year in '{}': {}", date, error),
        )
    })?;
    let month = date[5..7].parse::<u32>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid month in '{}': {}", date, error),
        )
    })?;
    let day = date[8..10].parse::<u32>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid day in '{}': {}", date, error),
        )
    })?;

    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid month in '{}'", date),
            ))
        }
    };
    if day == 0 || day > days_in_month {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid day in '{}'", date),
        ));
    }

    let (next_year, next_month, next_day) = if day < days_in_month {
        (year, month, day + 1)
    } else if month < 12 {
        (year, month + 1, 1)
    } else {
        (year + 1, 1, 1)
    };

    Ok(format!(
        "{:04}-{:02}-{:02}",
        next_year, next_month, next_day
    ))
}

pub fn looks_like_html(body: &str) -> bool {
    let trimmed = body.trim_start();
    trimmed.starts_with("<!DOCTYPE html")
        || trimmed.starts_with("<html")
        || trimmed.starts_with("<HTML")
}

pub fn date_part(timestamp: &str) -> Option<&str> {
    if timestamp.len() >= 10 {
        Some(&timestamp[..10])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_csv_quotes() {
        let fields = parse_csv_line("\"44713\",\"STARLINK-1007\"");
        assert_eq!(fields, vec!["44713", "STARLINK-1007"]);
    }

    #[test]
    fn computes_next_date() {
        assert_eq!(next_date("2024-08-02").unwrap(), "2024-08-03");
        assert_eq!(next_date("2024-12-31").unwrap(), "2025-01-01");
        assert_eq!(next_date("2024-02-28").unwrap(), "2024-02-29");
    }
}

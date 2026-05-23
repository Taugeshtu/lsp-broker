use std::path::{Path, PathBuf};
use std::io::{BufRead, BufReader};
use std::fs::File;

pub fn find_project_root(file_path: &str) -> PathBuf {
    let path = Path::new(file_path);
    let mut current = if path.is_file() {
        path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf()
    } else {
        path.to_path_buf()
    };
    
    let home = std::env::var("HOME").ok().map(PathBuf::from);

    loop {
        let git_marker = current.join(".git");
        let project_root_marker = current.join(".project-root");
        
        if git_marker.exists() || project_root_marker.exists() {
            return current;
        }

        if Some(current.clone()) == home {
            break;
        }

        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => break,
        }
    }

    // Fallback: directory containing the file
    let path = Path::new(file_path);
    if path.is_file() {
        path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf()
    } else {
        path.to_path_buf()
    }
}

pub fn detect_shebang_language(file_path: &str) -> Option<String> {
    let file = File::open(file_path).ok()?;
    let mut reader = BufReader::new(file);
    let mut first_line = String::new();
    reader.read_line(&mut first_line).ok()?;
    
    if first_line.starts_with("#!") {
        let line = first_line.trim_start_matches("#!").trim();
        if line.contains("python") {
            return Some("python".to_string());
        } else if line.contains("node") {
            return Some("javascript".to_string());
        } else if line.contains("ruby") {
            return Some("ruby".to_string());
        } else if line.contains("bash") || line.contains("sh") {
            return Some("shell".to_string());
        } else if line.contains("perl") {
            return Some("perl".to_string());
        }
    }
    None
}

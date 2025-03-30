use std::collections::HashSet;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use regex::Regex;
use rusqlite::{params, Connection};
use rusqlite::types::Value;
use std::os::unix::fs::symlink;
use std::{fs, io, thread};
use std::sync::{mpsc, Arc, LazyLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use notify::event::{ModifyKind, RenameMode};

fn safe_slice(input: &str, index: usize) -> Option<&str> {
    if index <= input.len() && input.is_char_boundary(index) {
        Some(&input[..index])
    } else {
        None
    }
}
#[derive(Debug)]
struct TitleResult {
    id: usize,
    path: String,
    title: String,
    file_name: String,
    file_size: usize,
    group_key: String,
    is_movie: bool,
    season_number: Option<usize>,
    episode_number: Option<usize>,
    naming_result: usize,
    files_in_group: usize,
    size_deviation: f32
}

const SEASON_REGEX: LazyLock<Regex>= LazyLock::new(|| { Regex::new(r"(?i)S(\d{2})[EX](\d{2})").unwrap() });
const YEAR_REGEX: LazyLock<Regex> = LazyLock::new(|| { Regex::new(r"(?i)(\d{4})").unwrap() });
const IGNORE_FILE_NAME_REGEX: LazyLock<Regex> = LazyLock::new(|| { Regex::new(r"(?i)sample").unwrap() });
const EXTENSIONS: &[&str] = &["mp4", "mkv"];

fn now_as_millis() -> u64 {
    let now = SystemTime::now();
    now.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64
}

fn create_symlink_with_dirs(target: &Path, link_path: &Path) -> io::Result<()> {
    // Ensure the parent directory of the symlink exists
    if let Some(parent) = link_path.parent() {
        fs::create_dir_all(parent)?; // Recursively create parent directories
    }

    println!("target: {}", target.display());
    println!("link_path: {}", link_path.display());
    // Create the symbolic link
    symlink(target, link_path)?;
    Ok(())
}
fn organize_them(conn: &Connection, base_dir: &Path, source_path: &Path, dest_movie_dir: &Path, dest_tv_dir: &Path) -> anyhow::Result<()> {
    println!("organizing them!");
    let mut stmt = conn.prepare("
    select id, path, title, file_size, group_key, is_movie, season_number,
            episode_number, naming_result,
           count(*) over(partition by group_key) files_in_group,
           1.0 * file_size / (sum(file_size) over(partition by group_key) /  count(*) over(partition by group_key)) as size_deviation,
           file_name
    from titles
    where dest_path IS NULL
    order by group_key, 1.0 * file_size / (sum(file_size) over(partition by group_key) /  count(*) over(partition by group_key)) desc
    ")?;

    let rows = stmt.query_map([], |row| {
        Ok(TitleResult {
            id: row.get(0)?,
            path: row.get(1)?,
            title: row.get(2)?,
            file_size: row.get(3)?,
            group_key: row.get(4)?,
            is_movie: row.get(5)?,
            season_number: row.get(6)?,
            episode_number: row.get(7)?,
            naming_result: row.get(8)?,
            files_in_group: row.get(9)?,
            size_deviation: row.get(10)?,
            file_name: row.get(11)?
        })
    })?;
    let mut current_movie_title: Option<String> = None;
    let mut current_movie_group_key: Option<String> = None;


    for r in rows {
        if let Ok(tr) = r {
            let symlink_path = PathBuf::new();
            let mut dest_path = PathBuf::new();

            if tr.is_movie {
                if let Some(cmgk) = current_movie_group_key.as_ref() {
                   if cmgk.as_str() != &tr.group_key {
                       // found a new movie
                       current_movie_title = Some(tr.title.clone());
                       current_movie_group_key = Some(tr.group_key);
                       // println!("set running title: {}", current_movie_title.as_ref().unwrap())
                   }
                } else {
                    current_movie_title = Some(tr.title.clone());
                    current_movie_group_key = Some(tr.group_key);
                    // println!("set running title: {}", current_movie_title.as_ref().unwrap())
                }


                dest_path.push(dest_movie_dir);

                if tr.size_deviation > 0.90f32 {
                    current_movie_title = Some(tr.title.clone());
                }

                dest_path.push(current_movie_title.as_ref().unwrap());

                if tr.size_deviation < 0.7f32 {
                    dest_path.push("extras");
                }
                dest_path.push(format!("{} - {}",current_movie_title.as_ref().unwrap(), tr.file_name));
                // println!("{}", tr.path);
                // println!("  {}", dest_path.display());
            } else {
                dest_path.push(dest_tv_dir);
                dest_path.push(&tr.title);
                if let Some(season) = tr.season_number {
                    dest_path.push(format!("season {}", season));
                }
                dest_path.push(tr.file_name);
            }
            if let Some(out_path) = dest_path.to_str() {
                let up_dirs = dest_path.as_path().components().count();
                let mut link_source_path = PathBuf::new();
                for _ in 0..up_dirs-1 {
                    link_source_path.push("..");
                }
                link_source_path.push(source_path);
                link_source_path.push(tr.path);
                let lsp = link_source_path.to_str().unwrap();

                let mut link_result = 0;
                let mut final_out_path = PathBuf::new();
                final_out_path.push(base_dir);
                final_out_path.push(out_path);
                println!("{} -> {}", &final_out_path.display(), &link_source_path.to_str().unwrap());

                match create_symlink_with_dirs(&link_source_path, &final_out_path) {
                    Ok(_) => {}
                    Err(err) => {
                        link_result = 1;
                        println!("ERROR: failed to create symlink: '{}'. #{} :  {}, {}", err, tr.id, link_source_path.to_str().unwrap(), dest_path.to_str().unwrap());
                    }
                }

                conn.execute("UPDATE titles SET dest_path = ?1, symlink_path=?2, link_result=?3 WHERE id = ?4", params![out_path, lsp, link_result, &tr.id])?;
            }
        }
    }


    Ok(())

}

fn process_discovered_file(conn: &Connection, entry_path: &Path, root_path_levels: usize) -> anyhow::Result<()> {
    let mut naming_result: usize = 0;

    if let Some(ext) = entry_path.extension() {
        let target_ext = ext.to_string_lossy();
        if !EXTENSIONS.contains(&target_ext.as_ref()) {
            return Ok(())
        }
    } else {
        return Ok(())
    }

    if entry_path.is_file() {
        let maybe_file_name = entry_path.file_name().unwrap().to_str();
        let mut end_cut: Option<usize> = None;
        if let Some(file_name) = maybe_file_name {

            let metadata = match std::fs::metadata(entry_path) {
                Ok(md) => md,
                Err(e) => return Ok(())
            };
            let file_size = metadata.len() / (1024 * 1024);



            if let Some(_) = IGNORE_FILE_NAME_REGEX.captures(file_name) {
                return Ok(())
            }

            let mut is_movie = true;
            let (season, episode) = if let Some(captures) = SEASON_REGEX.captures(file_name) {
                let season = captures.get(1).unwrap().as_str();
                let episode = captures.get(2).unwrap().as_str();
                end_cut = Some(captures.get(1).unwrap().start());

                is_movie = false;
                // println!("  {season} {episode}");
                (Value::Integer(season.parse()?), Value::Integer(episode.parse()?))
            } else {
                (Value::Null, Value::Null)
            };

            // println!("{file_name}");
            // println!("  Movie: {is_movie}");
            if let Some(yr_capture) = YEAR_REGEX.captures(file_name) {
                let year = yr_capture.get(1).unwrap().as_str();

                // println!("  year: {year}");
                let yr_end_cut = yr_capture.get(1).unwrap().start();
                end_cut = match((end_cut, yr_end_cut)) {
                    (None, _) => Some(yr_end_cut),
                    (Some(ec), yec) if yec < ec => Some(yec),
                    _ => end_cut
                }
            }
            let good_name = if let Some(end_cut) = end_cut {
                if let Some(slice_res) = safe_slice(file_name, end_cut - 1) {
                    let cleaned = slice_res.trim_end_matches('.');
                    // println!("  Good name: {cleaned}");
                    cleaned
                } else {
                    naming_result = 1;
                    file_name
                }
            } else {
                // println!("  Couldn't calculate good name");
                let p = entry_path.to_str().unwrap();
                // println!("  Path is: {p}");
                naming_result = 2;
                file_name
            };

            let depth = entry_path.components().count() - root_path_levels;
            // println!("depth is {depth}");
            let mut relative_path = PathBuf::new();
            for (index, component) in entry_path.iter().skip(root_path_levels).enumerate() {
                relative_path.push(component);
                // println!("  lvl: {index}: {:?}", component);
            }
            // println!("  relative_path: {relative_path:?}");
            let group_key = if let Some(first) = entry_path.iter().skip(root_path_levels).next() {
                // println!("  first part is {:?}", first);
                first.to_str().unwrap()
            } else {
                entry_path.to_str().unwrap()
            };
            // println!("  size is {file_size}");

            let mut processed_good_name = good_name.replace(".", " ");
            processed_good_name = processed_good_name.to_lowercase();

            conn.execute(
                "INSERT INTO titles (path, file_name, title, file_size, group_key, is_movie, season_number, episode_number, naming_result) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT(path) DO NOTHING;",
                params![relative_path.to_str().unwrap(), file_name, processed_good_name.trim(), file_size, group_key, is_movie, season, episode, naming_result],
            )?;
            println!("Discovered {:?}", entry_path)
        }
    }
    Ok(())

}

fn main() -> anyhow::Result<()> {

    // let base_dir: &Path = Path::new("/Volumes/md0/transmission_data");
    let base_dir: &Path = Path::new("/mnt/md0/transmission_data/");
    let source_dir = Path::new("completed");
    let dest_movie_dir =  Path::new("sorted_movies_2_testing");
    let dest_tv_dir = Path::new("sorted_tv_2_testing");

    let full_source_dir = PathBuf::from(base_dir).join(source_dir);

    let root_path_levels = full_source_dir.components().count();

    let db_path = PathBuf::from("./").join("vid_paths_testing.db");
    let conn = Connection::open(&db_path)?;
    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS titles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            path            TEXT NOT NULL,
            title           TEXT NOT NULL,
            file_name       TEXT NOT NULL,
            file_size       INTEGER NOT NULL,
            group_key       TEXT NOT NULL,
            is_movie        BOOL NOT NULL,
            season_number   INTEGER,
            episode_number  INTEGER,
            naming_result   INTEGER,
            symlink_path    TEXT,
            dest_path       TEXT,
            link_result     INTEGER
        );
        ",
        [], // No parameters for this query
    )?;
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_titles_path ON titles(path)",[])?;

    println!("vid_curator starting");

    // first we walk the whole thing and add it all into the db, itll be processed when scanned
    println!("scanning vid dir for changes...");
    for entry in WalkDir::new(&full_source_dir) {
        let entry = entry?;
        let entry_path = entry.path();
        process_discovered_file(&conn, entry_path, root_path_levels)?;
    }

    let p = Path::new(base_dir);
    let last_update_requested = Arc::new(AtomicU64::new(now_as_millis()));

    let (tx, rx) = mpsc::channel();
    let watch_conn = Connection::open(&db_path)?;
    let watcher_last_update_requested = last_update_requested.clone();
    let watcher_thread = thread::spawn(move || {
        let mut watcher = RecommendedWatcher::new(tx, Config::default())
            .expect("Failed to initialize watcher");

        watcher.watch(full_source_dir.as_path(), RecursiveMode::Recursive)
            .expect("Failed to watch directory");

        loop {
            match rx.recv() {
                Ok(Ok(notify::Event { kind: EventKind::Modify(ModifyKind::Name(RenameMode::To)), paths: ref p, .. } )) => {
                    for pb in p {
                        match process_discovered_file(&watch_conn, pb.as_path(), root_path_levels) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Failed to process discovered file {:?}", e);
                            }
                        }
                    }
                    println!("noticed this file! {:?}", p);
                    watcher_last_update_requested.store(now_as_millis(), Ordering::Relaxed);

                },
                Ok(e) => {
                    // println!("something else: {:?}", e);
                },
                Err(e) => println!("Watch error: {:?}", e),
            }
        }
    });

    let mut last_update_performed: u64 = 0;
    loop {
        if last_update_performed != last_update_requested.load(Ordering::Relaxed) {
            let debounce_time_elapsed = now_as_millis() - last_update_requested.load(Ordering::Relaxed);
            println!("Received request to reprocess un-curated files... debouncing after 5 secs: {}", debounce_time_elapsed);
            // we've requested an update, cool, but I want that request to be at least 5 seconds old
            if debounce_time_elapsed > 5000 {
                println!("Checking for un-curated recently added videos");
                let _ = organize_them(&conn, &p, &source_dir, dest_movie_dir, dest_tv_dir)?;
                last_update_performed = last_update_requested.load(Ordering::Relaxed);
            }
        }
        sleep(Duration::from_millis(1000));
    }

    // this is a scan - we should do it after initialization and debounce do it upon file discovery
    // let p = organize_them(&conn, &p, &source_dir, dest_movie_dir, dest_tv_dir)?;
    // watcher_thread.join().expect("Watcher thread panicked");
    Ok(())

}

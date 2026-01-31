use anyhow::Result;
use std::io::{self, Write};
use std::path::Path;

mod data;
mod load;
mod script;

#[tokio::main]
async fn main() -> Result<()> {
    //env_logger::Builder::from_default_env().init();
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Trace)
        .init();

    let mut project_loaded = false;

    print!("$ ");
    io::stdout().flush().unwrap();

    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            print!("$ ");
            io::stdout().flush().unwrap();
            continue;
        }

        let mut parts = input.split_whitespace();
        let cmd = parts.next().unwrap();

        match cmd {
            "load" => {
                let path = match parts.next() {
                    Some(p) => Path::new(p),
                    None => {
                        println!("Usage: load <path>");
                        continue;
                    }
                };

                if load_project(path).await.is_some() {
                    project_loaded = true;
                }
            }

            "run" => {
                if !project_loaded {
                    println!("No project loaded. Use `load <path>` first.");
                    continue;
                }

                let path = match parts.next() {
                    Some(p) => Path::new(p),
                    None => {
                        println!("Usage: run <script.json>");
                        continue;
                    }
                };

                match script::Script::load(path) {
                    Some(script) => {
                        if let Err(e) = script.run(data::get_storage()).await {
                            log::error!("{e}");
                        }
                    }
                    None => {}
                }
            }

            "exit" | "quit" => break,

            _ => {
                println!("Unknown command: {cmd}");
                println!("Commands: load <path>, run <script.json>, exit");
            }
        }

        print!("$ ");
        io::stdout().flush().unwrap();
    }

    Ok(())
}
async fn load_project(dir: &Path) -> Option<()> {
    let config = match load::load_config(&Path::new(dir).join("schema.json")) {
        Ok(v) => v,
        Err(e) => {
            log::error!("{e}");
            return None;
        }
    };
    let data = match load::load_data(&Path::new(dir).join("data.json"), &config) {
        Ok(v) => v,
        Err(e) => {
            log::error!("{e}");
            return None;
        }
    };
    match data::init(config, data).await {
        Ok(_) => {}
        Err(e) => {
            log::error!("{e}");
            return None;
        }
    }
    log::info!("Successfully loaded project");
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_not_crash() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Trace)
            .init();
        let test_path = Path::new("D:\\repo\\traverse\\tests\\test_dir");

        load_project(&test_path).await.unwrap();
        let script_path = &test_path.join("scripts").join("valve_io.json");
        let script = script::Script::load(script_path).unwrap();
        script.run(data::get_storage()).await.unwrap();
    }
}

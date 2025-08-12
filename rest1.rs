use scraper::{Html, Selector};
use reqwest::{header::USER_AGENT, Url};
use std::collections::{BinaryHeap};
use tokio::sync::{Semaphore, Mutex};
use std::sync::Arc;
use dashmap::DashMap;
use rusqlite::{params, Connection};

// ========== FILE STRUCTURE ==========
// [1] Global Structures
// [2] Score Ordering
// [3] Main Function (main)
// [4] Main Page Processing (process_url)
// [5] Robots.txt Parsing (check_robots_txt)
// [6] Robots.txt Extraction (extract_rules)
// [7] URL Normalization (normalize_url)
// [8] Scoring Logic (compute_score)
// [9] HTML Parsing (extract_pagedata)
// [10] Store Page Data (store_page_data)
// [11] Build Inverted Index (build_inverted_index)

// --------------------
// Global Structures
// --------------------
struct QueueItem {
    url: String,
    depth: u32,
    score: u32,
}

#[derive(Clone)]
struct RobotData {
    disallow: Vec<String>,
    allow: Vec<String>,
    crawl_delay: Option<u16>,
}

#[derive(Debug)]
struct PageData {
    url: Option<String>,
    title: Option<String>,
    meta_description: Option<String>,
    headings: Vec<String>,
    paragraph_snippets: Vec<String>,
    image_alt_texts: Vec<String>,
}

// --------------------
// Score Ordering
// --------------------
impl PartialEq for QueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url && self.score == other.score && self.depth == other.depth
    }
}
impl Eq for QueueItem {}

// Sorting logic for BinaryHeap
impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher score = higher priority
        other.score.cmp(&self.score)
            // If scores are equal, prefer lower depth
            .then_with(|| self.depth.cmp(&other.depth))
    }
}

// PartialOrd just delegates to Ord
impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// --------------------
// Main Function
// --------------------
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_link = "https://en.wikipedia.org/wiki/2025_Tour_de_France_Femmes";
    let client = reqwest::Client::new();
    let hash = Arc::new(DashMap::new());
    let robots_cache = Arc::new(DashMap::new());
    let link_appearance_counts = Arc::new(DashMap::new());
    let domain_counts = Arc::new(DashMap::new());
    let queue = Arc::new(Mutex::new(BinaryHeap::new()));
    let semaphore = Arc::new(Semaphore::new(50));
    let conn = Arc::new(Mutex::new(Connection::open("page_metadata.db")?));

    hash.insert(start_link.to_string(), None);
    queue.lock().await.push(QueueItem {
        url: start_link.to_string(),
        depth: 0,
        score: 0,
    });

    loop {
        let mut locked_queue = queue.lock().await;

        if locked_queue.is_empty() && semaphore.available_permits() == 50 {
            println!("✅ Crawling complete. Exiting.");
            return Ok(());
        }

        if let Some(current_item) = locked_queue.pop() {
            drop(locked_queue); // Release lock

            let permit = semaphore.clone().acquire_owned().await.unwrap();

            // Clone shared state
            let hash = Arc::clone(&hash);
            let robots_cache = Arc::clone(&robots_cache);
            let link_appearance_counts = Arc::clone(&link_appearance_counts);
            let domain_counts = Arc::clone(&domain_counts);
            let queue = Arc::clone(&queue);
            let client = client.clone();
            let conn = Arc::clone(&conn);
            
            // use spawn_blocking inside spawn
            tokio::spawn(async move {
                let result = tokio::task::spawn_blocking(move || {
                    tokio::runtime::Handle::current().block_on(async {
                        process_url(
                            current_item,
                            client,
                            hash,
                            robots_cache,
                            link_appearance_counts,
                            domain_counts,
                            queue,
                            conn,
                            permit,
                        ).await;
                    });
                }).await;

                if let Err(e) = result {
                    eprintln!("❌ Task panicked: {:?}", e);
                }
            });
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }
}


// --------------------
// Main Page Processing
// --------------------

async fn process_url(
    current_item: QueueItem,
    client: reqwest::Client,
    hash: Arc<DashMap<String, Option<PageData>>>,
    robots_cache: Arc<DashMap<String, RobotData>>,
    link_appearance_counts: Arc<DashMap<String, u32>>,
    domain_counts: Arc<DashMap<String, u32>>,
    queue: Arc<Mutex<BinaryHeap<QueueItem>>>,
    conn: Arc<Mutex<Connection>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let current_depth = current_item.depth;
    let current_url = current_item.url;
    const MAX_DEPTH: u32 = 10;
    if current_depth >= MAX_DEPTH {
        return;
    }
    println!("🌐 Crawling {}", current_url);
    // Main Loop Variables
    let base = match Url::parse(&current_url) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("🚨 Invalid URL {}: {}", current_url, e);
            return;
        }
    };
    
    let robots_url = match base.join("/robots.txt") {
        Ok(url) => url,
        Err(e) => {
            eprintln!("🚨 Failed to join robots.txt URL from {}: {}", base, e);
            return;
        }
    };
    
    let mut skip_page = false;
    let current_domain = base.domain().unwrap_or_default().to_string();

    // Fetch robots.txt
    let robotdata = if let Some(data) = robots_cache.get(&current_domain) {
        data.clone()
    } else {
        let resp = client
            .get(robots_url.clone())
            .header(USER_AGENT, "TestScraper")
            .send()
            .await;
    
        let new_robotdata = if let Ok(response) = resp {
            if response.status().is_success() {
                let robots_txt = response.text().await.unwrap_or_default();
                check_robots_txt(&robots_txt, base.clone())
            } else {
                RobotData { disallow: vec![], allow: vec![], crawl_delay: None }
            }
        } else {
            RobotData { disallow: vec![], allow: vec![], crawl_delay: None }
        };
    
        robots_cache.insert(current_domain.clone(), new_robotdata.clone());
        new_robotdata
    };        

    // Fetch main HTML
    let html_body = match client
        .get(&current_url)
        .header(USER_AGENT, "TestScraper")
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.text().await {
                    Ok(body) => body,
                    Err(e) => {
                        eprintln!("🚨 Failed to read body for {}: {}", current_url, e);
                        return;
                    }
                }
            } else if resp.status().as_u16() == 429 {
                eprintln!("⏳ Received 429 Too Many Requests for {}. Sleeping for 10s", current_url);
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                return;
            } else {
                eprintln!("🚨 Non-success status {} for {}", resp.status(), current_url);
                return;
            }            
        }
        Err(e) => {
            eprintln!("🚨 Failed to fetch {}: {}", current_url, e);
            return; // skip this page
        }
    };    

    // Parse the main HTML document
    let document = Html::parse_document(&html_body);
    let selector = Selector::parse("a").unwrap();

    for element in document.select(&Selector::parse("meta").unwrap()){
        if Some("robots") == element.attr("name") && Some("noindex") == element.attr("content"){
            println!("⏭️ Respecting noindex: skipping {}", &current_url);
            skip_page = true;
            break;
        }
    }

    // Skip current page if no index
    if skip_page {
        return;
    }

    // Storing all page data
    let page_data = extract_pagedata(&current_url, &html_body);

    let mut conn = conn.lock().await;
    if let Err(e) = store_page_data(&conn, &page_data, current_depth) {
        eprintln!("❌ Failed to store page data: {}", e);
    } 

    if let Err(e) = build_inverted_index(&mut conn, &page_data) {
        eprintln!("❌ Failed to build inverted index: {}", e);
    }        

    for element in document.select(&selector) {
        if let Some(link) = element.attr("href") {
            let joined_url = match base.join(link) {
                Ok(url) => url,
                Err(e) => {
                    eprintln!("🚨 Skipping bad link '{}': {}", link, e);
                    continue;
                }
            };
            
            let mut local_link = joined_url.as_str();
            if let Some(l) = local_link.split_once('#') {
                local_link = l.0;
            }
            
            // if element.attr("rel") == Some("nofollow"){
            //     println!("Nofollow:{}",local_link);
            // } else
            if true {
                // Skip non-web links
                let is_https = local_link.to_lowercase().starts_with("https://");
                let is_http = local_link.to_lowercase().starts_with("http://");
                if !is_http && !is_https {
                    continue;
                }

                // Skip already seen URLs
                if hash.contains_key(local_link) {
                    continue;
                }

                // Check robots.txt rules
                let is_allowed = robotdata.allow.iter().any(|rule| {
                    local_link.starts_with(rule)
                });

                let is_disallowed = robotdata.disallow.iter().any(|rule| {
                    local_link.starts_with(rule)
                });
                
                // If explicitly disallowed and not allowed → skip
                if is_disallowed && !is_allowed{
                    continue;
                }

                let joined = base.join(link).unwrap();
                let normalized_link = normalize_url(joined);

                if hash.contains_key(&normalized_link) {
                    continue; // already seen
                }

                let blocked_domains = ["web.archive.org", "accounts.google.com", "archive.today", "web.archive.org", "wayback"];
                if blocked_domains.iter().any(|&d| normalized_link.contains(d)) {
                    continue;
                }

                let skip_ext = [".rdf", ".nt", ".json", ".xml", ".pdf", ".gif", ".jpg", ".jpeg", ".png", ".svg"];
                if skip_ext.iter().any(|&d| normalized_link.contains(d)) {
                    continue;
                }

                let mut count = link_appearance_counts.entry(normalized_link.clone()).or_insert(0);
                *count += 1;                

                // Insert the URL into the visited hash map
                hash.insert(normalized_link.to_owned(), None);

                let temp_domain = Url::parse(&normalized_link).ok().and_then(|u| u.domain().map(|d| d.to_string())).unwrap_or_default();
                let domain_count = domain_counts.get(&temp_domain).map(|v| *v).unwrap_or(0);
                let projected_domain_count = domain_count + 1;

                // Compute score (don't use &mut with DashMap)
                let new_score = compute_score(&normalized_link, current_depth + 1, *count, projected_domain_count,);

                // 🔐 Lock the queue before pushing
                let mut locked_queue = queue.lock().await;
                locked_queue.push(QueueItem {url: normalized_link.to_owned(), depth: current_depth + 1, score: new_score,});
            }
        }
    }
    // After finishing crawling this page
    if let Some(delay) = robotdata.crawl_delay {
        println!("⏳ Respecting crawl-delay: {} seconds for {}", delay, current_domain);
        tokio::time::sleep(std::time::Duration::from_secs(delay.into())).await;
    }
}

// --------------------
// Robots.txt Parsing
// --------------------
fn check_robots_txt(robots_txt: &str, base:  Url ) -> RobotData {
    let mut lines: std::str::Lines<'_> = robots_txt.lines();
    let mut robo :RobotData = RobotData { disallow: Vec::new(), allow: Vec::new(), crawl_delay: None};

    loop{
        let line = lines.next();
        if line.is_none(){
            break;
        }
        let line: &str = line.unwrap().trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line.to_lowercase().starts_with("user-agent: ") {
            // Try both lowercase and original
            if let Some(agent_name) = line.strip_prefix("User-agent: ")
                .or_else(|| line.strip_prefix("user-agent: "))
            {
                let agent_name = agent_name.trim();
                if agent_name == "*" || agent_name.eq_ignore_ascii_case("cruggle") {
                    let mut new_robo = extract_rules(&mut lines, &base);
                    robo.allow.append(&mut new_robo.allow);
                    robo.disallow.append(&mut new_robo.disallow);
                    if new_robo.crawl_delay.is_some() {
                        robo.crawl_delay = new_robo.crawl_delay;
                    }
                }
            }
        }
    }
    return robo;
}

// --------------------
// Robots.txt Extraction
// --------------------
fn extract_rules(lines: &mut std::str::Lines<'_>,base: & Url) -> RobotData {
    let mut robo :RobotData = RobotData { disallow: Vec::new(), allow: Vec::new(), crawl_delay: None};

    loop {
        let line = lines.next();
        if line.is_none(){
            break;
        }
        let line: &str = line.unwrap().trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.to_lowercase();
        if line.starts_with("disallow: ") {
            if let Some(ban_name) = line.strip_prefix("disallow: ").map(str::trim) {
                if let Ok(url) = base.join(ban_name) {
                    robo.disallow.push(url.to_string());
                } else {
                    eprintln!("🚨 Skipping invalid disallow path: {}", ban_name);
                }
            }
        }     

        if line.starts_with("allow: ") {
            if let Some(allow_name) = line.strip_prefix("allow: ").map(str::trim) {
                if let Ok(url) = base.join(allow_name) {
                    robo.allow.push(url.to_string());
                } else {
                    eprintln!("🚨 Skipping invalid allow path: {}", allow_name);
                }
            }
        }

        if line.starts_with("crawl-delay: ") {
            if let Some(delay_str) = line.strip_prefix("crawl-delay: ").map(str::trim) {
                if let Ok(delay) = delay_str.parse::<u16>() {
                    robo.crawl_delay = Some(delay);
                } else {
                    eprintln!("🚨 Invalid crawl-delay value: {}", delay_str);
                }
            }
        }

        if line.to_lowercase().starts_with("user-agent: ") {
            break;

        } else {
            continue;
        }
        
    }
    return robo;
}

// --------------------
// URL Normalization
// --------------------
fn normalize_url(mut url: Url) -> String {
    // Lowercase the scheme
    if let Some(host) = url.host_str() {
        let lower_host = host.to_lowercase();
        let canonical_host = match lower_host.as_str() {
            "meta.m.wikimedia.org" => "meta.wikimedia.org",
            "en.m.wikipedia.org" => "en.wikipedia.org",
            // Add more rewrites as needed
            _ => &lower_host,
        };
    
        if url.set_host(Some(canonical_host)).is_err() {
            eprintln!("🚨 Failed to set host");
        }
    }
    
    if url.set_scheme(&url.scheme().to_lowercase()).is_err() {
        eprintln!("🚨 Failed to set scheme");
    }

    // Remove fragments (#section)
    url.set_fragment(None);

    // Clean query parameters
    let important_params = ["title"]; 
    let ignored_params = [
        "utm_source", "utm_medium", "utm_campaign",
        "utm_term", "utm_content", "fbclid", "gclid", "sessionid"
    ];

    let filtered: Vec<(String, String)> = url
        .query_pairs()
        .into_owned()
        .filter(|(k, _)| {
            important_params.contains(&k.as_str()) && !ignored_params.contains(&k.as_str())
        })
        .collect();

    if filtered.is_empty() {
        url.set_query(None);
    } else {
        let new_query: String = filtered
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");
        url.set_query(Some(&new_query));
    }

    // Remove trailing slash (except for root "/")
    let mut path = url.path().to_string();
    if path.ends_with('/') && path.len() > 1 {
        path.pop();
        url.set_path(&path);
    }

    // Collapse multiple slashes
    let collapsed_path = url.path().replace("//", "/");
    url.set_path(&collapsed_path);

    // Canonicalize percent-encoding (normalize UTF-8 vs %XX)
    let normalized = percent_encoding::percent_decode_str(&url.to_string())
        .decode_utf8_lossy()
        .to_string();

    normalized
}

// --------------------
// Scoring Logic
// --------------------
fn compute_score(url: &str, depth: u32, appearance_count: u32, domain_counts: u32,) -> u32 {
    // Starting URL score
    let mut score: u32 = 100;

    // Penalize repeated domains
    let penalty = (domain_counts * 5).min(100);
    score = score.saturating_sub(penalty);

    // Boost links that appear more than once on a page (max +60)
    if appearance_count > 1 {
        score += ((appearance_count - 1).min(4)) * 15;
    }

    // Penalize deeper links in the site hierarchy using log2(depth)
    score = score.saturating_sub((depth as f32).log2().ceil() as u32 * 10);

    let lower_url = url.to_lowercase();

    // Boost if the URL looks like a content page
    if lower_url.contains("blog") || lower_url.contains("news") || lower_url.contains("article") {
        score = score.saturating_add(20);
    }

    // Penalize login/signup/logout pages
    if lower_url.contains("login") || lower_url.contains("signup") || lower_url.contains("logout") {
        score = score.saturating_sub(50);
    }
    // Penalize complex URLs with many slashes
    let slash_penalty = url.matches('/').count() as u32;
    score = score.saturating_sub(slash_penalty);

    // Ensure score doesn't fall below 1
    score.max(1)
}

// --------------------
// HTML Parsing
// --------------------
fn extract_pagedata(current_url: &str, html_body: &str) -> PageData {
    let document = Html::parse_document(&html_body);

    let title = document.select(&Selector::parse("title").unwrap())
    .next()
    .map(|n| n.text().collect::<Vec<_>>().join("").trim().to_string());

    let meta_description = document.select(&Selector::parse("meta[name='description']").unwrap())
        .next()
        .and_then(|n| n.value().attr("content").map(|s| s.to_string()));

    let headings = document.select(&Selector::parse("h1,h2,h3").unwrap())
        .map(|n| n.text().collect::<Vec<_>>().join("").trim().to_string())
        .collect();

    let paragraph_snippets = document.select(&Selector::parse("p").unwrap())
        .take(10) // avoid dumping the whole article
        .map(|n| n.text().collect::<Vec<_>>().join("").trim().to_string())
        .collect();

    let image_alt_texts = document.select(&Selector::parse("img").unwrap())
        .filter_map(|n| n.value().attr("alt").map(|s| s.to_string()))
        .collect();

    let page_data = PageData {
        url: Some(current_url.to_string()),
        title,
        meta_description,
        headings,
        paragraph_snippets,
        image_alt_texts,
    };

    return page_data;
}

// --------------------
// Store Page Data
// --------------------
fn store_page_data(conn: &Connection, data: &PageData, depth: u32) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO pages 
         (url, title, meta_description, headings, paragraph_snippets, image_alt_texts, last_crawled_at, depth) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'), ?7)",
        params![
            data.url.as_deref().unwrap_or(""),
            data.title.as_deref().unwrap_or(""),
            data.meta_description.as_deref().unwrap_or(""),
            data.headings.join("\n"),
            data.paragraph_snippets.join("\n"),
            data.image_alt_texts.join("\n"),
            depth,
        ],
    )?;
    Ok(())
}

// --------------------
// Build Inverted Index
// --------------------
fn build_inverted_index(conn: &mut Connection, page_data: &PageData) -> rusqlite::Result<()> {
    let mut word_freq = std::collections::HashMap::new();

    // Combine all text fields
    let combined_text = format!(
        "{}\n{}\n{}\n{}\n{}",
        page_data.title.clone().unwrap_or_default(),
        page_data.meta_description.clone().unwrap_or_default(),
        page_data.headings.join(" "),
        page_data.paragraph_snippets.join(" "),
        page_data.image_alt_texts.join(" "),
    );

    // Tokenize and count
    for word in combined_text.split_whitespace() {
        let cleaned = word
            .to_lowercase()
            .trim_matches(|c: char| !c.is_alphanumeric())
            .to_string();
        if !cleaned.is_empty() {
            *word_freq.entry(cleaned).or_insert(0) += 1;
        }
    }

    let tx = conn.transaction()?;
    for (word, frequency) in word_freq {
        tx.execute(
            "INSERT OR REPLACE INTO inverted_index (word, url, frequency) VALUES (?1, ?2, ?3)",
            params![
                word,
                page_data.url.as_deref().unwrap_or(""),
                frequency
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

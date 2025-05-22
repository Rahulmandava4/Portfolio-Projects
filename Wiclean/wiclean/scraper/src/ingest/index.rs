use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use std::sync::LazyLock;
use url::Url;

/// Index page for the chunk downloads. Date must be in the past 5 dumps.
const INDEX_URL: &str = "https://wikimedia.bringyour.com/enwiki/20250301/";

pub async fn get_download_urls(client: &Client) -> anyhow::Result<Vec<String>> {
    let html = client.get(INDEX_URL).send().await?.text().await?;
    let urls = find_download_urls(&html);
    Ok(urls
        .into_iter()
        .map(|url| {
            let url = Url::parse(INDEX_URL)
                .unwrap()
                .join(&url)
                .expect("failed to join URL")
                .as_str()
                .to_owned();
            tracing::debug!("found download link: {url}");
            url
        })
        .collect())
}

/// Reads all chunk download URLs from the given index.
///
/// URLs are relative to the index URL.
fn find_download_urls(index: &str) -> Vec<String> {
    let document = Html::parse_document(index);
    let mut urls: Vec<String> = Vec::new();

    for link in document.select(&Selector::parse("a").unwrap()) {
        if let Some(href) = link.attr("href") {
            static REGEX: LazyLock<Regex> =
                LazyLock::new(|| Regex::new(r#".*meta-history.*\.7z$"#).unwrap());
            if REGEX.is_match(href) && !urls.contains(&href.to_owned()) {
                urls.push(href.to_owned());
            }
        }
    }

    urls
}

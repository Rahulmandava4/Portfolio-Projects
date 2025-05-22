use crate::ingest::temp_db::{TempArticle, TempArticleRevision};
use anyhow::{Context, bail};
use compact_str::CompactString;
use jiff::Timestamp;
use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use quick_xml::Reader;
use quick_xml::events::Event;
use regex::Regex;
use std::io::BufRead;
use std::str::FromStr;
use std::sync::LazyLock;

fn earliest_revision_time() -> Timestamp {
    DateTime::new(2023, 1, 1, 0, 0, 0, 0)
        .unwrap()
        .to_zoned(TimeZone::UTC)
        .unwrap()
        .timestamp()
}

/// Parser state machine for the Wikipedia dump format
/// documented [here](https://www.mediawiki.org/wiki/Help:Export).
/// Since the XML documents are massive (upwards of 20 GiB each),
/// we have to write a streaming event-based parser.
pub fn parse<R: BufRead>(
    mut reader: Reader<R>,
    mut article_callback: impl FnMut(TempArticle) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut buf = Vec::new();

    loop {
        let event = reader.read_event_into(&mut buf)?;
        match event {
            Event::Start(tag) if tag.name().into_inner() == b"page" => {
                // parse an article
                let mut title: Option<CompactString> = None;
                let mut revisions: Vec<TempArticleRevision> = Vec::new();
                loop {
                    let event = reader.read_event_into(&mut buf)?;
                    match event {
                        Event::Start(tag) if tag.name().into_inner() == b"title" => {
                            title = Some(read_text(&mut reader, &mut buf)?);
                        }
                        Event::Start(tag) if tag.name().into_inner() == b"revision" => {
                            // parse a revision of this article
                            let mut timestamp: Option<Timestamp> = None;
                            let mut text: Option<CompactString> = None;
                            let mut user_id: Option<i64> = None;
                            loop {
                                let event = reader.read_event_into(&mut buf)?;
                                match event {
                                    Event::Start(tag)
                                        if tag.name().into_inner() == b"timestamp" =>
                                    {
                                        let timestamp_str = read_text(&mut reader, &mut buf)?;
                                        timestamp = Some(Timestamp::from_str(&timestamp_str)?);
                                    }
                                    Event::Start(tag) if tag.name().into_inner() == b"text" => {
                                        text = Some(read_text(&mut reader, &mut buf)?);
                                    }
                                    Event::Start(tag)
                                        if tag.name().into_inner() == b"contributor" =>
                                    {
                                        loop {
                                            let event = reader.read_event_into(&mut buf)?;
                                            match event {
                                                Event::Start(tag)
                                                    if tag.name().into_inner() == b"id" =>
                                                {
                                                    user_id = Some(
                                                        read_text(&mut reader, &mut buf)?
                                                            .trim()
                                                            .parse()?,
                                                    );
                                                }
                                                Event::Start(tag)
                                                    if tag.name().into_inner() == b"ip" =>
                                                {
                                                    // IP users lack a name.
                                                    // Make fake user ID by hashing the IP address.
                                                    let hash = blake3::hash(
                                                        read_text(&mut reader, &mut buf)?
                                                            .trim()
                                                            .as_bytes(),
                                                    );
                                                    user_id = Some(i64::from_be_bytes(
                                                        hash.as_bytes()[..8].try_into().unwrap(),
                                                    ));
                                                }
                                                Event::Start(tag)
                                                    if tag.name().into_inner()
                                                        == b"contributor" =>
                                                {
                                                    bail!("cannot handle nested tags of same name")
                                                }
                                                Event::End(tag)
                                                    if tag.name().into_inner()
                                                        == b"contributor" =>
                                                {
                                                    break;
                                                }
                                                _ => {} // ignore
                                            }
                                            buf.clear();
                                        }
                                    }
                                    Event::Start(tag) if tag.name().into_inner() == b"revision" => {
                                        bail!("cannot handle nested tags of same name")
                                    }
                                    Event::End(tag) if tag.name().into_inner() == b"revision" => {
                                        break;
                                    }
                                    _ => {} // ignore
                                }
                                buf.clear();
                            }
                            let timestamp = timestamp.context("missing revision timestamp")?;
                            if timestamp >= earliest_revision_time() {
                                if revisions.len() > 10_000 {
                                    tracing::debug!(
                                        "too many revisions for article {:?}, skipping some",
                                        title
                                    );
                                } else {
                                    revisions.push(TempArticleRevision {
                                        timestamp,
                                        links: find_links(&text.unwrap_or_default()),
                                        user_id: user_id.unwrap_or(0),
                                    });
                                }
                            }
                        }
                        Event::Start(tag) if tag.name().into_inner() == b"page" => {
                            bail!("cannot handle nested tags of same name")
                        }
                        Event::End(tag) if tag.name().into_inner() == b"page" => break,
                        _ => {} // ignore
                    }
                    buf.clear();
                }
                if !revisions.is_empty() {
                    article_callback(TempArticle {
                        title: title.context("missing article title")?,
                        revisions,
                    })?;
                }
            }
            Event::Eof => break,
            _ => {} // ignore
        }
        buf.clear();
    }

    Ok(())
}

fn read_text<R: BufRead>(
    reader: &mut Reader<R>,
    buf: &mut Vec<u8>,
) -> anyhow::Result<CompactString> {
    let mut all_text = CompactString::default();
    loop {
        let event = reader.read_event_into(buf)?;
        match event {
            Event::Text(text) => {
                all_text.push_str(&text.unescape()?);
            }
            Event::Eof => bail!("unexpected EOF"),
            Event::End(_) => break,
            _ => {}
        }
    }
    Ok(all_text)
}

/// Primitive regex-based solution. Does not
/// correctly handle escapes.
fn find_links(wikitext: &str) -> Vec<CompactString> {
    static REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]"#).unwrap());
    let mut links = Vec::new();
    for capture_group in REGEX.captures_iter(wikitext) {
        let link = capture_group.get(1).unwrap();
        links.push(link.as_str().into());
    }
    links.sort_unstable();
    links.dedup();
    links
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_links() {
        let text = r#"
        San Francisco also has [[Public Transport|public transport]]ation. Examples include [[bus]]es, [[taxicab]]s, and [[tram]]s.
        "#;
        let links = find_links(text);
        let expected: Vec<CompactString> = vec![
            "Public Transport".into(),
            "bus".into(),
            "taxicab".into(),
            "tram".into(),
        ];
        assert_eq!(links, expected);
    }

    #[test]
    fn test_xml_data() {
        let data = include_str!("../../test_xml_data.xml");
        let mut articles = Vec::new();
        parse(Reader::from_str(data), |a| {
            articles.push(a);
            Ok(())
        })
        .unwrap();
    }
}

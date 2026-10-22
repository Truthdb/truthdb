//! A small XML element tree, for request bodies whose elements nest and
//! repeat (a lifecycle configuration's rules, their transitions and tags).
//! Text belongs to leaves only, kept as given (a prefix or tag may end in a
//! space); a root of another name, text beside child elements, or nesting
//! past a fixed depth is malformed.

use quick_xml::Reader;
use quick_xml::events::Event;

use crate::error::S3Error;
use crate::object_lock::malformed;

/// How deep elements may nest.
const MAX_DEPTH: usize = 16;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Element {
    pub name: String,
    /// A leaf's text as given (empty for an element holding others).
    pub text: String,
    pub children: Vec<Element>,
}

/// Parses `body`, whose root element must be `root`.
pub(crate) fn parse(body: &[u8], root: &str) -> Result<Element, S3Error> {
    let mut reader = Reader::from_reader(body);
    let mut open: Vec<Element> = Vec::new();
    let mut done: Option<Element> = None;
    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf).map_err(|_| malformed())?;
        // An empty element is its start and its end.
        let empty = matches!(event, Event::Empty(_));
        match event {
            Event::Start(element) | Event::Empty(element)
                if done.is_none() && open.len() < MAX_DEPTH =>
            {
                let name = String::from_utf8(element.local_name().as_ref().to_vec())
                    .map_err(|_| malformed())?;
                if open.is_empty() && name != root {
                    return Err(malformed());
                }
                open.push(Element {
                    name,
                    ..Element::default()
                });
                if empty {
                    close(&mut open, &mut done)?;
                }
            }
            Event::End(_) => close(&mut open, &mut done)?,
            Event::Text(text) => {
                let text = text.unescape().map_err(|_| malformed())?;
                match open.last_mut() {
                    Some(element) => element.text.push_str(&text),
                    None if text.trim().is_empty() => {}
                    None => return Err(malformed()),
                }
            }
            Event::CData(text) => match open.last_mut() {
                Some(element) => element
                    .text
                    .push_str(std::str::from_utf8(&text).map_err(|_| malformed())?),
                None => return Err(malformed()),
            },
            Event::Eof => break,
            Event::Start(_) | Event::Empty(_) => return Err(malformed()),
            _ => {}
        }
        buf.clear();
    }
    if !open.is_empty() {
        return Err(malformed());
    }
    done.ok_or_else(malformed)
}

/// Closes the innermost open element: into its parent, or as the root.
fn close(open: &mut Vec<Element>, done: &mut Option<Element>) -> Result<(), S3Error> {
    let mut element = open.pop().ok_or_else(malformed)?;
    if !element.children.is_empty() {
        if !element.text.trim().is_empty() {
            return Err(malformed());
        }
        element.text.clear();
    }
    match open.last_mut() {
        Some(parent) => parent.children.push(element),
        None => *done = Some(element),
    }
    Ok(())
}

impl Element {
    /// The children named `name`.
    pub fn all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Element> {
        self.children.iter().filter(move |child| child.name == name)
    }

    /// The child named `name`, if any; given twice, it is malformed.
    pub fn one(&self, name: &str) -> Result<Option<&Element>, S3Error> {
        let mut found = self.children.iter().filter(|child| child.name == name);
        let first = found.next();
        if found.next().is_some() {
            return Err(malformed());
        }
        Ok(first)
    }

    /// The text of the child named `name`, if any.
    pub fn text_of(&self, name: &str) -> Result<Option<&str>, S3Error> {
        Ok(self.one(name)?.map(|child| child.text.as_str()))
    }

    /// Refuses children other than those named in `allowed`.
    pub fn only(&self, allowed: &[&str]) -> Result<(), S3Error> {
        if self
            .children
            .iter()
            .any(|child| !allowed.contains(&child.name.as_str()))
        {
            return Err(malformed());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_reads_as_nested_repeated_elements() {
        let root = parse(
            b"<?xml version=\"1.0\"?><Doc xmlns=\"x\"><Item><Name> a </Name></Item>\
              <Item><Name>b&amp;c</Name><Empty/></Item></Doc>",
            "Doc",
        )
        .unwrap();
        let items: Vec<_> = root.all("Item").collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].text_of("Name").unwrap(), Some(" a "));
        assert_eq!(items[1].text_of("Name").unwrap(), Some("b&c"));
        assert_eq!(items[1].text_of("Empty").unwrap(), Some(""));
        assert_eq!(items[1].text_of("Missing").unwrap(), None);
        assert!(items[0].only(&["Name"]).is_ok());
        assert!(items[1].only(&["Name"]).is_err());
        assert!(root.one("Item").is_err(), "given twice");
        let deep = format!(
            "{}{}",
            "<A>".repeat(MAX_DEPTH + 1),
            "</A>".repeat(MAX_DEPTH + 1)
        );
        for bad in [
            &b"<Other/>"[..],
            b"<Doc><Item>text<Name/></Item></Doc>",
            b"<Doc>",
            b"<Doc></Doc><Doc></Doc>",
            b"stray<Doc/>",
            b"",
            deep.as_bytes(),
        ] {
            let root = if bad == deep.as_bytes() { "A" } else { "Doc" };
            assert!(
                parse(bad, root).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }
}

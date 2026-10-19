//! The two Thrift structures of a Parquet file that are checked before the
//! Parquet reader sees them: each page header, and the schema in the
//! footer.
//!
//! A page header is read exactly as the `parquet` crate (60) reads it, so
//! the sizes checked are the sizes it will use: a field is matched by its id
//! alone, whatever type its header claims; an id is the last one plus the
//! delta, or a full id cut to 16 bits; an integer is a zigzag varint cut to
//! the field's width; a boolean is the field header's type; and an unknown
//! field is skipped by the type it claims, at most 64 levels deep.
//!
//! The schema is read here instead of by the reader (which is handed the
//! result and skips the footer's copy): the reader builds it recursing once
//! per nesting level, so a deep one would overflow the stack. It is read
//! strictly — each field of the type the format gives it — and built without
//! recursion.

use std::sync::Arc;

use parquet::basic::{ConvertedType, LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::schema::types::{SchemaDescriptor, Type};

/// The deepest an unknown field is skipped, as the reader does.
const SKIP_DEPTH: u32 = 64;
/// The most schema elements a footer may hold.
pub(super) const MAX_SCHEMA_ELEMENTS: usize = 16 * 1024;

/// A Thrift compact-protocol reader over a byte slice.
struct Compact<'a> {
    bytes: &'a [u8],
    at: usize,
}

/// A struct field's header.
enum Field {
    Stop,
    Field { kind: u8, id: i16 },
}

impl Compact<'_> {
    fn byte(&mut self) -> Option<u8> {
        let byte = *self.bytes.get(self.at)?;
        self.at += 1;
        Some(byte)
    }

    fn advance(&mut self, len: u64) -> Option<()> {
        let end = self.at.checked_add(usize::try_from(len).ok()?)?;
        (end <= self.bytes.len()).then(|| self.at = end)
    }

    /// A ULEB128 varint, as the reader decodes it (bits past 64 wrap).
    fn vlq(&mut self) -> Option<u64> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7F).wrapping_shl(shift);
            if byte & 0x80 == 0 {
                return Some(value);
            }
            shift = shift.wrapping_add(7);
        }
    }

    fn zigzag(&mut self) -> Option<i64> {
        let value = self.vlq()?;
        Some((value >> 1) as i64 ^ -((value & 1) as i64))
    }

    fn i32(&mut self) -> Option<i32> {
        Some(self.zigzag()? as i32)
    }

    fn field(&mut self, last: i16) -> Option<Field> {
        let head = self.byte()?;
        if head & 0x0F == 0 {
            return Some(Field::Stop);
        }
        let kind = head & 0x0F;
        if kind > 13 {
            return None;
        }
        let id = match head >> 4 {
            0 => self.zigzag()? as i16,
            delta => last.checked_add(i16::from(delta))?,
        };
        Some(Field::Field { kind, id })
    }

    /// A list or set header: its element type (a boolean as 1) and count.
    fn list_begin(&mut self) -> Option<(u8, u64)> {
        let head = self.byte()?;
        if head == 0 {
            return Some((3, 0));
        }
        let element = match head & 0x0F {
            1 | 2 => 1,
            kind @ 3..=13 => kind,
            _ => return None,
        };
        let count = match head >> 4 {
            15 => u64::try_from(i32::try_from(self.vlq()?).ok()?).ok()?,
            count => u64::from(count),
        };
        Some((element, count))
    }

    /// Steps over a value of type `kind` as the reader skips it: a boolean
    /// takes no bytes, even as a list element.
    fn skip(&mut self, kind: u8, depth: u32) -> Option<()> {
        if depth == 0 {
            return None;
        }
        match kind {
            1 | 2 => Some(()),
            3 => self.byte().map(drop),
            4..=6 => self.vlq().map(drop),
            7 => self.advance(8),
            8 => {
                let len = self.vlq()?;
                self.advance(len)
            }
            9 | 10 => {
                let (element, count) = self.list_begin()?;
                if element == 1 {
                    return Some(());
                }
                // Every other element takes at least a byte, so a lying
                // count runs out of input rather than looping on.
                for _ in 0..count {
                    self.skip(element, depth - 1)?;
                }
                Some(())
            }
            11 => {
                let count = u64::try_from(i32::try_from(self.vlq()?).ok()?).ok()?;
                if count > 0 {
                    let kinds = self.byte()?;
                    let key = element_kind(kinds >> 4)?;
                    let value = element_kind(kinds & 0x0F)?;
                    if key == 1 && value == 1 {
                        return Some(());
                    }
                    for _ in 0..count {
                        self.skip(key, depth - 1)?;
                        self.skip(value, depth - 1)?;
                    }
                }
                Some(())
            }
            12 => loop {
                match self.field(0)? {
                    Field::Stop => return Some(()),
                    Field::Field { kind, .. } => self.skip(kind, depth - 1)?,
                }
            },
            13 => self.advance(16),
            _ => None,
        }
    }

    /// Reads a struct's fields to its end: `field` sees each (id, type) and
    /// returns `Some(Some(()))` when it read the value, `Some(None)` to have
    /// it skipped, and `None` to fail.
    fn fields(
        &mut self,
        mut field: impl FnMut(&mut Self, i16, u8) -> Option<Option<()>>,
    ) -> Option<()> {
        let mut last = 0i16;
        loop {
            match self.field(last)? {
                Field::Stop => return Some(()),
                Field::Field { kind, id } => {
                    if field(self, id, kind)?.is_none() {
                        self.skip(kind, SKIP_DEPTH)?;
                    }
                    last = id;
                }
            }
        }
    }
}

fn element_kind(kind: u8) -> Option<u8> {
    match kind {
        1 | 2 => Some(1),
        3..=13 => Some(kind),
        _ => None,
    }
}

/// A boolean field's value: its type, or a failure as in the reader.
fn bool_field(kind: u8) -> Option<bool> {
    match kind {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    }
}

/// What the page fetch checks of a page header.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct PageHeader {
    pub(super) page_type: i32,
    pub(super) uncompressed: i32,
    pub(super) compressed: i32,
    /// A data page's value count (levels included), either version.
    pub(super) values: Option<i32>,
    /// A dictionary page's value count.
    pub(super) dictionary_values: Option<i32>,
    /// A version 2 data page's level bytes, stored uncompressed before its
    /// values: (definition, repetition).
    pub(super) levels: Option<(i32, i32)>,
    /// Whether a version 2 data page's values are compressed.
    pub(super) is_compressed: Option<bool>,
}

pub(super) const DICTIONARY_PAGE: i32 = 2;

/// The header in `bytes`, when they hold exactly one the reader would
/// accept (see the module docs).
pub(super) fn page_header(bytes: &[u8]) -> Option<PageHeader> {
    let mut c = Compact { bytes, at: 0 };
    let (mut page_type, mut uncompressed, mut compressed) = (None, None, None);
    let mut header = PageHeader::default();
    // Matched by id alone, whatever type the field header claims.
    c.fields(|c, id, _| {
        Some(match id {
            1 => {
                page_type = Some(c.i32()?);
                Some(())
            }
            2 => {
                uncompressed = Some(c.i32()?);
                Some(())
            }
            3 => {
                compressed = Some(c.i32()?);
                Some(())
            }
            4 => c.i32().map(drop),
            5 => {
                // DataPageHeader: 1 num_values, 2-4 encodings.
                let mut values = None;
                c.fields(|c, id, _| {
                    Some(match id {
                        1 => {
                            values = Some(c.i32()?);
                            Some(())
                        }
                        2..=4 => c.i32().map(drop),
                        _ => None,
                    })
                })?;
                header.values = values;
                Some(())
            }
            7 => {
                // DictionaryPageHeader: 1 num_values, 2 encoding, 3 is_sorted.
                let mut values = None;
                c.fields(|c, id, kind| {
                    Some(match id {
                        1 => {
                            values = Some(c.i32()?);
                            Some(())
                        }
                        2 => c.i32().map(drop),
                        3 => bool_field(kind).map(drop),
                        _ => None,
                    })
                })?;
                header.dictionary_values = values;
                Some(())
            }
            8 => {
                // DataPageHeaderV2: 1 num_values, 2 num_nulls, 3 num_rows,
                // 4 encoding, 5-6 level lengths, 7 is_compressed.
                let (mut values, mut definition, mut repetition, mut is_compressed) =
                    (None, None, None, None);
                c.fields(|c, id, kind| {
                    Some(match id {
                        1 => {
                            values = Some(c.i32()?);
                            Some(())
                        }
                        2..=4 => c.i32().map(drop),
                        5 => {
                            definition = Some(c.i32()?);
                            Some(())
                        }
                        6 => {
                            repetition = Some(c.i32()?);
                            Some(())
                        }
                        7 => {
                            is_compressed = Some(bool_field(kind)?);
                            Some(())
                        }
                        _ => None,
                    })
                })?;
                header.values = values;
                header.levels = Some((definition.unwrap_or(0), repetition.unwrap_or(0)));
                header.is_compressed = is_compressed;
                Some(())
            }
            // 6 (IndexPageHeader) has no fields: everything in it is skipped.
            _ => None,
        })
    })?;
    (c.at == bytes.len()).then_some(())?;
    header.page_type = page_type?;
    header.uncompressed = uncompressed?;
    header.compressed = compressed?;
    Some(header)
}

/// One schema element, as the footer lists them.
#[derive(Default)]
struct Element {
    physical: Option<i32>,
    type_length: Option<i32>,
    repetition: Option<i32>,
    name: Option<String>,
    num_children: Option<i32>,
    converted: Option<i32>,
    scale: Option<i32>,
    precision: Option<i32>,
    field_id: Option<i32>,
    logical: Option<LogicalType>,
}

/// The schema in a Parquet footer's metadata, or why it cannot be read: at
/// most [`MAX_SCHEMA_ELEMENTS`] elements nested at most `max_depth` deep.
/// Only field 1 (the version) may come before it, as every writer puts it.
pub(super) fn footer_schema(metadata: &[u8], max_depth: usize) -> Result<SchemaDescriptor, String> {
    let bad = || "the file metadata cannot be read".to_string();
    let mut c = Compact {
        bytes: metadata,
        at: 0,
    };
    let mut last = 0i16;
    let elements = loop {
        match c.field(last).ok_or_else(bad)? {
            Field::Field { kind: 5, id: 1 } => {
                c.i32().ok_or_else(bad)?;
                last = 1;
            }
            Field::Field { kind: 9, id: 2 } => break read_elements(&mut c).ok_or_else(bad)?,
            _ => return Err("the file metadata does not start with its schema".to_string()),
        }
    };
    build_schema(elements, max_depth)
}

fn read_elements(c: &mut Compact<'_>) -> Option<Vec<Element>> {
    let (kind, count) = c.list_begin()?;
    if kind != 12 || count as usize > MAX_SCHEMA_ELEMENTS {
        return None;
    }
    (0..count).map(|_| read_element(c)).collect()
}

/// One schema element, each known field of its own type.
fn read_element(c: &mut Compact<'_>) -> Option<Element> {
    let mut element = Element::default();
    c.fields(|c, id, kind| {
        let int = |c: &mut Compact<'_>| (kind == 5).then(|| c.i32()).flatten();
        Some(match id {
            1 => {
                element.physical = Some(int(c)?);
                Some(())
            }
            2 => {
                element.type_length = Some(int(c)?);
                Some(())
            }
            3 => {
                element.repetition = Some(int(c)?);
                Some(())
            }
            4 => {
                (kind == 8).then_some(())?;
                let len = c.vlq()?;
                let start = c.at;
                c.advance(len)?;
                element.name = Some(String::from_utf8(c.bytes[start..c.at].to_vec()).ok()?);
                Some(())
            }
            5 => {
                element.num_children = Some(int(c)?);
                Some(())
            }
            6 => {
                element.converted = Some(int(c)?);
                Some(())
            }
            7 => {
                element.scale = Some(int(c)?);
                Some(())
            }
            8 => {
                element.precision = Some(int(c)?);
                Some(())
            }
            9 => {
                element.field_id = Some(int(c)?);
                Some(())
            }
            10 => {
                (kind == 12).then_some(())?;
                element.logical = read_logical(c)?;
                Some(())
            }
            _ => None,
        })
    })?;
    Some(element)
}

/// A LogicalType union: the types this reader converts, `None` for the
/// rest (read as their converted or physical type).
fn read_logical(c: &mut Compact<'_>) -> Option<Option<LogicalType>> {
    let mut logical = None;
    c.fields(|c, id, kind| {
        (kind == 12).then_some(())?;
        logical = match id {
            5 => {
                let (mut scale, mut precision) = (None, None);
                c.fields(|c, id, kind| {
                    Some(match (id, kind) {
                        (1, 5) => {
                            scale = Some(c.i32()?);
                            Some(())
                        }
                        (2, 5) => {
                            precision = Some(c.i32()?);
                            Some(())
                        }
                        (1 | 2, _) => return None,
                        _ => None,
                    })
                })?;
                Some(LogicalType::decimal(scale?, precision?))
            }
            7 | 8 => {
                let (mut adjusted, mut unit) = (None, None);
                c.fields(|c, field, kind| {
                    Some(match (field, kind) {
                        (1, 1 | 2) => {
                            adjusted = bool_field(kind);
                            Some(())
                        }
                        (2, 12) => {
                            c.fields(|_, id, kind| {
                                (kind == 12).then_some(())?;
                                unit = match id {
                                    1 => Some(TimeUnit::MILLIS),
                                    2 => Some(TimeUnit::MICROS),
                                    3 => Some(TimeUnit::NANOS),
                                    _ => None,
                                };
                                Some(None)
                            })?;
                            Some(())
                        }
                        (1 | 2, _) => return None,
                        _ => None,
                    })
                })?;
                let (adjusted, unit) = (adjusted?, unit?);
                Some(if id == 7 {
                    LogicalType::time(adjusted, unit)
                } else {
                    LogicalType::timestamp(adjusted, unit)
                })
            }
            10 => {
                let (mut width, mut signed) = (None, None);
                c.fields(|c, field, kind| {
                    Some(match (field, kind) {
                        (1, 3) => {
                            width = Some(c.byte()? as i8);
                            Some(())
                        }
                        (2, 1 | 2) => {
                            signed = bool_field(kind);
                            Some(())
                        }
                        (1 | 2, _) => return None,
                        _ => None,
                    })
                })?;
                Some(LogicalType::integer(width?, signed?))
            }
            other => {
                c.skip(12, SKIP_DEPTH)?;
                match other {
                    1 => Some(LogicalType::String),
                    2 => Some(LogicalType::Map),
                    3 => Some(LogicalType::List),
                    4 => Some(LogicalType::Enum),
                    6 => Some(LogicalType::Date),
                    12 => Some(LogicalType::Json),
                    13 => Some(LogicalType::Bson),
                    14 => Some(LogicalType::Uuid),
                    15 => Some(LogicalType::Float16),
                    _ => None,
                }
            }
        };
        Some(Some(()))
    })?;
    Some(logical)
}

fn physical_type(value: i32) -> Result<PhysicalType, String> {
    Ok(match value {
        0 => PhysicalType::BOOLEAN,
        1 => PhysicalType::INT32,
        2 => PhysicalType::INT64,
        3 => PhysicalType::INT96,
        4 => PhysicalType::FLOAT,
        5 => PhysicalType::DOUBLE,
        6 => PhysicalType::BYTE_ARRAY,
        7 => PhysicalType::FIXED_LEN_BYTE_ARRAY,
        other => return Err(format!("unknown physical type {other}")),
    })
}

fn repetition(value: i32) -> Result<Repetition, String> {
    Ok(match value {
        0 => Repetition::REQUIRED,
        1 => Repetition::OPTIONAL,
        2 => Repetition::REPEATED,
        other => return Err(format!("unknown repetition {other}")),
    })
}

fn converted_type(value: Option<i32>) -> Result<ConvertedType, String> {
    use ConvertedType as C;
    Ok(match value {
        None => C::NONE,
        Some(value) => match value {
            0 => C::UTF8,
            1 => C::MAP,
            2 => C::MAP_KEY_VALUE,
            3 => C::LIST,
            4 => C::ENUM,
            5 => C::DECIMAL,
            6 => C::DATE,
            7 => C::TIME_MILLIS,
            8 => C::TIME_MICROS,
            9 => C::TIMESTAMP_MILLIS,
            10 => C::TIMESTAMP_MICROS,
            11 => C::UINT_8,
            12 => C::UINT_16,
            13 => C::UINT_32,
            14 => C::UINT_64,
            15 => C::INT_8,
            16 => C::INT_16,
            17 => C::INT_32,
            18 => C::INT_64,
            19 => C::JSON,
            20 => C::BSON,
            21 => C::INTERVAL,
            other => return Err(format!("unknown converted type {other}")),
        },
    })
}

/// A group being built: its element and the children built so far.
struct Open {
    element: Element,
    is_root: bool,
    children: Vec<Arc<Type>>,
    remaining: usize,
}

/// Builds the schema tree from its elements in the order the footer lists
/// them (each group followed by its children), with a stack instead of
/// recursion, by the rules the Parquet reader applies.
fn build_schema(elements: Vec<Element>, max_depth: usize) -> Result<SchemaDescriptor, String> {
    let mut stack: Vec<Open> = Vec::new();
    let mut root = None;
    let count = elements.len();
    for (index, element) in elements.into_iter().enumerate() {
        if root.is_some() {
            return Err("the schema has more than one root".to_string());
        }
        let is_root = index == 0;
        let children = match element.num_children {
            None | Some(0) => 0,
            Some(n) => usize::try_from(n).map_err(|_| "a negative child count".to_string())?,
        };
        let built = if children == 0 {
            Some(build_type(element, is_root, None)?)
        } else {
            if stack.len() >= max_depth {
                return Err(format!("the schema nests deeper than {max_depth} levels"));
            }
            if children > count {
                return Err("a group with more children than elements".to_string());
            }
            stack.push(Open {
                element,
                is_root,
                children: Vec::with_capacity(children),
                remaining: children,
            });
            None
        };
        let mut done = built;
        while let Some(child) = done.take() {
            match stack.last_mut() {
                None => root = Some(child),
                Some(open) => {
                    open.children.push(child);
                    open.remaining -= 1;
                    if open.remaining == 0 {
                        let open = stack.pop().expect("an open group");
                        done = Some(build_type(open.element, open.is_root, Some(open.children))?);
                    }
                }
            }
        }
    }
    if !stack.is_empty() {
        return Err("a group with fewer children than it says".to_string());
    }
    let root = root.ok_or_else(|| "the schema is empty".to_string())?;
    if !root.is_group() {
        return Err("the schema's root is not a group".to_string());
    }
    Ok(SchemaDescriptor::new(root))
}

/// One schema node, as the Parquet reader builds it from its element: a
/// group with `children`, or (with `None`) an element that has none.
fn build_type(
    element: Element,
    is_root: bool,
    children: Option<Vec<Arc<Type>>>,
) -> Result<Arc<Type>, String> {
    let name = element
        .name
        .ok_or_else(|| "a schema element with no name".to_string())?;
    let converted = converted_type(element.converted)?;
    let field_id = element.field_id;
    let built = if let Some(children) = children {
        let mut builder = Type::group_type_builder(&name)
            .with_converted_type(converted)
            .with_logical_type(element.logical)
            .with_fields(children)
            .with_id(field_id);
        if !is_root {
            let repetition = repetition(
                element
                    .repetition
                    .ok_or_else(|| "a field with no repetition".to_string())?,
            )?;
            builder = builder.with_repetition(repetition);
        }
        builder.build()
    } else {
        if is_root {
            Type::group_type_builder(&name).build()
        } else {
            let repetition = repetition(
                element
                    .repetition
                    .ok_or_else(|| "a field with no repetition".to_string())?,
            )?;
            match element.physical {
                Some(physical) => Type::primitive_type_builder(&name, physical_type(physical)?)
                    .with_repetition(repetition)
                    .with_converted_type(converted)
                    .with_logical_type(element.logical)
                    .with_length(element.type_length.unwrap_or(-1))
                    .with_precision(element.precision.unwrap_or(-1))
                    .with_scale(element.scale.unwrap_or(-1))
                    .with_id(field_id)
                    .build(),
                None => Type::group_type_builder(&name)
                    .with_converted_type(converted)
                    .with_logical_type(element.logical)
                    .with_id(field_id)
                    .with_repetition(repetition)
                    .build(),
            }
        }
    };
    built.map(Arc::new).map_err(|err| err.to_string())
}

#[cfg(test)]
pub(in crate::engine) mod tests {
    use super::*;

    /// File metadata: the version, then a schema list of `elements`, each
    /// given by its child count (written as `num_children_kind`: 5 for i32,
    /// 6 for i64) and named `x`.
    pub(in crate::engine) fn metadata(elements: &[i64], num_children_kind: u8) -> Vec<u8> {
        let mut out = vec![0x15, 0x02]; // field 1, version 1
        out.push(0x19); // field 2, a list
        out.push(0xFC); // of structs, count follows
        let mut count = elements.len() as u64;
        loop {
            let byte = (count & 0x7F) as u8;
            count >>= 7;
            out.push(if count > 0 { byte | 0x80 } else { byte });
            if count == 0 {
                break;
            }
        }
        for (index, &children) in elements.iter().enumerate() {
            out.push(0x48); // field 4, a binary name
            out.push(1);
            out.push(b'x');
            if children > 0 {
                out.push(0x10 | num_children_kind); // field 5
                out.push((children * 2) as u8);
                if index > 0 {
                    // Field 3 (REQUIRED), a long-form id after field 5.
                    out.extend_from_slice(&[0x05, 0x06, 0x00]);
                }
            } else {
                // A leaf: field 1 (physical type INT32) and field 3
                // (REQUIRED), long-form ids since they come after field 4.
                out.extend_from_slice(&[0x05, 0x02, 0x02, 0x05, 0x06, 0x00]);
            }
            out.push(0);
        }
        out.push(0);
        out
    }

    #[test]
    fn a_page_header_is_read_as_the_parquet_reader_reads_it() {
        // type 0, uncompressed 100, compressed 60, a data page header with
        // num_values 7 and a statistics struct skipped.
        let header = [
            0x15, 0x00, 0x15, 0xC8, 0x01, 0x15, 0x78, // fields 1-3
            0x2C, 0x15, 0x0E, 0x15, 0x00, 0x15, 0x00, 0x15, 0x00, 0x1C, 0x00, 0x00, // field 5
            0x00,
        ];
        let read = page_header(&header).unwrap();
        assert_eq!(
            (
                read.page_type,
                read.uncompressed,
                read.compressed,
                read.values
            ),
            (0, 100, 60, Some(7))
        );
        assert_eq!(
            page_header(&[&header[..], &[0]].concat()),
            None,
            "trailing bytes"
        );
        assert_eq!(page_header(&header[..header.len() - 1]), None, "cut short");
        // A size given again, as a long-form i64 field: the reader keeps the
        // last value, whatever the type, and so does this.
        let again = [
            0x15, 0x00, 0x15, 0x30, 0x15, 0x78, // sizes 24 and 60
            0x06, 0x04, 0x80, 0x80, 0x80, 0x80, 0x08, // field 2 again, i64 1 GiB
            0x00,
        ];
        assert_eq!(page_header(&again).unwrap().uncompressed, 1 << 30);
        // A version 2 data page: level lengths as an i64 field still count.
        let v2 = [
            0x15, 0x06, 0x15, 0xC8, 0x01, 0x15, 0x78, 0x5C, // type 3, sizes, field 8
            0x15, 0x02, 0x15, 0x00, 0x15, 0x02, 0x15, 0x02, // fields 1-4
            0x16, 0x08, 0x15, 0x06, 0x12, 0x00, // 5 = 4 (as i64), 6 = 3, 7 = false
            0x00,
        ];
        let read = page_header(&v2).unwrap();
        assert_eq!(
            (read.levels, read.is_compressed),
            (Some((4, 3)), Some(false))
        );
        // A dictionary page's value count.
        let dictionary = [
            0x15, 0x04, 0x15, 0x08, 0x15, 0x08, 0x4C, 0x15, 0xFE, 0xFF, 0xFF, 0xFF, 0x0F, 0x00,
            0x00,
        ];
        let read = page_header(&dictionary).unwrap();
        assert_eq!(
            (read.page_type, read.dictionary_values),
            (2, Some(i32::MAX))
        );
        // Nested lists in a skipped field are bounded by the skip depth, not
        // the stack.
        let mut deep = vec![0x15, 0x00, 0x15, 0x02, 0x15, 0x02, 0x99];
        deep.extend(std::iter::repeat_n(0x19, 100_000));
        assert_eq!(page_header(&deep), None);
    }

    #[test]
    fn the_footer_schema_is_built_without_recursing_and_strictly() {
        // message { a; group g { b; group h { c; } } d; }
        let schema = footer_schema(&metadata(&[3, 0, 2, 0, 1, 0, 0], 5), 32).unwrap();
        assert_eq!(schema.num_columns(), 4);
        assert_eq!(schema.column(2).path().string(), "x.x.x");
        // Too deep, measured without recursing; too many elements, refused
        // before they are read.
        let mut chain = vec![1; 1000];
        chain.push(0);
        let err = footer_schema(&metadata(&chain, 5), 32).unwrap_err();
        assert!(err.contains("deeper"), "{err}");
        let mut long = vec![1; MAX_SCHEMA_ELEMENTS];
        long.push(0);
        assert!(footer_schema(&metadata(&long, 5), usize::MAX).is_err());
        // A child count written as an i64 (which the reader would read as
        // its i32) is refused rather than misread.
        assert!(footer_schema(&metadata(&chain, 6), 32).is_err());
        assert!(footer_schema(&metadata(&[3, 0, 2, 0, 1, 0, 0], 6), 32).is_err());
        // A group with more children than elements, and a second root.
        assert!(footer_schema(&metadata(&[5, 0], 5), 32).is_err());
        assert!(footer_schema(&metadata(&[1, 0, 0], 5), 32).is_err());
        assert!(footer_schema(&metadata(&[1, 0], 5)[..10], 32).is_err());
    }
}

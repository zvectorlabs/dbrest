//! Query parameter parsing
//!
//! URL query parameter parsing for dbrest.
//! Parses URL query strings into structured types for select, filter, order,
//! logic trees, columns, and ranges.

use compact_str::CompactString;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};

use crate::error::Error;

use super::range::Range;
use super::types::*;

// ==========================================================================
// QueryParams struct
// ==========================================================================

/// Parsed query parameters.
///
/// Parsed URL query parameters.
#[derive(Debug, Clone)]
pub struct QueryParams {
    /// Canonical query string (sorted alphabetically)
    pub canonical: String,
    /// RPC parameters (key-value pairs without operators)
    pub params: Vec<(CompactString, CompactString)>,
    /// Ranges from &limit and &offset params
    pub ranges: HashMap<CompactString, Range>,
    /// Order parameters for each embed level
    pub order: Vec<(EmbedPath, Vec<OrderTerm>)>,
    /// Logic trees for &and and &or parameters
    pub logic: Vec<(EmbedPath, LogicTree)>,
    /// &columns parameter
    pub columns: Option<HashSet<FieldName>>,
    /// &select parameter parsed into a tree
    pub select: Vec<SelectItem>,
    /// All filters (embed_path, filter)
    pub filters: Vec<(EmbedPath, Filter)>,
    /// Filters on the root table only (no embed path)
    pub filters_root: Vec<Filter>,
    /// Filters on embedded tables (non-root)
    pub filters_not_root: Vec<(EmbedPath, Filter)>,
    /// Set of field names that have filters
    pub filter_fields: HashSet<FieldName>,
    /// &on_conflict parameter
    pub on_conflict: Option<Vec<FieldName>>,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            canonical: String::new(),
            params: Vec::new(),
            ranges: HashMap::new(),
            order: Vec::new(),
            logic: Vec::new(),
            columns: None,
            select: vec![SelectItem::Field {
                field: ("*".into(), SmallVec::new()),
                alias: None,
                cast: None,
                aggregate: None,
                aggregate_cast: None,
            }],
            filters: Vec::new(),
            filters_root: Vec::new(),
            filters_not_root: Vec::new(),
            filter_fields: HashSet::new(),
            on_conflict: None,
        }
    }
}

/// Parse query parameters from a URL query string.
///
/// Parse query parameters from a URL query string.
pub fn parse(is_rpc_read: bool, query_string: &str) -> Result<QueryParams, Error> {
    let pairs: Vec<(String, String)> = form_urlencoded::parse(query_string.as_bytes())
        .into_owned()
        .collect();

    // Build canonical representation (sorted)
    let mut sorted_pairs = pairs.clone();
    sorted_pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical = sorted_pairs
        .iter()
        .map(|(k, v)| {
            if v.is_empty() {
                form_urlencoded::byte_serialize(k.as_bytes()).collect::<String>()
            } else {
                format!(
                    "{}={}",
                    form_urlencoded::byte_serialize(k.as_bytes()).collect::<String>(),
                    form_urlencoded::byte_serialize(v.as_bytes()).collect::<String>()
                )
            }
        })
        .collect::<Vec<_>>()
        .join("&");

    // Categorize parameters
    let reserved = ["select", "columns", "on_conflict"];
    let reserved_embeddable = ["order", "limit", "offset", "and", "or"];

    let select_str = pairs
        .iter()
        .find(|(k, _)| k == "select")
        .map(|(_, v)| v.as_str())
        .unwrap_or("*");

    let columns_str = pairs
        .iter()
        .find(|(k, _)| k == "columns")
        .map(|(_, v)| v.as_str());

    let on_conflict_str = pairs
        .iter()
        .find(|(k, _)| k == "on_conflict")
        .map(|(_, v)| v.as_str());

    // Parse select
    let select = parse_select(select_str)?;

    // Parse columns
    let columns = match columns_str {
        Some(s) => Some(parse_columns(s)?),
        None => None,
    };

    // Parse on_conflict
    let on_conflict = match on_conflict_str {
        Some(s) => Some(parse_columns_list(s)?),
        None => None,
    };

    // Separate order, limit, offset, logic, and filter params
    let mut order_params = Vec::new();
    let mut logic_params = Vec::new();
    let mut limit_params = Vec::new();
    let mut offset_params = Vec::new();
    let mut filter_params = Vec::new();

    for (k, v) in &pairs {
        if v.is_empty() && reserved.contains(&k.as_str()) {
            continue;
        }
        if reserved.contains(&k.as_str()) {
            continue;
        }

        let last_word = k.rsplit('.').next().unwrap_or(k);
        if last_word == "order" {
            order_params.push((k.as_str(), v.as_str()));
        } else if last_word == "limit" {
            limit_params.push((k.as_str(), v.as_str()));
        } else if last_word == "offset" {
            offset_params.push((k.as_str(), v.as_str()));
        } else if last_word == "and" || last_word == "or" {
            logic_params.push((k.as_str(), v.as_str()));
        } else if !reserved_embeddable.contains(&last_word) {
            filter_params.push((k.as_str(), v.as_str()));
        }
    }

    // Parse orders
    let mut order = Vec::new();
    for (k, v) in &order_params {
        let (path, _) = parse_tree_path(k)?;
        let terms = parse_order(v)?;
        order.push((path, terms));
    }

    // Parse logic trees
    let mut logic = Vec::new();
    for (k, v) in &logic_params {
        let (mut path, op) = parse_logic_path(k)?;
        // Remove "not" from path, prepend to operator
        let negated = path.contains(&CompactString::from("not"));
        path.retain(|s| s.as_str() != "not");

        let op_str = if negated {
            format!("not.{}{}", op, v)
        } else {
            format!("{}{}", op, v)
        };
        let tree = parse_logic_tree(&op_str)?;
        logic.push((path, tree));
    }

    // Parse ranges from limit/offset
    let mut ranges: HashMap<CompactString, Range> = HashMap::new();

    for (k, v) in &limit_params {
        let embed_key = replace_last_segment(k, "limit");
        if let Ok(limit) = v.parse::<i64>() {
            let range = Range::all().restrict(Some(limit));
            ranges.insert(CompactString::from(embed_key), range);
        }
    }

    for (k, v) in &offset_params {
        // Map offset key to limit key for merging
        let embed_key = replace_last_segment(k, "limit");
        if let Ok(offset) = v.parse::<i64>() {
            let entry = ranges
                .entry(CompactString::from(embed_key))
                .or_insert_with(Range::all);
            // Combine offset with existing limit
            if let Some(limit) = entry.limit() {
                *entry = Range::new(offset, offset + limit - 1);
            } else {
                *entry = Range::from_offset(offset);
            }
        }
    }

    // Parse filters
    let mut all_filters = Vec::new();
    let mut rpc_params = Vec::new();
    let mut filter_fields = HashSet::new();

    for (k, v) in &filter_params {
        filter_fields.insert(CompactString::from(*k));

        // Reject casting in filter field names (PostgREST restriction)
        if k.contains("::") {
            return Err(Error::InvalidQueryParam {
                param: "filter".to_string(),
                message: "casting not allowed in filters".to_string(),
            });
        }

        let (path, field) = parse_tree_path(k)?;
        let op_expr = parse_filter_value(v, is_rpc_read)?;

        match &op_expr {
            OpExpr::NoOp(val) => {
                rpc_params.push((field.0.clone(), val.clone()));
            }
            _ => {
                all_filters.push((path, Filter { field, op_expr }));
            }
        }
    }

    // Split into root and non-root filters
    let mut filters_root = Vec::new();
    let mut filters_not_root = Vec::new();
    for (path, filter) in &all_filters {
        if path.is_empty() {
            filters_root.push(filter.clone());
        } else {
            filters_not_root.push((path.clone(), filter.clone()));
        }
    }

    Ok(QueryParams {
        canonical,
        params: rpc_params,
        ranges,
        order,
        logic,
        columns,
        select,
        filters: all_filters,
        filters_root,
        filters_not_root,
        filter_fields,
        on_conflict,
    })
}

// ==========================================================================
// Select parser
// ==========================================================================

/// Parse the `select=` parameter into a list of SelectItems.
///
/// Handles: fields, aliases, casts, JSON paths, aggregates,
/// relation embeddings, spread syntax, hints, join types.
pub fn parse_select(input: &str) -> Result<Vec<SelectItem>, Error> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut items = Vec::new();
    let mut chars = input;

    while !chars.is_empty() {
        let (item, rest) = parse_select_item(chars)?;
        items.push(item);

        chars = rest.trim_start();
        if chars.starts_with(',') {
            chars = &chars[1..];
            chars = chars.trim_start();
        } else if !chars.is_empty() && !chars.starts_with(')') {
            return Err(Error::ParseError {
                location: "select".to_string(),
                message: format!("unexpected character '{}' in select", &chars[..1]),
            });
        }
    }

    Ok(items)
}

fn parse_select_item(input: &str) -> Result<(SelectItem, &str), Error> {
    let input = input.trim_start();

    // Check for spread: ...relation(...)
    if let Some(rest) = input.strip_prefix("...") {
        return parse_spread_relation(rest);
    }

    // Try to parse as relation or field
    // First, try to detect alias: look for "name:" where : is not followed by :
    let (alias, after_alias) = try_parse_alias(input);

    // Check if this is a relation (has parentheses for children)
    // A relation is: name!hint!jointype(...) or name(...)
    let after = after_alias;

    // Try to parse relation first
    if let Some((item, rest)) = try_parse_relation(after, alias.clone())? {
        return Ok((item, rest));
    }

    // Otherwise parse as field
    parse_field_select(after, alias)
}

fn parse_spread_relation(input: &str) -> Result<(SelectItem, &str), Error> {
    // Parse: name!hint!jointype(children)
    let (name, rest) = parse_field_name(input)?;

    // Check that name is not "count"
    let (hint, join_type, rest) = parse_embed_params(rest);

    // Must have opening paren
    if !rest.starts_with('(') {
        return Err(Error::ParseError {
            location: "select".to_string(),
            message: format!("expected '(' after spread relation '{}'", name),
        });
    }

    let (children, rest) = parse_children(&rest[1..])?;

    Ok((
        SelectItem::Spread {
            relation: name,
            hint,
            join_type,
            children,
        },
        rest,
    ))
}

fn try_parse_relation(
    input: &str,
    alias: Option<Alias>,
) -> Result<Option<(SelectItem, &str)>, Error> {
    // Parse name
    let (name, rest) = match parse_field_name(input) {
        Ok(r) => r,
        Err(_) => return Ok(None),
    };

    // "count" as a relation name is not allowed (it's the count() aggregate)
    if name.as_str() == "count" {
        return Ok(None);
    }

    let (hint, join_type, rest) = parse_embed_params(rest);

    // Must have opening paren for a relation
    if !rest.starts_with('(') {
        return Ok(None);
    }

    let (children, rest) = parse_children(&rest[1..])?;

    Ok(Some((
        SelectItem::Relation {
            relation: name,
            alias,
            hint,
            join_type,
            children,
        },
        rest,
    )))
}

fn parse_field_select(input: &str, alias: Option<Alias>) -> Result<(SelectItem, &str), Error> {
    // Handle star
    if let Some(rest) = input.strip_prefix('*') {
        // Star can't have anything after it except delimiter
        return Ok((
            SelectItem::Field {
                field: ("*".into(), SmallVec::new()),
                alias: None,
                cast: None,
                aggregate: None,
                aggregate_cast: None,
            },
            rest,
        ));
    }

    // Handle standalone count()
    if let Some(after_count) = input.strip_prefix("count()") {
        let (agg_cast, rest) = parse_optional_cast(after_count);
        return Ok((
            SelectItem::Field {
                field: ("*".into(), SmallVec::new()),
                alias,
                cast: None,
                aggregate: Some(AggregateFunction::Count),
                aggregate_cast: agg_cast,
            },
            rest,
        ));
    }

    // Parse field name and JSON path
    let (name, rest) = parse_field_name(input)?;
    let (json_path, rest) = parse_json_path(rest);

    // Parse cast
    let (cast, rest) = parse_optional_cast(rest);

    // Parse aggregate
    let (aggregate, rest) = parse_optional_aggregate(rest);
    let (aggregate_cast, rest) = if aggregate.is_some() {
        parse_optional_cast(rest)
    } else {
        (None, rest)
    };

    Ok((
        SelectItem::Field {
            field: (name, json_path),
            alias,
            cast,
            aggregate,
            aggregate_cast,
        },
        rest,
    ))
}

fn parse_children(input: &str) -> Result<(Vec<SelectItem>, &str), Error> {
    let items = parse_select_until_close(input)?;
    Ok(items)
}

fn parse_select_until_close(input: &str) -> Result<(Vec<SelectItem>, &str), Error> {
    let mut items = Vec::new();
    let mut chars = input;

    loop {
        chars = chars.trim_start();
        if let Some(after_close) = chars.strip_prefix(')') {
            return Ok((items, after_close));
        }
        if chars.is_empty() {
            return Err(Error::ParseError {
                location: "select".to_string(),
                message: "unclosed parenthesis in select".to_string(),
            });
        }

        let (item, rest) = parse_select_item(chars)?;
        items.push(item);
        chars = rest.trim_start();

        if let Some(after_comma) = chars.strip_prefix(',') {
            chars = after_comma;
        }
    }
}

// ==========================================================================
// Field name parser
// ==========================================================================

fn parse_field_name(input: &str) -> Result<(FieldName, &str), Error> {
    // Handle quoted names
    if input.starts_with('"') {
        return parse_quoted_value(input).map(|(v, r)| (CompactString::from(v), r));
    }

    // Handle star
    if let Some(rest) = input.strip_prefix('*') {
        return Ok(("*".into(), rest));
    }

    // Regular field name: letters, digits, _, $, spaces (trimmed), dashes (not ->)
    let mut end = 0;
    let bytes = input.as_bytes();
    let len = bytes.len();

    while end < len {
        let ch = bytes[end] as char;
        if ch.is_alphanumeric() || ch == '_' || ch == '$' || ch == ' ' {
            end += 1;
        } else if ch == '-' && end + 1 < len && bytes[end + 1] != b'>' {
            // Dash that's not part of -> or ->>
            end += 1;
        } else {
            break;
        }
    }

    if end == 0 {
        return Err(Error::ParseError {
            location: "field name".to_string(),
            message: format!(
                "expected field name, got '{}'",
                &input[..input.len().min(10)]
            ),
        });
    }

    let name = input[..end].trim();
    Ok((CompactString::from(name), &input[end..]))
}

fn parse_quoted_value(input: &str) -> Result<(&str, &str), Error> {
    if !input.starts_with('"') {
        return Err(Error::ParseError {
            location: "quoted value".to_string(),
            message: "expected opening quote".to_string(),
        });
    }

    let bytes = input.as_bytes();
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            i += 2; // skip escaped char
        } else if bytes[i] == b'"' {
            return Ok((&input[1..i], &input[i + 1..]));
        } else {
            i += 1;
        }
    }

    Err(Error::ParseError {
        location: "quoted value".to_string(),
        message: "unclosed quote".to_string(),
    })
}

// ==========================================================================
// JSON path parser
// ==========================================================================

fn parse_json_path(input: &str) -> (JsonPath, &str) {
    let mut path = SmallVec::new();
    let mut rest = input;

    loop {
        if let Some(after) = rest.strip_prefix("->>") {
            match parse_json_operand(after) {
                Ok((operand, r)) => {
                    path.push(JsonOperation::Arrow2(operand));
                    rest = r;
                }
                Err(_) => break,
            }
        } else if let Some(after) = rest.strip_prefix("->") {
            match parse_json_operand(after) {
                Ok((operand, r)) => {
                    path.push(JsonOperation::Arrow(operand));
                    rest = r;
                }
                Err(_) => break,
            }
        } else {
            break;
        }
    }

    (path, rest)
}

fn parse_json_operand(input: &str) -> Result<(JsonOperand, &str), Error> {
    // Try index first (digits, possibly preceded by -)
    let bytes = input.as_bytes();
    if !bytes.is_empty() {
        let (has_sign, start) = if bytes[0] == b'-' {
            (true, 1)
        } else {
            (false, 0)
        };

        if start < bytes.len() && bytes[start].is_ascii_digit() {
            let mut end = start + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            // Check that next char is a delimiter
            if end >= bytes.len()
                || bytes[end] == b'-'
                || bytes[end] == b':'
                || bytes[end] == b'.'
                || bytes[end] == b','
                || bytes[end] == b')'
            {
                let sign = if has_sign { "-" } else { "+" };
                let idx = format!("{}{}", sign, &input[start..end]);
                return Ok((JsonOperand::Idx(CompactString::from(idx)), &input[end..]));
            }
        }
    }

    // Parse key
    let mut end = 0;
    while end < bytes.len() {
        let ch = bytes[end] as char;
        // Stop at delimiters
        if ch == '(' || ch == '-' || ch == ':' || ch == '.' || ch == ',' || ch == '>' || ch == ')' {
            break;
        }
        end += 1;
    }

    if end == 0 {
        return Err(Error::ParseError {
            location: "json operand".to_string(),
            message: "empty json key".to_string(),
        });
    }

    Ok((
        JsonOperand::Key(CompactString::from(&input[..end])),
        &input[end..],
    ))
}

// ==========================================================================
// Alias parser
// ==========================================================================

fn try_parse_alias(input: &str) -> (Option<Alias>, &str) {
    // Look for "name:" where : is not followed by :
    // Be careful: don't consume past special chars
    let bytes = input.as_bytes();
    let mut colon_pos = None;

    for (i, &b) in bytes.iter().enumerate() {
        if b == b':' {
            if i + 1 < bytes.len() && bytes[i + 1] == b':' {
                // This is :: (cast), not alias separator
                break;
            }
            colon_pos = Some(i);
            break;
        }
        // Stop at characters that can't be in a field name
        if b == b'(' || b == b')' || b == b',' || b == b'!' || b == b'.' {
            break;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'>' {
            break;
        }
    }

    match colon_pos {
        Some(pos) if pos > 0 => {
            let alias = input[..pos].trim();
            (Some(CompactString::from(alias)), &input[pos + 1..])
        }
        _ => (None, input),
    }
}

// ==========================================================================
// Embed params (!hint, !inner, !left)
// ==========================================================================

fn parse_embed_params(input: &str) -> (Option<Hint>, Option<JoinType>, &str) {
    let mut hint = None;
    let mut join_type = None;
    let mut rest = input;

    // Parse up to 2 embed params
    for _ in 0..2 {
        if let Some(after_bang) = rest.strip_prefix('!') {
            if let Some(r) = after_bang.strip_prefix("left")
                && !r.starts_with(|c: char| c.is_alphanumeric() || c == '_')
            {
                join_type = join_type.or(Some(JoinType::Left));
                rest = r;
                continue;
            }
            if let Some(r) = after_bang.strip_prefix("inner")
                && !r.starts_with(|c: char| c.is_alphanumeric() || c == '_')
            {
                join_type = join_type.or(Some(JoinType::Inner));
                rest = r;
                continue;
            }
            // It's a hint
            if let Ok((name, r)) = parse_field_name(after_bang) {
                hint = hint.or(Some(name));
                rest = r;
                continue;
            }
        }
        break;
    }

    (hint, join_type, rest)
}

// ==========================================================================
// Cast parser
// ==========================================================================

fn parse_optional_cast(input: &str) -> (Option<Cast>, &str) {
    if let Some(rest) = input.strip_prefix("::") {
        let mut end = 0;
        let bytes = rest.as_bytes();
        while end < bytes.len() {
            let ch = bytes[end] as char;
            if ch.is_alphanumeric() || ch == '_' || ch == ' ' {
                end += 1;
            } else {
                break;
            }
        }
        if end > 0 {
            let cast = rest[..end].trim();
            (Some(CompactString::from(cast)), &rest[end..])
        } else {
            (None, input) // :: but nothing after it
        }
    } else {
        (None, input)
    }
}

// ==========================================================================
// Aggregate parser
// ==========================================================================

fn parse_optional_aggregate(input: &str) -> (Option<AggregateFunction>, &str) {
    if !input.starts_with('.') {
        return (None, input);
    }

    let rest = &input[1..];
    let aggregates = [
        ("count()", AggregateFunction::Count),
        ("sum()", AggregateFunction::Sum),
        ("avg()", AggregateFunction::Avg),
        ("max()", AggregateFunction::Max),
        ("min()", AggregateFunction::Min),
    ];

    for (prefix, agg) in &aggregates {
        if let Some(after) = rest.strip_prefix(prefix) {
            return (Some(*agg), after);
        }
    }

    (None, input) // Not a recognized aggregate
}

// ==========================================================================
// Filter parser
// ==========================================================================

/// Parse a filter value string (the RHS of `column=value`).
fn parse_filter_value(value: &str, is_rpc_read: bool) -> Result<OpExpr, Error> {
    match try_parse_op_expr(value) {
        Ok(expr) => Ok(expr),
        Err(_) if is_rpc_read => Ok(OpExpr::NoOp(CompactString::from(value))),
        Err(e) => Err(e),
    }
}

fn try_parse_op_expr(value: &str) -> Result<OpExpr, Error> {
    // Check for "not." prefix
    let (negated, rest) = if let Some(after) = value.strip_prefix("not.") {
        (true, after)
    } else {
        (false, value)
    };

    let operation = parse_operation(rest)?;
    Ok(OpExpr::Expr { negated, operation })
}

fn parse_operation(input: &str) -> Result<Operation, Error> {
    // Try each operation type in order (by parser priority)

    // in.(val1,val2)
    if let Some(rest) = input.strip_prefix("in.") {
        let list = parse_list_val(rest)?;
        return Ok(Operation::In(list));
    }

    // is.null, is.true, is.false, is.unknown, is.not_null
    if let Some(rest) = input.strip_prefix("is.") {
        let val = parse_is_val(rest)?;
        return Ok(Operation::Is(val));
    }

    // isdistinct.value
    if let Some(rest) = input.strip_prefix("isdistinct.") {
        return Ok(Operation::IsDistinctFrom(CompactString::from(rest)));
    }

    // FTS operators
    if let Some(rest) = input.strip_prefix("fts") {
        let (lang, val) = parse_fts_args(rest)?;
        return Ok(Operation::Fts(FtsOperator::Fts, lang, val));
    }
    if let Some(rest) = input.strip_prefix("plfts") {
        let (lang, val) = parse_fts_args(rest)?;
        return Ok(Operation::Fts(FtsOperator::FtsPlain, lang, val));
    }
    if let Some(rest) = input.strip_prefix("phfts") {
        let (lang, val) = parse_fts_args(rest)?;
        return Ok(Operation::Fts(FtsOperator::FtsPhrase, lang, val));
    }
    if let Some(rest) = input.strip_prefix("wfts") {
        let (lang, val) = parse_fts_args(rest)?;
        return Ok(Operation::Fts(FtsOperator::FtsWebsearch, lang, val));
    }

    // Simple operators (must try before quant operators to avoid ambiguity)
    let simple_ops = [
        ("neq.", SimpleOperator::NotEqual),
        ("cs.", SimpleOperator::Contains),
        ("cd.", SimpleOperator::Contained),
        ("ov.", SimpleOperator::Overlap),
        ("sl.", SimpleOperator::StrictlyLeft),
        ("sr.", SimpleOperator::StrictlyRight),
        ("nxr.", SimpleOperator::NotExtendsRight),
        ("nxl.", SimpleOperator::NotExtendsLeft),
        ("adj.", SimpleOperator::Adjacent),
    ];

    for (prefix, op) in &simple_ops {
        if let Some(rest) = input.strip_prefix(prefix) {
            return Ok(Operation::Simple(*op, CompactString::from(rest)));
        }
    }

    // Quant operators (with optional quantifier)
    let quant_ops = [
        ("eq", QuantOperator::Equal),
        ("gte", QuantOperator::GreaterThanEqual),
        ("gt", QuantOperator::GreaterThan),
        ("lte", QuantOperator::LessThanEqual),
        ("lt", QuantOperator::LessThan),
        ("like", QuantOperator::Like),
        ("ilike", QuantOperator::ILike),
        ("match", QuantOperator::Match),
        ("imatch", QuantOperator::IMatch),
    ];

    for (prefix, op) in &quant_ops {
        if let Some(rest) = input.strip_prefix(prefix) {
            // Check for quantifier: (any) or (all)
            let (quant, rest) = if let Some(after_any) = rest.strip_prefix("(any)") {
                (Some(OpQuantifier::Any), after_any)
            } else if let Some(after_all) = rest.strip_prefix("(all)") {
                (Some(OpQuantifier::All), after_all)
            } else {
                (None, rest)
            };

            if let Some(val) = rest.strip_prefix('.') {
                return Ok(Operation::Quant(*op, quant, CompactString::from(val)));
            }
        }
    }

    Err(Error::ParseError {
        location: "filter".to_string(),
        message: format!("unknown operator in '{}'", input),
    })
}

fn parse_is_val(input: &str) -> Result<IsValue, Error> {
    let lower = input.to_lowercase();
    match lower.as_str() {
        "null" => Ok(IsValue::Null),
        "not_null" => Ok(IsValue::NotNull),
        "true" => Ok(IsValue::True),
        "false" => Ok(IsValue::False),
        "unknown" => Ok(IsValue::Unknown),
        _ => Err(Error::ParseError {
            location: "is value".to_string(),
            message: format!(
                "expected null, not_null, true, false, or unknown, got '{}'",
                input
            ),
        }),
    }
}

fn parse_fts_args(input: &str) -> Result<(Option<Language>, SingleVal), Error> {
    // fts(lang).value or fts.value
    if input.starts_with('(') {
        if let Some(close) = input.find(')') {
            let lang = &input[1..close];
            let rest = &input[close + 1..];
            if let Some(val) = rest.strip_prefix('.') {
                return Ok((Some(CompactString::from(lang)), CompactString::from(val)));
            }
        }
        return Err(Error::ParseError {
            location: "fts".to_string(),
            message: "malformed FTS expression".to_string(),
        });
    }

    if let Some(val) = input.strip_prefix('.') {
        Ok((None, CompactString::from(val)))
    } else {
        Err(Error::ParseError {
            location: "fts".to_string(),
            message: format!("expected '.' after FTS operator, got '{}'", input),
        })
    }
}

fn parse_list_val(input: &str) -> Result<ListVal, Error> {
    // Expect (val1,val2,val3)
    let input = input.trim();
    if !input.starts_with('(') {
        return Err(Error::ParseError {
            location: "list value".to_string(),
            message: "expected '(' for in list".to_string(),
        });
    }

    let inner = &input[1..];
    let close = find_matching_paren(inner).ok_or_else(|| Error::ParseError {
        location: "list value".to_string(),
        message: "unclosed '(' in list".to_string(),
    })?;

    let content = &inner[..close];
    let values = split_list_elements(content);

    Ok(values
        .into_iter()
        .map(|s| CompactString::from(s.as_str()))
        .collect())
}

fn find_matching_paren(input: &str) -> Option<usize> {
    let mut depth = 0;
    let mut in_quote = false;
    for (i, ch) in input.chars().enumerate() {
        if ch == '"' {
            in_quote = !in_quote;
        } else if !in_quote {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                if depth == 0 {
                    return Some(i);
                }
                depth -= 1;
            }
        }
    }
    None
}

fn split_list_elements(input: &str) -> Vec<String> {
    let mut elements = Vec::new();
    let mut current = String::new();
    let chars = input.chars();
    let mut escape_next = false;
    let mut in_quote = false;

    for ch in chars {
        if escape_next {
            current.push(ch);
            escape_next = false;
            continue;
        }

        if in_quote {
            if ch == '\\' {
                escape_next = true;
            } else if ch == '"' {
                in_quote = false;
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quote = true;
        } else if ch == ',' {
            elements.push(current.clone());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    elements.push(current);
    elements
}

// ==========================================================================
// Order parser
// ==========================================================================

/// Parse the `order=` parameter value.
pub fn parse_order(input: &str) -> Result<Vec<OrderTerm>, Error> {
    let mut terms = Vec::new();

    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        terms.push(parse_order_term(part)?);
    }

    Ok(terms)
}

fn parse_order_term(input: &str) -> Result<OrderTerm, Error> {
    // Check for relation order: relation(field).dir.nulls
    if let Some(paren_pos) = input.find('(')
        && let Some(close_pos) = input.find(')')
    {
        let relation = CompactString::from(input[..paren_pos].trim());
        let field_str = &input[paren_pos + 1..close_pos];
        let (name, rest_in_paren) = parse_field_name(field_str)?;
        let (json_path, _) = parse_json_path(rest_in_paren);

        let after_paren = &input[close_pos + 1..];
        let (direction, nulls) = parse_order_modifiers(after_paren);

        return Ok(OrderTerm::RelationTerm {
            relation,
            field: (name, json_path),
            direction,
            nulls,
        });
    }

    // Regular order: field.dir.nulls
    let (name, rest) = parse_field_name(input)?;
    let (json_path, rest) = parse_json_path(rest);
    let (direction, nulls) = parse_order_modifiers(rest);

    Ok(OrderTerm::Term {
        field: (name, json_path),
        direction,
        nulls,
    })
}

fn parse_order_modifiers(input: &str) -> (Option<OrderDirection>, Option<OrderNulls>) {
    let mut direction = None;
    let mut nulls = None;
    let mut rest = input;

    // Parse direction
    if let Some(r) = rest.strip_prefix(".asc") {
        if !r.starts_with(|c: char| c.is_alphanumeric()) {
            direction = Some(OrderDirection::Asc);
            rest = r;
        }
    } else if let Some(r) = rest.strip_prefix(".desc")
        && !r.starts_with(|c: char| c.is_alphanumeric())
    {
        direction = Some(OrderDirection::Desc);
        rest = r;
    }

    // Parse nulls
    if let Some(r) = rest.strip_prefix(".nullsfirst") {
        if !r.starts_with(|c: char| c.is_alphanumeric()) {
            nulls = Some(OrderNulls::First);
        }
    } else if let Some(r) = rest.strip_prefix(".nullslast")
        && !r.starts_with(|c: char| c.is_alphanumeric())
    {
        nulls = Some(OrderNulls::Last);
    }

    (direction, nulls)
}

// ==========================================================================
// Logic tree parser
// ==========================================================================

/// Parse a logic tree expression like "and(col.eq.1,col.gt.5)" or
/// "or(col.eq.1,not.and(a.eq.1,b.eq.2))"
pub fn parse_logic_tree(input: &str) -> Result<LogicTree, Error> {
    let input = input.trim();

    // Check for negation
    let (negated, rest) = if let Some(after) = input.strip_prefix("not.") {
        (true, after)
    } else {
        (false, input)
    };

    // Check for logic operator
    let (operator, rest) = if let Some(rest) = rest.strip_prefix("and") {
        (Some(LogicOperator::And), rest)
    } else if let Some(rest) = rest.strip_prefix("or") {
        (Some(LogicOperator::Or), rest)
    } else {
        (None, rest)
    };

    match operator {
        Some(op) => {
            // Must have parentheses
            let rest = rest.trim();
            if !rest.starts_with('(') {
                return Err(Error::ParseError {
                    location: "logic tree".to_string(),
                    message: format!("expected '(' after logic operator, got '{}'", rest),
                });
            }

            let inner = &rest[1..];
            let close = find_matching_paren(inner).ok_or_else(|| Error::ParseError {
                location: "logic tree".to_string(),
                message: "unclosed '(' in logic tree".to_string(),
            })?;

            let content = &inner[..close];
            let children = parse_logic_children(content)?;

            Ok(LogicTree::Expr {
                negated,
                operator: op,
                children,
            })
        }
        None => {
            // Must be a filter statement
            let filter = parse_logic_filter(rest)?;
            Ok(LogicTree::Stmnt(filter))
        }
    }
}

fn parse_logic_children(input: &str) -> Result<Vec<LogicTree>, Error> {
    let parts = split_logic_args(input);
    let mut children = Vec::new();

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        children.push(parse_logic_tree(part)?);
    }

    if children.is_empty() {
        return Err(Error::ParseError {
            location: "logic tree".to_string(),
            message: "empty logic expression".to_string(),
        });
    }

    Ok(children)
}

fn split_logic_args(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&input[start..i]);
                start = i + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&input[start..]);
    parts
}

fn parse_logic_filter(input: &str) -> Result<Filter, Error> {
    // field.op.value
    let (name, rest) = parse_field_name(input)?;
    let (json_path, rest) = parse_json_path(rest);

    let rest = rest.strip_prefix('.').ok_or_else(|| Error::ParseError {
        location: "logic filter".to_string(),
        message: format!("expected '.' after field name '{}'", name),
    })?;

    let op_expr = try_parse_op_expr(rest)?;

    Ok(Filter {
        field: (name, json_path),
        op_expr,
    })
}

// ==========================================================================
// Tree path parser
// ==========================================================================

/// Parse a dot-separated tree path with a final field name.
///
/// E.g., "clients.projects.name" -> (["clients", "projects"], ("name", []))
fn parse_tree_path(key: &str) -> Result<(EmbedPath, Field), Error> {
    let parts: Vec<&str> = key.split('.').collect();
    if parts.is_empty() {
        return Err(Error::ParseError {
            location: "tree path".to_string(),
            message: "empty path".to_string(),
        });
    }

    let path: Vec<FieldName> = parts[..parts.len() - 1]
        .iter()
        .map(|s| CompactString::from(*s))
        .collect();

    let last = *parts.last().unwrap();
    let (name, rest) = parse_field_name(last)?;
    let (json_path, _) = parse_json_path(rest);

    Ok((path, (name, json_path)))
}

/// Parse a logic path: "clients.projects.and" -> (["clients", "projects"], "and")
fn parse_logic_path(key: &str) -> Result<(EmbedPath, String), Error> {
    let parts: Vec<&str> = key.split('.').collect();
    if parts.is_empty() {
        return Err(Error::ParseError {
            location: "logic path".to_string(),
            message: "empty path".to_string(),
        });
    }

    let op = parts.last().unwrap().to_string();
    let path: Vec<FieldName> = parts[..parts.len() - 1]
        .iter()
        .map(|s| CompactString::from(*s))
        .collect();

    Ok((path, op))
}

// ==========================================================================
// Columns parser
// ==========================================================================

fn parse_columns(input: &str) -> Result<HashSet<FieldName>, Error> {
    Ok(parse_columns_list(input)?.into_iter().collect())
}

fn parse_columns_list(input: &str) -> Result<Vec<FieldName>, Error> {
    Ok(input
        .split(',')
        .map(|s| CompactString::from(s.trim()))
        .filter(|s| !s.is_empty())
        .collect())
}

// ==========================================================================
// Helpers
// ==========================================================================

fn replace_last_segment(key: &str, _replacement: &str) -> String {
    let parts: Vec<&str> = key.split('.').collect();
    if parts.len() <= 1 {
        return "limit".to_string();
    }
    let mut result: Vec<&str> = parts[..parts.len() - 1].to_vec();
    result.push("limit");
    result.join(".")
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- Select parser tests ----------

    #[test]
    fn test_parse_select_star() {
        let items = parse_select("*").unwrap();
        assert_eq!(items.len(), 1);
        assert!(matches!(&items[0], SelectItem::Field { field, .. } if field.0 == "*"));
    }

    #[test]
    fn test_parse_select_simple_fields() {
        let items = parse_select("id,name").unwrap();
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], SelectItem::Field { field, .. } if field.0 == "id"));
        assert!(matches!(&items[1], SelectItem::Field { field, .. } if field.0 == "name"));
    }

    #[test]
    fn test_parse_select_with_alias() {
        let items = parse_select("my_id:id,my_name:name").unwrap();
        assert_eq!(items.len(), 2);
        if let SelectItem::Field { alias, field, .. } = &items[0] {
            assert_eq!(alias.as_deref(), Some("my_id"));
            assert_eq!(field.0.as_str(), "id");
        }
    }

    #[test]
    fn test_parse_select_with_cast() {
        let items = parse_select("id::text").unwrap();
        assert_eq!(items.len(), 1);
        if let SelectItem::Field { cast, .. } = &items[0] {
            assert_eq!(cast.as_deref(), Some("text"));
        }
    }

    #[test]
    fn test_parse_select_with_json_path() {
        let items = parse_select("data->key->>value").unwrap();
        assert_eq!(items.len(), 1);
        if let SelectItem::Field { field, .. } = &items[0] {
            assert_eq!(field.0.as_str(), "data");
            assert_eq!(field.1.len(), 2);
            assert!(matches!(&field.1[0], JsonOperation::Arrow(JsonOperand::Key(k)) if k == "key"));
            assert!(
                matches!(&field.1[1], JsonOperation::Arrow2(JsonOperand::Key(k)) if k == "value")
            );
        }
    }

    #[test]
    fn test_parse_select_with_aggregate() {
        let items = parse_select("amount.sum()").unwrap();
        assert_eq!(items.len(), 1);
        if let SelectItem::Field {
            field, aggregate, ..
        } = &items[0]
        {
            assert_eq!(field.0.as_str(), "amount");
            assert_eq!(*aggregate, Some(AggregateFunction::Sum));
        }
    }

    #[test]
    fn test_parse_select_count() {
        let items = parse_select("count()").unwrap();
        assert_eq!(items.len(), 1);
        if let SelectItem::Field {
            field, aggregate, ..
        } = &items[0]
        {
            assert_eq!(field.0.as_str(), "*");
            assert_eq!(*aggregate, Some(AggregateFunction::Count));
        }
    }

    #[test]
    fn test_parse_select_relation() {
        let items = parse_select("posts(id,title)").unwrap();
        assert_eq!(items.len(), 1);
        if let SelectItem::Relation {
            relation, children, ..
        } = &items[0]
        {
            assert_eq!(relation.as_str(), "posts");
            assert_eq!(children.len(), 2);
        }
    }

    #[test]
    fn test_parse_select_relation_with_alias_and_hint() {
        let items = parse_select("my_posts:posts!fk_author(*)").unwrap();
        assert_eq!(items.len(), 1);
        if let SelectItem::Relation {
            relation,
            alias,
            hint,
            children,
            ..
        } = &items[0]
        {
            assert_eq!(relation.as_str(), "posts");
            assert_eq!(alias.as_deref(), Some("my_posts"));
            assert_eq!(hint.as_deref(), Some("fk_author"));
            assert_eq!(children.len(), 1);
        }
    }

    #[test]
    fn test_parse_select_relation_with_join_type() {
        let items = parse_select("posts!inner(*)").unwrap();
        assert_eq!(items.len(), 1);
        if let SelectItem::Relation {
            relation,
            join_type,
            ..
        } = &items[0]
        {
            assert_eq!(relation.as_str(), "posts");
            assert_eq!(*join_type, Some(JoinType::Inner));
        }
    }

    #[test]
    fn test_parse_select_spread() {
        let items = parse_select("...details(*)").unwrap();
        assert_eq!(items.len(), 1);
        if let SelectItem::Spread {
            relation, children, ..
        } = &items[0]
        {
            assert_eq!(relation.as_str(), "details");
            assert_eq!(children.len(), 1);
        }
    }

    #[test]
    fn test_parse_select_nested() {
        let items = parse_select("*,clients(*,projects(*))").unwrap();
        assert_eq!(items.len(), 2);
        if let SelectItem::Relation { children, .. } = &items[1] {
            assert_eq!(children.len(), 2);
            assert!(
                matches!(&children[1], SelectItem::Relation { relation, .. } if relation == "projects")
            );
        }
    }

    #[test]
    fn test_parse_select_empty() {
        let items = parse_select("").unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_select_complex() {
        let items =
            parse_select("alias:name->key::cast,posts!hint!inner(id,author(*)),...spread(*)")
                .unwrap();
        assert_eq!(items.len(), 3);
    }

    // ---------- Filter parser tests ----------

    #[test]
    fn test_parse_filter_eq() {
        let expr = try_parse_op_expr("eq.5").unwrap();
        assert_eq!(
            expr,
            OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "5".into())
            }
        );
    }

    #[test]
    fn test_parse_filter_not_eq() {
        let expr = try_parse_op_expr("not.eq.5").unwrap();
        assert_eq!(
            expr,
            OpExpr::Expr {
                negated: true,
                operation: Operation::Quant(QuantOperator::Equal, None, "5".into())
            }
        );
    }

    #[test]
    fn test_parse_filter_neq() {
        let expr = try_parse_op_expr("neq.5").unwrap();
        assert_eq!(
            expr,
            OpExpr::Expr {
                negated: false,
                operation: Operation::Simple(SimpleOperator::NotEqual, "5".into())
            }
        );
    }

    #[test]
    fn test_parse_filter_gt_lt() {
        let gt = try_parse_op_expr("gt.10").unwrap();
        assert!(matches!(
            gt,
            OpExpr::Expr {
                operation: Operation::Quant(QuantOperator::GreaterThan, None, _),
                ..
            }
        ));

        let lt = try_parse_op_expr("lt.5").unwrap();
        assert!(matches!(
            lt,
            OpExpr::Expr {
                operation: Operation::Quant(QuantOperator::LessThan, None, _),
                ..
            }
        ));
    }

    #[test]
    fn test_parse_filter_in() {
        let expr = try_parse_op_expr("in.(1,2,3)").unwrap();
        if let OpExpr::Expr {
            operation: Operation::In(vals),
            ..
        } = &expr
        {
            assert_eq!(vals.len(), 3);
            assert_eq!(vals[0].as_str(), "1");
            assert_eq!(vals[1].as_str(), "2");
            assert_eq!(vals[2].as_str(), "3");
        } else {
            panic!("expected In operation");
        }
    }

    #[test]
    fn test_parse_filter_is_null() {
        let expr = try_parse_op_expr("is.null").unwrap();
        assert_eq!(
            expr,
            OpExpr::Expr {
                negated: false,
                operation: Operation::Is(IsValue::Null)
            }
        );
    }

    #[test]
    fn test_parse_filter_is_true() {
        let expr = try_parse_op_expr("is.true").unwrap();
        assert_eq!(
            expr,
            OpExpr::Expr {
                negated: false,
                operation: Operation::Is(IsValue::True)
            }
        );
    }

    #[test]
    fn test_parse_filter_fts() {
        let expr = try_parse_op_expr("fts.search_term").unwrap();
        if let OpExpr::Expr {
            operation: Operation::Fts(op, lang, val),
            ..
        } = &expr
        {
            assert_eq!(*op, FtsOperator::Fts);
            assert!(lang.is_none());
            assert_eq!(val.as_str(), "search_term");
        }
    }

    #[test]
    fn test_parse_filter_fts_with_lang() {
        let expr = try_parse_op_expr("fts(english).search_term").unwrap();
        if let OpExpr::Expr {
            operation: Operation::Fts(_, lang, _),
            ..
        } = &expr
        {
            assert_eq!(lang.as_deref(), Some("english"));
        }
    }

    #[test]
    fn test_parse_filter_like() {
        let expr = try_parse_op_expr("like.*john*").unwrap();
        assert!(matches!(
            expr,
            OpExpr::Expr {
                operation: Operation::Quant(QuantOperator::Like, None, _),
                ..
            }
        ));
    }

    #[test]
    fn test_parse_filter_quant_any() {
        let expr = try_parse_op_expr("eq(any).5").unwrap();
        assert_eq!(
            expr,
            OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(
                    QuantOperator::Equal,
                    Some(OpQuantifier::Any),
                    "5".into()
                )
            }
        );
    }

    #[test]
    fn test_parse_filter_quant_all() {
        let expr = try_parse_op_expr("eq(all).5").unwrap();
        assert_eq!(
            expr,
            OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(
                    QuantOperator::Equal,
                    Some(OpQuantifier::All),
                    "5".into()
                )
            }
        );
    }

    #[test]
    fn test_parse_filter_isdistinct() {
        let expr = try_parse_op_expr("isdistinct.value").unwrap();
        assert_eq!(
            expr,
            OpExpr::Expr {
                negated: false,
                operation: Operation::IsDistinctFrom("value".into())
            }
        );
    }

    #[test]
    fn test_parse_filter_cs_cd() {
        let cs = try_parse_op_expr("cs.{1,2,3}").unwrap();
        assert!(matches!(
            cs,
            OpExpr::Expr {
                operation: Operation::Simple(SimpleOperator::Contains, _),
                ..
            }
        ));

        let cd = try_parse_op_expr("cd.{1,2}").unwrap();
        assert!(matches!(
            cd,
            OpExpr::Expr {
                operation: Operation::Simple(SimpleOperator::Contained, _),
                ..
            }
        ));
    }

    #[test]
    fn test_parse_filter_rpc_no_op() {
        let expr = parse_filter_value("plain_value", true).unwrap();
        assert_eq!(expr, OpExpr::NoOp("plain_value".into()));
    }

    #[test]
    fn test_parse_filter_rpc_with_op() {
        let expr = parse_filter_value("eq.5", true).unwrap();
        assert!(matches!(expr, OpExpr::Expr { .. }));
    }

    #[test]
    fn test_parse_filter_non_rpc_requires_op() {
        let result = parse_filter_value("plain_value", false);
        assert!(result.is_err());
    }

    // ---------- Order parser tests ----------

    #[test]
    fn test_parse_order_simple() {
        let terms = parse_order("name").unwrap();
        assert_eq!(terms.len(), 1);
        if let OrderTerm::Term {
            field,
            direction,
            nulls,
        } = &terms[0]
        {
            assert_eq!(field.0.as_str(), "name");
            assert!(direction.is_none());
            assert!(nulls.is_none());
        }
    }

    #[test]
    fn test_parse_order_desc_nullsfirst() {
        let terms = parse_order("name.desc.nullsfirst").unwrap();
        assert_eq!(terms.len(), 1);
        if let OrderTerm::Term {
            direction, nulls, ..
        } = &terms[0]
        {
            assert_eq!(*direction, Some(OrderDirection::Desc));
            assert_eq!(*nulls, Some(OrderNulls::First));
        }
    }

    #[test]
    fn test_parse_order_multiple() {
        let terms = parse_order("name.asc,id.desc").unwrap();
        assert_eq!(terms.len(), 2);
    }

    #[test]
    fn test_parse_order_json() {
        let terms = parse_order("json_col->key.asc.nullslast").unwrap();
        assert_eq!(terms.len(), 1);
        if let OrderTerm::Term { field, .. } = &terms[0] {
            assert_eq!(field.0.as_str(), "json_col");
            assert_eq!(field.1.len(), 1);
        }
    }

    #[test]
    fn test_parse_order_relation() {
        let terms = parse_order("clients(name).desc.nullsfirst").unwrap();
        assert_eq!(terms.len(), 1);
        assert!(
            matches!(&terms[0], OrderTerm::RelationTerm { relation, .. } if relation == "clients")
        );
    }

    // ---------- Logic tree tests ----------

    #[test]
    fn test_parse_logic_tree_simple() {
        let tree = parse_logic_tree("and(name.eq.John,age.gt.18)").unwrap();
        if let LogicTree::Expr {
            negated,
            operator,
            children,
        } = &tree
        {
            assert!(!negated);
            assert_eq!(*operator, LogicOperator::And);
            assert_eq!(children.len(), 2);
        }
    }

    #[test]
    fn test_parse_logic_tree_or() {
        let tree = parse_logic_tree("or(id.eq.1,id.eq.2)").unwrap();
        if let LogicTree::Expr { operator, .. } = &tree {
            assert_eq!(*operator, LogicOperator::Or);
        }
    }

    #[test]
    fn test_parse_logic_tree_nested() {
        let tree = parse_logic_tree("and(name.eq.John,or(id.eq.1,id.eq.2))").unwrap();
        if let LogicTree::Expr { children, .. } = &tree {
            assert_eq!(children.len(), 2);
            assert!(matches!(
                &children[1],
                LogicTree::Expr {
                    operator: LogicOperator::Or,
                    ..
                }
            ));
        }
    }

    #[test]
    fn test_parse_logic_tree_negated() {
        let tree = parse_logic_tree("not.and(a.eq.1,b.eq.2)").unwrap();
        if let LogicTree::Expr { negated, .. } = &tree {
            assert!(negated);
        }
    }

    // ---------- Full parse tests ----------

    #[test]
    fn test_parse_full_query() {
        let qp = parse(false, "select=id,name&id=eq.1&order=name.asc").unwrap();
        assert_eq!(qp.select.len(), 2);
        assert_eq!(qp.filters_root.len(), 1);
        assert_eq!(qp.order.len(), 1);
    }

    #[test]
    fn test_parse_with_limit_offset() {
        let qp = parse(false, "select=*&limit=25&offset=50").unwrap();
        let range = qp.ranges.get("limit").unwrap();
        assert_eq!(range.offset, 50);
        assert_eq!(range.limit(), Some(25));
    }

    #[test]
    fn test_parse_canonical() {
        let qp = parse(false, "b=eq.2&a=eq.1").unwrap();
        // Canonical should be sorted
        assert!(qp.canonical.starts_with("a="));
    }

    #[test]
    fn test_parse_columns() {
        let qp = parse(false, "select=*&columns=id,name").unwrap();
        let cols = qp.columns.as_ref().unwrap();
        assert!(cols.contains("id"));
        assert!(cols.contains("name"));
    }

    #[test]
    fn test_parse_on_conflict() {
        let qp = parse(false, "select=*&on_conflict=id,email").unwrap();
        let oc = qp.on_conflict.as_ref().unwrap();
        assert_eq!(oc.len(), 2);
    }

    #[test]
    fn test_parse_rpc_params() {
        let qp = parse(true, "id=5&name=john").unwrap();
        assert_eq!(qp.params.len(), 2);
    }

    #[test]
    fn test_parse_embedded_filter() {
        let qp = parse(false, "select=*,posts(*)&posts.status=eq.published").unwrap();
        assert_eq!(qp.filters_not_root.len(), 1);
        assert_eq!(qp.filters_not_root[0].0, vec![CompactString::from("posts")]);
    }

    #[test]
    fn test_parse_embedded_order() {
        let qp = parse(false, "select=*,posts(*)&posts.order=created_at.desc").unwrap();
        assert_eq!(qp.order.len(), 1);
        assert_eq!(qp.order[0].0, vec![CompactString::from("posts")]);
    }

    #[test]
    fn test_parse_logic() {
        let qp = parse(false, "select=*&or=(id.eq.1,id.eq.2)").unwrap();
        assert_eq!(qp.logic.len(), 1);
    }

    #[test]
    fn test_parse_default_select() {
        let qp = parse(false, "id=eq.1").unwrap();
        assert_eq!(qp.select.len(), 1);
        assert!(matches!(&qp.select[0], SelectItem::Field { field, .. } if field.0 == "*"));
    }

    #[test]
    fn test_parse_all_simple_operators() {
        for op in ["neq", "cs", "cd", "ov", "sl", "sr", "nxr", "nxl", "adj"] {
            let expr = try_parse_op_expr(&format!("{}.value", op)).unwrap();
            assert!(
                matches!(
                    expr,
                    OpExpr::Expr {
                        operation: Operation::Simple(..),
                        ..
                    }
                ),
                "operator {} should parse as Simple",
                op
            );
        }
    }

    #[test]
    fn test_parse_all_quant_operators() {
        for op in [
            "eq", "gte", "gt", "lte", "lt", "like", "ilike", "match", "imatch",
        ] {
            let expr = try_parse_op_expr(&format!("{}.value", op)).unwrap();
            assert!(
                matches!(
                    expr,
                    OpExpr::Expr {
                        operation: Operation::Quant(..),
                        ..
                    }
                ),
                "operator {} should parse as Quant",
                op
            );
        }
    }

    #[test]
    fn test_parse_all_fts_operators() {
        for op in ["fts", "plfts", "phfts", "wfts"] {
            let expr = try_parse_op_expr(&format!("{}.term", op)).unwrap();
            assert!(
                matches!(
                    expr,
                    OpExpr::Expr {
                        operation: Operation::Fts(..),
                        ..
                    }
                ),
                "operator {} should parse as Fts",
                op
            );
        }
    }

    #[test]
    fn test_parse_is_values() {
        for (val, expected) in [
            ("is.null", IsValue::Null),
            ("is.not_null", IsValue::NotNull),
            ("is.true", IsValue::True),
            ("is.false", IsValue::False),
            ("is.unknown", IsValue::Unknown),
        ] {
            let expr = try_parse_op_expr(val).unwrap();
            assert_eq!(
                expr,
                OpExpr::Expr {
                    negated: false,
                    operation: Operation::Is(expected)
                },
                "is value {} should parse",
                val
            );
        }
    }
}

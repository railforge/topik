use proc_macro2::Span;
use syn::{
    Attribute, DeriveInput, Error, Fields, Ident, LitStr, Path, Result, Type, parse::ParseStream,
    punctuated::Punctuated, token::Comma,
};

/// Intermediate representation of a `#[derive(Topic)]` input.
pub struct TopicInput {
    pub name: Ident,

    /// Ordered list of segments from `#[topic(segments(...))]`.
    pub segments: Vec<SegmentKind>,

    pub payload: PayloadField,

    /// Optional encoding path from `#[topic(encoding = ...)]`.
    /// None = use default (JsonEncoding when json feature is enabled).
    pub encoding: Option<Path>,

    /// All non-payload fields.
    pub fields: Vec<TopicField>,
}

/// A single entry in `segments(...)`.
pub enum SegmentKind {
    Literal(String),
    Dynamic(Ident),
}

/// The field marked `#[payload]`.
pub struct PayloadField {
    pub name: Ident,
    pub ty: Type,
}

/// A non-payload field.
pub struct TopicField {
    pub name: Ident,
    pub ty: Type,
}

pub fn parse_topic_input(input: DeriveInput) -> Result<TopicInput> {
    let name = input.ident.clone();

    // only named structs supported
    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(Error::new_spanned(
                    &name,
                    "Topic can only be derived on structs with named fields",
                ));
            }
        },
        _ => {
            return Err(Error::new_spanned(
                &name,
                "Topic can only be derived on structs, not enums or unions",
            ));
        }
    };

    let topic_attrs = parse_topic_attrs(&input.attrs, &name)?;

    let segments = topic_attrs
        .segments
        .ok_or_else(|| Error::new_spanned(&name, "missing #[topic(segments(...))] attribute"))?;

    let encoding = topic_attrs.encoding;

    let mut payload: Option<PayloadField> = None;
    let mut topic_fields: Vec<TopicField> = Vec::new();

    for field in fields {
        let field_name = field
            .ident
            .clone()
            .ok_or_else(|| Error::new_spanned(field, "Topic fields must be named"))?;

        match (has_payload_attr(&field.attrs), payload.is_some()) {
            (true, true) => {
                // see a payload field while one was already discovered
                return Err(Error::new_spanned(
                    field,
                    "only one field can be marked #[payload]",
                ));
            }
            (true, false) => {
                payload = Some(PayloadField {
                    name: field_name,
                    ty: field.ty.clone(),
                })
            }
            (false, _) => topic_fields.push(TopicField {
                name: field_name,
                ty: field.ty.clone(),
            }),
        }
    }

    let payload =
        payload.ok_or_else(|| Error::new_spanned(&name, "one field must be marked #[payload]"))?;

    // validate all dynamic segments refer to real fields
    for segment in &segments {
        if let SegmentKind::Dynamic(ident) = segment {
            let exists = topic_fields.iter().any(|f| f.name == *ident);
            if !exists {
                return Err(Error::new_spanned(
                    ident,
                    format!("segment '{}' does not match any field on the struct", ident),
                ));
            }
        }
    }

    Ok(TopicInput {
        name,
        segments,
        payload,
        encoding,
        fields: topic_fields,
    })
}

struct TopicAttrs {
    segments: Option<Vec<SegmentKind>>,
    encoding: Option<Path>,
}

fn parse_topic_attrs(attrs: &[Attribute], span: &Ident) -> Result<TopicAttrs> {
    let mut segments: Option<Vec<SegmentKind>> = None;
    let mut encoding: Option<Path> = None;

    for attr in attrs {
        if !attr.path().is_ident("topic") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("segments") {
                let content;
                syn::parenthesized!(content in meta.input);
                segments = Some(parse_segments(&content)?);
                Ok(())
            } else if meta.path.is_ident("encoding") {
                meta.input.parse::<syn::Token![=]>()?;
                encoding = Some(meta.input.parse::<Path>()?);
                Ok(())
            } else {
                Err(meta
                    .error("unknown topic attribute — expected segments(...) or encoding = ..."))
            }
        })?;
    }

    Ok(TopicAttrs { segments, encoding })
}

fn parse_segments(input: ParseStream) -> Result<Vec<SegmentKind>> {
    let mut segments = Vec::new();

    while !input.is_empty() {
        if input.peek(LitStr) {
            let lit: LitStr = input.parse()?;
            segments.push(SegmentKind::Literal(lit.value()));
        } else if input.peek(Ident) {
            let ident: Ident = input.parse()?;
            segments.push(SegmentKind::Dynamic(ident));
        } else {
            return Err(input.error("expected a string literal or field name in segments(...)"));
        }

        // consume comma if present
        if input.peek(syn::Token![,]) {
            input.parse::<syn::Token![,]>()?;
        }
    }

    if segments.is_empty() {
        return Err(input.error("segments(...) cannot be empty"));
    }

    Ok(segments)
}

fn has_payload_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|a| a.path().is_ident("payload"))
}

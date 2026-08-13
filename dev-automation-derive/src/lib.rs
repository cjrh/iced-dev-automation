use proc_macro::TokenStream;
use quote::{ToTokens, format_ident, quote};
use syn::{Data, DeriveInput, Fields, Ident, Type, Variant, parse_macro_input};

/// Generates a debug-only JSON dispatcher for explicitly annotated enum variants.
///
/// Supported variant attributes:
///
/// ```ignore
/// #[automation]
/// RefreshAll,
/// ```
#[proc_macro_derive(DevAutomation, attributes(automation))]
pub fn derive_dev_automation(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "DevAutomation does not support generic enums",
        ));
    }

    let Data::Enum(data) = input.data else {
        return Err(syn::Error::new_spanned(
            input.ident,
            "DevAutomation can only be derived for enums",
        ));
    };

    let enum_name = input.ident;
    let wire_name = format_ident!("__{}AutomationRequest", enum_name);
    let variants = data
        .variants
        .iter()
        .filter(|variant| has_automation_attribute(variant))
        .collect::<Vec<_>>();

    if variants.is_empty() {
        return Err(syn::Error::new_spanned(
            enum_name,
            "DevAutomation requires at least one #[automation] variant",
        ));
    }

    let wire_variants = variants
        .iter()
        .map(|variant| wire_variant(variant))
        .collect::<syn::Result<Vec<_>>>()?;
    let conversions = variants
        .iter()
        .map(|variant| conversion(variant, &enum_name))
        .collect::<syn::Result<Vec<_>>>()?;
    let schema_variants = variants
        .iter()
        .map(|variant| schema_variant(variant))
        .collect::<syn::Result<Vec<_>>>()?;

    Ok(quote! {
        #[allow(non_camel_case_types)]
        #[derive(::iced_dev_automation::serde::Deserialize)]
        #[serde(
            crate = "::iced_dev_automation::serde",
            tag = "variant",
            content = "value",
            deny_unknown_fields
        )]
        enum #wire_name {
            #(#wire_variants),*
        }

        impl ::iced_dev_automation::DevAutomation for #enum_name {
            fn from_automation_value(
                value: ::iced_dev_automation::serde_json::Value,
            ) -> ::std::result::Result<Self, ::iced_dev_automation::serde_json::Error> {
                let request: #wire_name =
                    ::iced_dev_automation::serde_json::from_value(value)?;

                Ok(match request {
                    #(#conversions),*
                })
            }

            fn automation_schema() -> ::iced_dev_automation::serde_json::Value {
                ::iced_dev_automation::serde_json::json!({
                    "protocol": "iced-dev-automation",
                    "version": 1,
                    "tag": "variant",
                    "content": "value",
                    "message": ::std::stringify!(#enum_name),
                    "variants": [#(#schema_variants),*],
                })
            }
        }
    })
}

fn has_automation_attribute(variant: &Variant) -> bool {
    variant
        .attrs
        .iter()
        .any(|attribute| attribute.path().is_ident("automation"))
}

fn wire_variant(variant: &Variant) -> syn::Result<proc_macro2::TokenStream> {
    let name = &variant.ident;

    match &variant.fields {
        Fields::Unit => Ok(quote!(#name)),
        Fields::Unnamed(fields) => {
            let types = fields.unnamed.iter().map(|field| &field.ty);
            Ok(quote!(#name(#(#types),*)))
        }
        Fields::Named(fields) => {
            let fields = fields.named.iter().map(|field| {
                let name = field
                    .ident
                    .as_ref()
                    .expect("named fields have an identifier");
                let ty = &field.ty;
                quote!(#name: #ty)
            });
            Ok(quote!(#name { #(#fields),* }))
        }
    }
}

fn conversion(variant: &Variant, enum_name: &Ident) -> syn::Result<proc_macro2::TokenStream> {
    let name = &variant.ident;
    let wire_name = format_ident!("__{}AutomationRequest", enum_name);

    match &variant.fields {
        Fields::Unit => Ok(quote!(#wire_name::#name => #enum_name::#name)),
        Fields::Unnamed(fields) => {
            let bindings = indexed_bindings(fields.unnamed.len());
            Ok(quote!(
                #wire_name::#name(#(#bindings),*) => #enum_name::#name(#(#bindings),*)
            ))
        }
        Fields::Named(fields) => {
            let bindings = fields
                .named
                .iter()
                .map(|field| {
                    field
                        .ident
                        .as_ref()
                        .expect("named fields have an identifier")
                })
                .collect::<Vec<_>>();
            Ok(quote!(
                #wire_name::#name { #(#bindings),* } => #enum_name::#name { #(#bindings),* }
            ))
        }
    }
}

fn schema_variant(variant: &Variant) -> syn::Result<proc_macro2::TokenStream> {
    let name = variant.ident.to_string();

    match &variant.fields {
        Fields::Unit => Ok(quote!(::iced_dev_automation::serde_json::json!({
            "variant": #name,
            "shape": "unit",
        }))),
        Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
            let ty = type_name(&fields.unnamed[0].ty);
            Ok(quote!(::iced_dev_automation::serde_json::json!({
                "variant": #name,
                "shape": "newtype",
                "type": #ty,
            })))
        }
        Fields::Unnamed(fields) => {
            let field_schema = fields.unnamed.iter().enumerate().map(|(index, field)| {
                let ty = type_name(&field.ty);
                quote!(::iced_dev_automation::serde_json::json!({ "index": #index, "type": #ty }))
            });
            Ok(quote!(::iced_dev_automation::serde_json::json!({
                "variant": #name,
                "shape": "tuple",
                "fields": [#(#field_schema),*],
            })))
        }
        Fields::Named(fields) => {
            let field_schema = fields.named.iter().map(|field| {
                let name = field
                    .ident
                    .as_ref()
                    .expect("named fields have an identifier")
                    .to_string();
                let ty = type_name(&field.ty);
                quote!(::iced_dev_automation::serde_json::json!({ "name": #name, "type": #ty }))
            });
            Ok(quote!(::iced_dev_automation::serde_json::json!({
                "variant": #name,
                "shape": "struct",
                "fields": [#(#field_schema),*],
            })))
        }
    }
}

fn indexed_bindings(count: usize) -> Vec<Ident> {
    (0..count)
        .map(|index| format_ident!("field_{index}"))
        .collect()
}

fn type_name(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}

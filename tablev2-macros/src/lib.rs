use quote::quote;
use syn::ItemStruct;
use syn::{parse_quote, parse_str};
use syn::{Error, Field, Ident};

#[proc_macro_derive(Queryable, attributes(index))]
pub fn derive_queryable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive_queryable_inner(proc_macro2::TokenStream::from(input))
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn derive_queryable_inner(
    input: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let input: ItemStruct = syn::parse2(input).unwrap();

    // Check if generics exist
    let syn::Generics { params, .. } = &input.generics;
    if !params.empty_or_trailing() {
        return Err(syn::Error::new(
            input.ident.span(),
            "Deriving TableRow on generic types is not yet supported.",
        ));
    }

    println!("{:#?}", input);
    let name = &input.ident;
    let fields = &input.fields;

    let index_ident: Ident = parse_quote!(index);
    let has_index_attr = |field: &Field| {
        for attr in field.attrs.iter() {
            for segment in &attr.path.segments {
                if segment.ident == index_ident {
                    return true;
                }
            }
        }
        false
    };

    let index_fields = fields
        .iter()
        .filter(|field| has_index_attr(field))
        .collect::<Vec<&Field>>();
    // let range_fields =  fields
    // .iter()
    // .filter(|field| has_index_attr(field))
    // .collect::<Vec<&Field>>();

    let mut field_names = Vec::new();
    let mut field_ranges = Vec::new();
    let mut field_types = Vec::new();
    for field in index_fields {
        let field_name = field.ident.as_ref().ok_or(syn::Error::new(
            name.span(),
            "Only named fields are allowed.",
        ))?;
        let range_name: Ident = parse_str(&format!("{}_range", field_name)).unwrap();
        let field_ty = &field.ty;

        field_names.push(field_name);
        field_ranges.push(range_name);
        field_types.push(field_ty);
    }

    let table_name: Ident = parse_str(&format!("{}Table", name)).unwrap();

    let impl_queryable = quote!(
        impl tablev2::Queryable for #name {
            type Table<S: tablev2::Store<Self>> = #table_name<S>;
        }

        #[derive(Default)]
        struct #table_name<S> {
            store: S,

            #(#field_names :
              std::collections::BTreeMap<#field_types, std::collections::BTreeSet<usize>>,)*
        }


        impl<S> tablev2::TableTrait<#name, S> for #table_name<S>
        where
            S: tablev2::Store<#name>,
        {
            fn new() -> Self {
                Self {
                    store: S::new(),
                    name: std::collections::BTreeMap::new(),
                    age: std::collections::BTreeMap::new(),
                }
            }

            fn query(&self) -> tablev2::Query<#name, S> {
                tablev2::Query {
                    table: self,
                    matches: Vec::new(),
                    match_columns: std::collections::HashSet::new(),
                }
            }

            fn query_mut(&mut self) -> tablev2::QueryMut<#name, S> {
                tablev2::QueryMut::new(self)
            }

            fn insert(&mut self, value: #name) {
                let i = self.store.insert(value);
                self.add_to_index(i);
            }

            fn add_to_index(&mut self, i: usize) {
                if let Some(value) = self.store.get(i) {
                    #(
                        self.#field_names.entry(value.#field_names.clone()).or_default().insert(i);
                    )*
                }
            }

            fn remove_from_index(&mut self, i: usize) {
                if let Some(value) = self.store.get(i) {
                    #(
                        self.#field_names.get_mut(&value.#field_names).map(|map| map.remove(&i));
                    )*
                }
            }

            fn store(&self) -> &S {
                &self.store
            }

            fn store_mut(&mut self) -> &mut S {
                &mut self.store
            }

            // pub fn remove<Tname, Tage, Rname>(&mut self, query: &UserQuery<Tname, Tage, Rname>) {}
        }

        pub trait UserQuery {
            #(
                fn #field_names<T>(self, #field_names: &T) -> Self
                where
                    #field_types: std::borrow::Borrow<T> + Ord,
                    T: Ord + ?Sized;
            )*

            #(
                fn #field_ranges<T, R>(self, #field_ranges: R) -> Self
                where
                    #field_types: std::borrow::Borrow<T> + Ord,
                    T: Ord + ?Sized,
                    R: std::ops::RangeBounds<T>;
            )*
        }


        impl<'a, S: tablev2::Store<#name>> UserQuery for tablev2::Query<'a, #name, S> {
          #(
            fn #field_names<T>(mut self, #field_names: &T) -> Self
            where
                #field_types: std::borrow::Borrow<T> + Ord,
                T: Ord + ?Sized
            {
                self.match_columns.insert("#field_names");
                if let Some(matches) = self.table.#field_names.get(#field_names) {
                    self.matches.push(tablev2::QueryColumnMatches::Selected(matches));
                }
                self
            }

          )*

          #(
            fn #field_ranges<T, R>(mut self, #field_ranges: R) -> Self
            where
                #field_types: std::borrow::Borrow<T> + Ord,
                T: Ord + ?Sized,
                R: std::ops::RangeBounds<T>
            {
                self.match_columns.insert("#field_names");
                let matches: Vec<_> = self.table.#field_names.range(#field_ranges).map(|(_, m)| m).collect();
                if matches.len() > 0 {
                    self.matches.push(tablev2::QueryColumnMatches::Range(matches));
                }
                self
            }

          )*
        }


        pub trait UserQueryMut {
            #(
                fn #field_names<T>(self, #field_names: &T) -> Self
              where
                  #field_types: std::borrow::Borrow<T> + Ord,
                  T: Ord + ?Sized;
            )*

            #(
                fn #field_ranges<T, R>(self, #field_ranges: R) -> Self
                where
                    #field_types: std::borrow::Borrow<T> + Ord,
                    T: Ord + ?Sized,
                    R: std::ops::RangeBounds<T>;
            )*
        }

        impl<'a, S: tablev2::Store<#name>> UserQueryMut for tablev2::QueryMut<'a, #name, S> {
            #(
              fn #field_names<T>(mut self, #field_names: &T) -> Self
              where
                  #field_types: std::borrow::Borrow<T> + Ord,
                  T: Ord + ?Sized
              {
                    self.match_columns.insert(stringify!(#field_names));
                    if let Some(matches) = self.table.#field_names.get(#field_names) {
                        for m in matches.iter() {
                            let counter = self.match_counter.entry(*m).or_default();
                            *counter += 1;
                        }
                    }
                    self
              }

            )*

            #(
              fn #field_ranges<T, R>(mut self, #field_ranges: R) -> Self
              where
                  #field_types: std::borrow::Borrow<T> + Ord,
                  T: Ord + ?Sized,
                  R: std::ops::RangeBounds<T>
              {
                    self.match_columns.insert(stringify!(#field_names));
                    for matches in self.table.#field_names.range(#field_ranges).map(|(_, m)| m) {
                        for m in matches.iter() {
                            let counter = self.match_counter.entry(*m).or_default();
                            *counter += 1;
                        }
                    }
                    self
              }

            )*
          }
    );

    Ok(impl_queryable)
}

#[test]
fn test_derive_queryable() {
    let input = quote!(
        struct User {
            #[index]
            name: String,
            #[index(range)]
            age: u32,
        }
    );
    let output = derive_queryable_inner(input).unwrap().to_string();
    println!("{}", rustfmt(&output));
}

fn rustfmt(input: &str) -> String {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("rustfmt")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    let input = input.to_owned();
    std::thread::spawn(move || {
        stdin.write_all(input.as_bytes()).unwrap();
    });

    let output = child.wait_with_output().unwrap();
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[test]
fn test_rustfmt() {
    let output = rustfmt("fn main() {\n\n}");
    println!("{}", output);
}

struct A<T> {
    b: T,
}

// pub struct TableRange<T> {
//     start: Bound<
// }

#[test]
fn test_derive_queryable_errors() {
    let input = quote!(
        enum Test {
            A,
        }
    );
    assert!(derive_queryable_inner(input).is_err());
}

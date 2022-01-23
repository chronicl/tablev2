use quote::quote;
use syn::ItemStruct;
use syn::{parse_quote, parse_str};
use syn::{Error, Field, Ident};

#[proc_macro_derive(Queryable, attributes(index))]
pub fn derive_queryable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive_queryable_inner(proc_macro2::TokenStream::from(input), false)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(QueryableSerde, attributes(index))]
pub fn derive_queryable_serde(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive_queryable_inner(proc_macro2::TokenStream::from(input), true)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn derive_queryable_inner(
    input: proc_macro2::TokenStream,
    serde: bool,
) -> syn::Result<proc_macro2::TokenStream> {
    let input: ItemStruct = syn::parse2(input).unwrap();

    println!("{:#?}", input);

    let serde = if serde {
        quote!(#[derive(serde::Serialize, serde::Deserialize)])
    } else {
        quote!()
    };

    let vis = &input.vis;

    // Check if generics exist
    let syn::Generics { params, .. } = &input.generics;
    if !params.empty_or_trailing() {
        return Err(syn::Error::new(
            input.ident.span(),
            "Deriving TableRow on generic types is not yet supported.",
        ));
    }

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

    let index_name: Ident = parse_str(&format!("{}Index", name)).unwrap();
    let query_name: Ident = parse_str(&format!("{}Query", name)).unwrap();
    let query_mut_name: Ident = parse_str(&format!("{}QueryMut", name)).unwrap();

    let impl_queryable = quote!(
        impl tablev2::Queryable for #name {
            type Index = #index_name;
            type Query<'a, S: 'static> = #query_name<'a, S>;
            type QueryMut<'a, S: 'static> = #query_mut_name<'a, S>;
        }

        #serde
        #[derive(Default, Debug, Clone)]
        #vis struct #index_name {
            #(#field_names :
              std::collections::BTreeMap<#field_types, std::collections::BTreeSet<usize>>,)*
        }

        impl tablev2::Index<#name> for #index_name {
            fn new() -> Self {
                Self {
                    #(
                        #field_names: std::collections::BTreeMap::new(),
                    )*
                }
            }

            fn query<'a, S>(&'a self, store: &'a S) -> <#name as tablev2::Queryable>::Query<'a, S>
            where
                S: 'static,
            {
                #query_name {
                    index: self,
                    store,
                    matches: Vec::new(),
                    match_columns: std::collections::HashSet::new(),
                }
            }

            fn query_mut<'a, S>(&'a mut self, store: &'a mut S) -> <#name as tablev2::Queryable>::QueryMut<'a, S>
            where
                S: 'static,
            {
                #query_mut_name {
                    index: self,
                    store,
                    match_counter: std::collections::BTreeMap::new(),
                    match_columns: std::collections::HashSet::new(),
                }
            }

            fn add_to_index<S: tablev2::Store<#name>>(&mut self, store: &mut S, i: usize) {
                if let Some(value) = store.get(i) {
                    #(
                        self.#field_names.entry(value.#field_names.clone()).or_default().insert(i);
                    )*
                }
            }

            fn remove_from_index<S: tablev2::Store<#name>>(&mut self, store: &mut S, i: usize) {
                if let Some(value) = store.get(i) {
                    #(
                        self.#field_names.get_mut(&value.#field_names).map(|map| map.remove(&i));
                    )*
                }
            }
        }

        #vis struct #query_name<'a, S> {
            pub index: &'a <#name as tablev2::Queryable>::Index,
            pub store: &'a S,
            pub matches: Vec<tablev2::QueryColumnMatches<'a>>,
            pub match_columns: std::collections::HashSet<&'static str>,
        }

        impl<'a, S> tablev2::Query<#name, S> for #query_name<'a, S> {
            fn get<'b>(&'b self) -> tablev2::Matches<'b, #name, S> {
                let columns = &self.matches;
                let columns_len = self.match_columns.len() as u32;

                if columns.len() == 0 {
                    return tablev2::Matches {
                        index: self.index,
                        store: self.store,
                        strategy: tablev2::QueryStrategy::MatchAll { current: 0 },
                    };
                }

                if columns.len() < columns_len as usize {
                    return tablev2::Matches {
                        index: self.index,
                        store: self.store,
                        strategy: tablev2::QueryStrategy::NoMatches,
                    };
                }

                let mut matches = Vec::new();

                for column in columns.iter() {
                    match column {
                        tablev2::QueryColumnMatches::Selected(column) => matches.push(column.iter().peekable()),
                        tablev2::QueryColumnMatches::Range(column) => {
                            for c in column {
                                matches.push(c.iter().peekable())
                            }
                        }
                    }
                }

                let strategy = tablev2::QueryStrategy::MatchCounter {
                    match_counter: std::collections::BTreeMap::new(),
                    matches,
                    columns_len,
                };

                tablev2::Matches {
                    index: self.index,
                    store: self.store,
                    strategy,
                }
            }
        }

        #vis struct #query_mut_name<'a, S> {
            pub index: &'a mut <#name as tablev2::Queryable>::Index,
            pub store: &'a mut S,
            // Instead of keeping track of QueryColumnMatches like for a normal
            // Query, we create a match counter which is filled up immediately rather
            // than lazily during iteration.
            pub match_counter: std::collections::BTreeMap<usize, u32>,
            pub match_columns: std::collections::HashSet<&'static str>,
        }

        impl<'a, S> tablev2::QueryMut<#name, S> for #query_mut_name<'a, S> {
            fn get<'b>(&'b self) -> tablev2::Matches<'b, #name, S> {
                tablev2::Matches {
                    index: self.index,
                    store: self.store,
                    strategy: tablev2::QueryStrategy::PrefilledMatchCounter {
                        match_counter: self.match_counter.clone(),
                        columns_len: self.match_columns.len() as u32,
                    },
                }
            }

            fn update(&mut self, f: impl Fn(&mut #name))
            where
                S: tablev2::Store<#name>,
            {
                use tablev2::Index;

                let mut matches = Vec::new();
                let mut query_result: tablev2::Matches<_, _> = self.get();
                while let Some(i) = query_result.next_match() {
                    matches.push(i);
                }

                for i in matches {
                    // Removing value from index
                    self.index.remove_from_index(self.store, i);

                    // Modifying value
                    if let Some(m) = self.store.get_mut(i) {
                        f(m);
                    }

                    // Adding value back to the index
                    self.index.add_to_index(self.store, i);
                }
            }

            fn remove(&mut self)
            where
                S: tablev2::Store<#name>,
            {
                use tablev2::Index;

                let mut matches = Vec::new();
                let mut query_result: tablev2::Matches<_, _> = self.get();
                while let Some(i) = query_result.next_match() {
                    matches.push(i);
                }

                for i in matches {
                    self.index.remove_from_index(self.store, i);
                    self.store.remove(i);
                }
            }
        }


        // Filling in the query
        impl<'a, S> #query_name<'a, S> {
            #(
            fn #field_names<T>(mut self, #field_names: &T) -> Self
            where
                #field_types: std::borrow::Borrow<T> + Ord,
                T: Ord + ?Sized
            {
                self.match_columns.insert(stringify!(#field_names));
                if let Some(matches) = self.index.#field_names.get(#field_names) {
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
                self.match_columns.insert(stringify!(#field_names));
                let matches: Vec<_> = self.index.#field_names.range(#field_ranges).map(|(_, m)| m).collect();
                if matches.len() > 0 {
                    self.matches.push(tablev2::QueryColumnMatches::Range(matches));
                }
                self
            }

            )*
        }

        impl<'a, S> #query_mut_name<'a, S> {
            #(
            fn #field_names<T>(mut self, #field_names: &T) -> Self
            where
                #field_types: std::borrow::Borrow<T> + Ord,
                T: Ord + ?Sized
            {
                self.match_columns.insert(stringify!(#field_names));
                if let Some(matches) = self.index.#field_names.get(#field_names) {
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
                for matches in self.index.#field_names.range(#field_ranges).map(|(_, m)| m) {
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
        #[derive(Serialize)]
        pub struct User {
            #[index]
            name: String,
            #[index(range)]
            age: u32,
        }
    );
    let output = derive_queryable_inner(input, false).unwrap().to_string();
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
    assert!(derive_queryable_inner(input, false).is_err());
}

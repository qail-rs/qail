use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parse, parse::ParseStream, parse_macro_input, Expr, Ident, LitStr, Token};
use qail_core::transpiler::ToSql;

struct QailInput {
    pool: Ident,
    _comma1: Token![,],
    query: LitStr,
    args: Vec<Expr>,
}

impl Parse for QailInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let pool: Ident = input.parse()?;
        let _comma1: Token![,] = input.parse()?;
        let query: LitStr = input.parse()?;
        
        let mut args = Vec::new();
        while input.peek(Token![,]) {
            let _comma: Token![,] = input.parse()?;
            if input.is_empty() {
                break;
            }
            let arg: Expr = input.parse()?;
            args.push(arg);
        }

        Ok(QailInput {
            pool,
            _comma1,
            query,
            args,
        })
    }
}

#[proc_macro]
pub fn qail(input: TokenStream) -> TokenStream {
    let QailInput {
        pool,
        query,
        args,
        ..
    } = parse_macro_input!(input as QailInput);

    let query_str = query.value();

    // Parse QAIL at compile time
    let cmd = match qail_core::parse(&query_str) {
        Ok(cmd) => cmd,
        Err(e) => {
            return syn::Error::new(query.span(), format!("QAIL Parse Error: {}", e))
                .to_compile_error()
                .into();
        }
    };

    // Transpile to SQL
    let sql = cmd.to_sql();

    // Generate the sqlx::query! call
    // usage: sqlx::query!(sql, args...).fetch_all(&pool)
    let expand = quote! {
        sqlx::query!(#sql, #(#args),*)
            .fetch_all(&#pool)
    };

    TokenStream::from(expand)
}

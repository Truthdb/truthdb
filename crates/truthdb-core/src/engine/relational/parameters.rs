/// The parameter names of a declaration list (`@p1 int, @p2 nvarchar(10)`),
/// in order: the first token of each top-level comma-separated entry.
/// `sp_execute` values arrive unnamed on the wire; these names bind them.
pub(crate) fn decl_names(decls: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut depth = 0usize;
    let mut in_quote = false;
    let mut entry = String::new();
    for ch in decls.chars().chain(std::iter::once(',')) {
        match ch {
            // A quoted default value (`@p varchar(10) = 'a,b'`) may contain
            // commas and parens; none of them separate declarations. A
            // doubled '' escape toggles twice, landing back where it was.
            '\'' => {
                in_quote = !in_quote;
                entry.push(ch);
            }
            '(' if !in_quote => {
                depth += 1;
                entry.push(ch);
            }
            ')' if !in_quote => {
                depth = depth.saturating_sub(1);
                entry.push(ch);
            }
            ',' if !in_quote && depth == 0 => {
                if let Some(name) = entry.split_whitespace().next() {
                    names.push(name.to_string());
                }
                entry.clear();
            }
            _ => entry.push(ch),
        }
    }
    names
}

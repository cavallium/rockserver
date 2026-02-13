use std::sync::Arc;

#[derive(Debug)]
pub enum Query {
    And(Vec<Query>),
    Or(Vec<Query>),
    Text(String),
    ChatId(i64),
    SenderId(i64),
    CheckKeyMatchesValue,
}

pub struct MessageContext<'a> {
    pub text: Vec<&'a str>,
    pub chat_id: Option<i64>,
    pub sender_id: Option<i64>,
    pub key_chat_id: Option<i64>,
    pub key_message_id: Option<i64>,
    pub val_chat_id: Option<i64>,
    pub val_message_id: Option<i64>,
}

impl Query {
    pub fn matches(&self, ctx: &MessageContext) -> bool {
        match self {
            Query::And(subqueries) => {
                for q in subqueries {
                    if !q.matches(ctx) {
                        return false;
                    }
                }
                true
            }
            Query::Or(subqueries) => {
                for q in subqueries {
                    if q.matches(ctx) {
                        return true;
                    }
                }
                false
            }
            Query::Text(pattern) => {
                ctx.text.iter().any(|text| text.to_lowercase().contains(pattern))
            }
            Query::ChatId(id) => {
                ctx.chat_id == Some(*id)
            }
            Query::SenderId(id) => {
                ctx.sender_id == Some(*id)
            }
            Query::CheckKeyMatchesValue => {
                if let (Some(k_cid), Some(k_mid), Some(v_cid), Some(v_mid)) = (ctx.key_chat_id, ctx.key_message_id, ctx.val_chat_id, ctx.val_message_id) {
                    k_cid != v_cid || k_mid != v_mid
                } else {
                    false
                }
            }
        }
    }
}

pub fn parse_query(input: &str) -> Result<Query, String> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    LParen,
    RParen,
    And,
    Or,
    KeyOpValue(String, String), // key, value
    StringLit(String), // Fallback for plain text search or error
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut chars = input.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' | '\r' | '\n' => { chars.next(); }
            '(' => { tokens.push(Token::LParen); chars.next(); }
            ')' => { tokens.push(Token::RParen); chars.next(); }
            '&' => {
                chars.next();
                if let Some('&') = chars.peek() { chars.next(); }
                tokens.push(Token::And);
            }
            '|' => {
                chars.next();
                if let Some('|') = chars.peek() { chars.next(); }
                tokens.push(Token::Or);
            }
            '"' => {
                // String literal
                chars.next();
                let mut s = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '"' {
                        chars.next();
                        break;
                    }
                    s.push(c);
                    chars.next();
                }
                // Check if previous token was a key, if so, this isn't handled here easily
                // Actually, we should handle `key="value"` or `key:"value"`
                // But let's handle simple identifiers first.
                tokens.push(Token::StringLit(s));
            }
            _ => {
                // Identifier or number
                let mut s = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' {
                        s.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                
                if s.eq_ignore_ascii_case("AND") {
                    tokens.push(Token::And);
                } else if s.eq_ignore_ascii_case("OR") {
                    tokens.push(Token::Or);
                } else {
                    // Check for operator
                    let mut op_found = false;
                    if let Some(&next_c) = chars.peek() {
                         if next_c == ':' || next_c == '=' {
                             chars.next();
                             if next_c == '=' && chars.peek() == Some(&'=') {
                                 chars.next(); // skip second =
                             }
                             op_found = true;
                         }
                    }

                    if op_found {
                        // Read value
                        // Skip whitespace
                        while let Some(&c) = chars.peek() {
                            if c.is_whitespace() { chars.next(); } else { break; }
                        }
                        
                        let mut value = String::new();
                        if let Some(&'"') = chars.peek() {
                             chars.next();
                             while let Some(&c) = chars.peek() {
                                if c == '"' { chars.next(); break; }
                                value.push(c);
                                chars.next();
                            }
                        } else {
                            while let Some(&c) = chars.peek() {
                                if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' {
                                    value.push(c);
                                    chars.next();
                                } else {
                                    break;
                                }
                            }
                        }
                        tokens.push(Token::KeyOpValue(s.to_lowercase(), value));
                    } else if s.starts_with("text_contains(") {
                        // Hacky support for `text_contains("...")`
                        // Expected: `text_contains("foo")`
                        // We already consumed `text_contains`, peek is `(`... no, wait.
                        // My lexing above stops at `(`, so `s` is `text_contains`.
                        // But `(` is separate token.
                        // This tokenizing is too simple for function calls.
                        // I will rely on `text:"foo"` syntax primarily, 
                        // but if user writes `text_contains` it might appear as identifier.
                        tokens.push(Token::StringLit(s)); // Treat as bare string? No.
                    } else {
                         tokens.push(Token::StringLit(s));
                    }
                }
            }
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn parse(&mut self) -> Result<Query, String> {
        let q = self.parse_or()?;
        if self.pos < self.tokens.len() {
            return Err("Unexpected trailing tokens".to_string());
        }
        Ok(q)
    }

    fn parse_or(&mut self) -> Result<Query, String> {
        let mut left = self.parse_and()?;
        
        while let Some(Token::Or) = self.peek() {
            self.advance();
            let right = self.parse_and()?;
            left = match left {
                Query::Or(mut list) => { list.push(right); Query::Or(list) },
                _ => Query::Or(vec![left, right]),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Query, String> {
        let mut left = self.parse_primary()?;
        
        while let Some(Token::And) = self.peek() {
            self.advance();
            let right = self.parse_primary()?;
            left = match left {
                Query::And(mut list) => { list.push(right); Query::And(list) },
                _ => Query::And(vec![left, right]),
            };
        }
        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Query, String> {
        let token = self.peek().cloned();
        match token {
            Some(Token::LParen) => {
                self.advance();
                let q = self.parse_or()?;
                if let Some(Token::RParen) = self.peek() {
                    self.advance();
                    Ok(q)
                } else {
                    Err("Missing closing parenthesis".to_string())
                }
            }
            Some(Token::KeyOpValue(key, val)) => {
                self.advance();
                match key.as_str() {
                    "text" | "text_contains" => Ok(Query::Text(val.to_lowercase())),
                    "chat" | "chat_id" | "chatid" => {
                         let id = val.parse::<i64>().map_err(|_| format!("Invalid chat id: {}", val))?;
                         Ok(Query::ChatId(id))
                    },
                    "sender" | "sender_id" | "user" | "userid" => {
                        let id = val.parse::<i64>().map_err(|_| format!("Invalid sender id: {}", val))?;
                        Ok(Query::SenderId(id))
                    },
                    "check_integrity" | "corrupt" => {
                        Ok(Query::CheckKeyMatchesValue)
                    },
                    _ => Err(format!("Unknown key: {}", key)),
                }
            }
            Some(Token::StringLit(s)) => {
                 self.advance();
                 Ok(Query::Text(s.to_lowercase()))
            }
            _ => Err("Unexpected token".to_string()),
        }
    }
}

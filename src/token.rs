//! Split a string into tokens. A token is a run of whitespace, a run of word characters, or one other character.
//!
//! The tokens cover every character of the string in order, and they join back to the string.

/// One token, by its position in the string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token {
    /// The byte offset of the first byte.
    pub start: u32,
    /// The byte offset after the last byte.
    pub end: u32,
    /// The offset of the first character, counted in Unicode code points.
    pub char_start: u32,
    /// A hash of the bytes. Two tokens with different hashes are different.
    pub hash: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Class {
    Space,
    Word,
    Other,
}

fn class(c: char) -> Class {
    if c.is_alphanumeric() || c == '_' {
        Class::Word
    } else if c.is_whitespace() {
        Class::Space
    } else {
        Class::Other
    }
}

/// The 64-bit FNV-1a hash of the bytes.
fn hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The tokens of `text`, or `None` for a string of 4 GiB or more, which the 32-bit offsets cannot hold.
pub fn tokenize(text: &str) -> Option<Vec<Token>> {
    if text.len() >= u32::MAX as usize {
        return None;
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(text.len() / 3 + 1);
    let mut chars = text.char_indices().peekable();
    let mut char_at: u32 = 0;
    while let Some((start, c)) = chars.next() {
        let kind = class(c);
        let char_start = char_at;
        char_at += 1;
        let mut end = start + c.len_utf8();
        if kind != Class::Other {
            while let Some(&(at, next)) = chars.peek() {
                if class(next) != kind {
                    break;
                }
                end = at + next.len_utf8();
                char_at += 1;
                chars.next();
            }
        }
        out.push(Token { start: start as u32, end: end as u32, char_start, hash: hash(&bytes[start..end]) });
    }
    Some(out)
}

/// The text of a token.
pub fn text_of<'a>(text: &'a str, token: &Token) -> &'a str {
    &text[token.start as usize..token.end as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(text: &str) -> Vec<&str> {
        tokenize(text).unwrap().iter().map(|t| text_of(text, t)).collect()
    }

    #[test]
    fn splits_words_spaces_and_single_marks() {
        assert_eq!(parts("We were here, 2 times!"), ["We", " ", "were", " ", "here", ",", " ", "2", " ", "times", "!"]);
    }

    #[test]
    fn keeps_a_run_of_marks_as_single_tokens() {
        assert_eq!(parts("a...b"), ["a", ".", ".", ".", "b"]);
    }

    #[test]
    fn keeps_a_run_of_mixed_whitespace_as_one_token() {
        assert_eq!(parts("a \t\n b"), ["a", " \t\n ", "b"]);
    }

    #[test]
    fn reads_letters_outside_ascii_as_word_characters() {
        assert_eq!(parts("café naïve_x"), ["café", " ", "naïve_x"]);
    }

    #[test]
    fn joins_back_to_the_string() {
        for text in ["", " ", "a", "Hello,  world!\n", "x\u{1F600}y z", "\u{00a0}a\u{2003}b"] {
            assert_eq!(parts(text).concat(), text);
        }
    }

    #[test]
    fn counts_char_offsets_in_code_points() {
        let tokens = tokenize("é a").unwrap();
        assert_eq!(tokens.iter().map(|t| t.char_start).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(tokens.iter().map(|t| t.start).collect::<Vec<_>>(), [0, 2, 3]);
    }

    #[test]
    fn gives_equal_tokens_equal_hashes() {
        let tokens = tokenize("the cat the").unwrap();
        assert_eq!(tokens[0].hash, tokens[4].hash);
        assert_ne!(tokens[0].hash, tokens[2].hash);
    }
}

//! The edits that turn a kept string into a derived string, and the rebuild of the derived string from them.
//!
//! An edit is a gap, a length and a replacement, all counted in Unicode code points. The gap is the distance from
//! the end of the previous edit to the start of this one. The length is the number of code points of the kept
//! string that the replacement takes the place of.

use crate::diff::{diff, Op};
use crate::token::{tokenize, Token};

/// One edit in gap form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Edit {
    pub gap: i32,
    pub length: i32,
    pub replacement: String,
}

/// The cost of one edit beside its replacement, in code points. Two 32-bit integers stay near this size after the
/// encoding and the compression of a parquet column.
pub const EDIT_COST: usize = 6;

/// The largest number of token inserts and deletes that the diff searches for. A row past it keeps its derived
/// string, and its edits would cost more than the string in most cases.
pub const MAX_TOKEN_CHANGES: usize = 2048;

/// The form that a row is stored in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stored {
    /// The derived value is null. The edits are null too.
    Null,
    /// The derived string is stored in full. The edits are null.
    Whole,
    /// The derived string is null, and the edits rebuild it from the kept string.
    Edits(Vec<Edit>),
}

/// The cheaper form for one row: the edits when they cost less than the derived string, or else the derived string.
pub fn choose(keep: Option<&str>, derive: Option<&str>) -> Stored {
    let Some(derive) = derive else { return Stored::Null };
    let Some(keep) = keep else { return Stored::Whole };
    match edits_of(keep, derive) {
        Some(edits) if cost(&edits) < derive.chars().count() => Stored::Edits(edits),
        _ => Stored::Whole,
    }
}

/// The cost of a list of edits, in code points.
pub fn cost(edits: &[Edit]) -> usize {
    edits.iter().map(|e| e.replacement.chars().count() + EDIT_COST).sum()
}

/// The edits that turn `keep` into `derive`, or `None` when the diff passes `MAX_TOKEN_CHANGES` or an offset does
/// not fit in 32 bits.
pub fn edits_of(keep: &str, derive: &str) -> Option<Vec<Edit>> {
    if keep == derive {
        return Some(Vec::new());
    }
    let ta = tokenize(keep)?;
    let tb = tokenize(derive)?;
    let ops = diff(keep, &ta, derive, &tb, MAX_TOKEN_CHANGES)?;
    let keep_chars = ta.last().map_or(0, |t| t.char_start as usize + keep[t.start as usize..].chars().count());
    let ops = merge(&ops, &ta, keep_chars);
    let mut out = Vec::with_capacity(ops.len());
    let mut last = 0usize;
    for op in ops {
        let old = &keep[byte_at(&ta, op.a_start, keep.len())..byte_at(&ta, op.a_end, keep.len())];
        let new = &derive[byte_at(&tb, op.b_start, derive.len())..byte_at(&tb, op.b_end, derive.len())];
        let start = char_at(&ta, op.a_start, keep_chars);
        let (prefix, suffix, replacement) = trim(old, new);
        let old_chars = old.chars().count();
        if prefix + suffix == old_chars && replacement.is_empty() {
            continue;
        }
        let from = start + prefix;
        let to = start + old_chars - suffix;
        out.push(Edit {
            gap: i32::try_from(from - last).ok()?,
            length: i32::try_from(to - from).ok()?,
            replacement: replacement.to_string(),
        });
        last = to;
    }
    Some(out)
}

/// The byte offset of token `i`, where `i` equal to the token count is the end of the string.
fn byte_at(tokens: &[Token], i: usize, len: usize) -> usize {
    tokens.get(i).map_or(len, |t| t.start as usize)
}

/// The code point offset of token `i`, where `i` equal to the token count is the end of the string.
fn char_at(tokens: &[Token], i: usize, chars: usize) -> usize {
    tokens.get(i).map_or(chars, |t| t.char_start as usize)
}

/// The spans with each short equal run between two spans folded into one span. A run shorter than `EDIT_COST` code
/// points costs less as part of a replacement than as the gap of a second edit.
fn merge(ops: &[Op], ta: &[Token], keep_chars: usize) -> Vec<Op> {
    let mut out: Vec<Op> = Vec::with_capacity(ops.len());
    for &op in ops {
        match out.last_mut() {
            Some(last) if char_at(ta, op.a_start, keep_chars) - char_at(ta, last.a_end, keep_chars) < EDIT_COST => {
                last.a_end = op.a_end;
                last.b_end = op.b_end;
            }
            _ => out.push(op),
        }
    }
    out
}

/// The common prefix and suffix of `old` and `new` in code points, and the middle of `new` that replaces the middle
/// of `old`. The prefix and the suffix do not overlap in either string.
fn trim<'a>(old: &str, new: &'a str) -> (usize, usize, &'a str) {
    let mut prefix = 0;
    let mut prefix_bytes = 0;
    for (x, y) in old.chars().zip(new.chars()) {
        if x != y {
            break;
        }
        prefix += 1;
        prefix_bytes += x.len_utf8();
    }
    let (old_rest, new_rest) = (&old[prefix_bytes..], &new[prefix_bytes..]);
    let mut suffix = 0;
    let mut suffix_bytes = 0;
    for (x, y) in old_rest.chars().rev().zip(new_rest.chars().rev()) {
        if x != y {
            break;
        }
        suffix += 1;
        suffix_bytes += x.len_utf8();
    }
    (prefix, suffix, &new_rest[..new_rest.len() - suffix_bytes])
}

/// A failure of the rebuild: an edit points past the end of the kept string.
#[derive(Debug, PartialEq, Eq)]
pub struct OutOfRange {
    pub edit: usize,
}

impl std::fmt::Display for OutOfRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "edit {} points past the end of the kept string", self.edit)
    }
}

impl std::error::Error for OutOfRange {}

/// The derived string, rebuilt from the kept string and its edits as (gap, length, replacement).
pub fn apply<'a, I>(keep: &str, edits: I) -> Result<String, OutOfRange>
where
    I: IntoIterator<Item = (i32, i32, &'a str)>,
{
    let ascii = keep.is_ascii();
    let skip = |from: usize, n: usize| -> Option<usize> {
        if ascii {
            let to = from + n;
            return (to <= keep.len()).then_some(to);
        }
        if n == 0 {
            return Some(from);
        }
        keep[from..].char_indices().nth(n - 1).map(|(i, c)| from + i + c.len_utf8())
    };
    let mut out = String::with_capacity(keep.len() + keep.len() / 8);
    let mut at = 0usize;
    for (index, (gap, length, replacement)) in edits.into_iter().enumerate() {
        let bad = OutOfRange { edit: index };
        let (gap, length) = (usize::try_from(gap), usize::try_from(length));
        let (Ok(gap), Ok(length)) = (gap, length) else { return Err(bad) };
        let start = skip(at, gap).ok_or(OutOfRange { edit: index })?;
        let end = skip(start, length).ok_or(bad)?;
        out.push_str(&keep[at..start]);
        out.push_str(replacement);
        at = end;
    }
    out.push_str(&keep[at..]);
    Ok(out)
}

/// The derived string rebuilt from a list of `Edit`.
pub fn apply_edits(keep: &str, edits: &[Edit]) -> Result<String, OutOfRange> {
    apply(keep, edits.iter().map(|e| (e.gap, e.length, e.replacement.as_str())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(keep: &str, derive: &str) -> Vec<Edit> {
        let edits = edits_of(keep, derive).unwrap();
        assert_eq!(apply_edits(keep, &edits).unwrap(), derive, "keep {keep:?}");
        edits
    }

    fn edit(gap: i32, length: i32, replacement: &str) -> Edit {
        Edit { gap, length, replacement: replacement.to_string() }
    }

    #[test]
    fn trims_a_changed_word_to_the_changed_characters() {
        assert_eq!(round_trip("we were here", "we where here"), [edit(4, 0, "h")]);
    }

    #[test]
    fn counts_gaps_from_the_end_of_the_previous_edit() {
        assert_eq!(round_trip("a cat and a dog", "a bat and a hog"), [edit(2, 1, "b"), edit(9, 1, "h")]);
    }

    #[test]
    fn folds_two_edits_across_a_short_equal_run() {
        assert_eq!(round_trip("one two three", "1 2 three"), [edit(0, 7, "1 2")]);
    }

    #[test]
    fn counts_offsets_in_code_points() {
        assert_eq!(round_trip("café au lait", "cafés au lait"), [edit(4, 0, "s")]);
        round_trip("naïve résumé", "naive resume");
        round_trip("\u{1F600} smile", "\u{1F601} smile");
    }

    #[test]
    fn gives_no_edits_for_equal_strings() {
        assert!(round_trip("same", "same").is_empty());
        assert!(round_trip("", "").is_empty());
    }

    #[test]
    fn handles_an_empty_side() {
        round_trip("", "all new");
        round_trip("all gone", "");
    }

    #[test]
    fn handles_a_number_spoken_in_words() {
        round_trip("It cost 25 pounds on 3 May.", "It cost twenty-five pounds on the third of May.");
    }

    #[test]
    fn chooses_the_edits_when_they_cost_less() {
        assert!(matches!(choose(Some("we were here today"), Some("we where here today")), Stored::Edits(_)));
    }

    #[test]
    fn chooses_the_whole_string_when_the_edits_cost_more() {
        let reply = "I checked the passage and it reads correctly.";
        assert_eq!(choose(Some(reply), Some("A wholly different passage of text.")), Stored::Whole);
    }

    #[test]
    fn keeps_an_empty_derived_string_whole() {
        assert_eq!(choose(Some("text"), Some("")), Stored::Whole);
    }

    #[test]
    fn keeps_a_null_derived_value_null() {
        assert_eq!(choose(Some("text"), None), Stored::Null);
        assert_eq!(choose(None, None), Stored::Null);
    }

    #[test]
    fn keeps_the_derived_string_whole_when_the_kept_value_is_null() {
        assert_eq!(choose(None, Some("text")), Stored::Whole);
    }

    #[test]
    fn refuses_an_edit_past_the_end() {
        assert_eq!(apply("abc", [(2, 5, "x")]), Err(OutOfRange { edit: 0 }));
        assert_eq!(apply("abc", [(4, 0, "x")]), Err(OutOfRange { edit: 0 }));
        assert_eq!(apply("é", [(0, 2, "x")]), Err(OutOfRange { edit: 0 }));
        assert_eq!(apply("abc", [(-1, 0, "x")]), Err(OutOfRange { edit: 0 }));
    }

    #[test]
    fn applies_an_edit_at_the_end() {
        assert_eq!(apply("abc", [(3, 0, "d")]).unwrap(), "abcd");
        assert_eq!(apply("é", [(1, 0, "!")]).unwrap(), "é!");
    }

    /// A seeded xorshift generator, so the property test gives the same strings on every run.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }

        /// A string of up to `most - 1` pieces.
        fn text(&mut self, most: usize) -> String {
            let len = self.below(most);
            const PIECES: [&str; 14] =
                ["a", "b", "the", "cat", " ", " ", "  ", "\n", ",", ".", "é", "\u{1F600}", "12", "_"];
            (0..len).map(|_| PIECES[self.below(PIECES.len())]).collect()
        }
    }

    #[test]
    fn rebuilds_every_random_pair_exactly() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..5000 {
            let keep = rng.text(40);
            let mut derive: Vec<char> = keep.chars().collect();
            for _ in 0..rng.below(6) {
                let at = rng.below(derive.len() + 1);
                match rng.below(3) {
                    0 => derive.splice(at..at, rng.text(4).chars()).for_each(drop),
                    1 if at < derive.len() => {
                        derive.remove(at);
                    }
                    _ => derive.insert(at, ['x', 'é', ' ', '\u{1F600}'][rng.below(4)]),
                }
            }
            let derive: String = derive.into_iter().collect();
            round_trip(&keep, &derive);
            let other = rng.text(20);
            round_trip(&keep, &other);
        }
    }
}

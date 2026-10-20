//! The shortest edit script between two token lists, by the O(ND) algorithm of Myers (1986).
//!
//! The common prefix and suffix are cut first. A passage and its near copy then leave a short middle, and the search
//! cost grows with the number of changed tokens alone.

use crate::token::{text_of, Token};

/// One changed span: the tokens `a[a_start..a_end]` become the tokens `b[b_start..b_end]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Op {
    pub a_start: usize,
    pub a_end: usize,
    pub b_start: usize,
    pub b_end: usize,
}

struct Side<'a> {
    text: &'a str,
    tokens: &'a [Token],
}

impl Side<'_> {
    fn same(&self, i: usize, other: &Side, j: usize) -> bool {
        let (x, y) = (&self.tokens[i], &other.tokens[j]);
        x.hash == y.hash && text_of(self.text, x) == text_of(other.text, y)
    }
}

/// The changed spans that turn `a` into `b`, in order, or `None` when the script needs more than `max_d` token
/// inserts and deletes.
pub fn diff(a_text: &str, a: &[Token], b_text: &str, b: &[Token], max_d: usize) -> Option<Vec<Op>> {
    let sa = Side { text: a_text, tokens: a };
    let sb = Side { text: b_text, tokens: b };
    let mut lo = 0;
    while lo < a.len() && lo < b.len() && sa.same(lo, &sb, lo) {
        lo += 1;
    }
    let (mut ha, mut hb) = (a.len(), b.len());
    while ha > lo && hb > lo && sa.same(ha - 1, &sb, hb - 1) {
        ha -= 1;
        hb -= 1;
    }
    let steps = myers(&sa, lo, ha, &sb, lo, hb, max_d)?;
    Some(group(steps))
}

/// One insert or delete of a single token, as the points before and after it on the edit graph.
#[derive(Clone, Copy)]
struct Step {
    x0: usize,
    y0: usize,
    x1: usize,
    y1: usize,
}

/// The single-token steps of the shortest script between `a[a0..a1]` and `b[b0..b1]`, in order.
fn myers(a: &Side, a0: usize, a1: usize, b: &Side, b0: usize, b1: usize, max_d: usize) -> Option<Vec<Step>> {
    let n = (a1 - a0) as isize;
    let m = (b1 - b0) as isize;
    if n == 0 && m == 0 {
        return Some(Vec::new());
    }
    let limit = max_d.min((n + m) as usize) as isize;
    let off = limit + 1;
    let mut v = vec![0isize; (2 * off + 1) as usize];
    // The state before each round d is kept for the walk back: the entries for k in -d..=d.
    let mut trace: Vec<isize> = Vec::new();
    let mut found = None;
    'search: for d in 0..=limit {
        trace.extend_from_slice(&v[(off - d) as usize..=(off + d) as usize]);
        let mut k = -d;
        while k <= d {
            let i = (off + k) as usize;
            let mut x = if k == -d || (k != d && v[i - 1] < v[i + 1]) { v[i + 1] } else { v[i - 1] + 1 };
            let mut y = x - k;
            while x < n && y < m && a.same(a0 + x as usize, b, b0 + y as usize) {
                x += 1;
                y += 1;
            }
            v[i] = x;
            if x >= n && y >= m {
                found = Some(d);
                break 'search;
            }
            k += 2;
        }
    }
    let total = found?;
    let mut steps = Vec::with_capacity(total as usize);
    let (mut x, mut y) = (n, m);
    for d in (1..=total).rev() {
        // The round d state starts at the sum of 2e+1 for e < d, which is d squared.
        let base = (d * d) as usize;
        let prev = |k: isize| trace[base + (k + d) as usize];
        let k = x - y;
        let down = k == -d || (k != d && prev(k - 1) < prev(k + 1));
        let pk = if down { k + 1 } else { k - 1 };
        let px = prev(pk);
        let py = px - pk;
        let (sx, sy) = if down { (px, py + 1) } else { (px + 1, py) };
        steps.push(Step { x0: a0 + px as usize, y0: b0 + py as usize, x1: a0 + sx as usize, y1: b0 + sy as usize });
        x = px;
        y = py;
    }
    steps.reverse();
    Some(steps)
}

/// The steps joined into spans: two steps touch when one ends where the next starts.
fn group(steps: Vec<Step>) -> Vec<Op> {
    let mut out: Vec<Op> = Vec::new();
    for s in steps {
        match out.last_mut() {
            Some(op) if op.a_end == s.x0 && op.b_end == s.y0 => {
                op.a_end = s.x1;
                op.b_end = s.y1;
            }
            _ => out.push(Op { a_start: s.x0, a_end: s.x1, b_start: s.y0, b_end: s.y1 }),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::tokenize;

    fn ops(a: &str, b: &str) -> Vec<Op> {
        diff(a, &tokenize(a).unwrap(), b, &tokenize(b).unwrap(), 1000).unwrap()
    }

    /// The spans applied to the tokens of `a` give the tokens of `b`.
    fn applies(a: &str, b: &str) {
        let (ta, tb) = (tokenize(a).unwrap(), tokenize(b).unwrap());
        let mut out = String::new();
        let mut at = 0;
        for op in ops(a, b) {
            for t in &ta[at..op.a_start] {
                out.push_str(text_of(a, t));
            }
            for t in &tb[op.b_start..op.b_end] {
                out.push_str(text_of(b, t));
            }
            at = op.a_end;
        }
        for t in &ta[at..] {
            out.push_str(text_of(a, t));
        }
        assert_eq!(out, b);
    }

    #[test]
    fn finds_nothing_between_equal_strings() {
        assert!(ops("the same text", "the same text").is_empty());
    }

    #[test]
    fn finds_one_word_replaced() {
        assert_eq!(ops("we were here", "we where here"), [Op { a_start: 2, a_end: 3, b_start: 2, b_end: 3 }]);
    }

    #[test]
    fn finds_an_insert_and_a_delete() {
        applies("a b c d", "a x b d");
    }

    #[test]
    fn handles_one_side_empty() {
        applies("", "new text");
        applies("old text", "");
        assert!(ops("", "").is_empty());
    }

    #[test]
    fn handles_a_full_rewrite() {
        applies("one two three", "four five six seven");
    }

    #[test]
    fn gives_up_past_the_limit() {
        let (a, b) = ("a b c d e f", "u v w x y z");
        assert!(diff(a, &tokenize(a).unwrap(), b, &tokenize(b).unwrap(), 3).is_none());
    }
}

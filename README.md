---
type: Repository Guide
title: Pairpack
description: Rust tool that stores a near-copy string column of a parquet shard as character edits of another column, and rebuilds it byte for byte.
status: stable
tags: [data, parquet, compression, rust]
generated:
  by: claude-code/opus-5.5
  at: 2026-10-20T16:40:00Z
supervised:
  by: human:ciprian-florin_ifrim
  at: 2026-10-20T16:40:00Z
---

# Pairpack

Datasets that pair each passage with a lightly edited copy of it, such as a corrected version or one with its figures written out as words, store the shared text twice, as Parquet compresses each column independently and zstd cannot exploit redundancy that spans two columns of the same row.

The relevant question is therefore not how to compress the derived column more aggressively, but how to avoid encoding its shared text at all. Pairpack keeps one column in full and stores the other as a list of character edits against it, and a row retains its full string only where the edits would exceed it in size, as with a short reply that shares no text with the passage. Every other column chunk is copied from the source byte for byte, which confines recompression to the two columns that change.

A packed shard is checked row by row against its source before it receives its final name, and any reader can rebuild the derived column with ten lines of Python and no library.

| Work | Where |
|---|---|
| The tokens of a string | `src/token.rs` |
| The shortest token diff, by the Myers algorithm | `src/diff.rs` |
| The edits of one row, the choice of form, and the rebuild | `src/edits.rs` |
| The pack, the unpack, the check and the column sizes of a shard | `src/shard.rs` |
| The command line and the pool of workers | `src/main.rs` |

## 1. Build and Run

    cargo build --release
    cargo test

    pairpack --workers 12 pack --keep response --derive text --out OUT SHARD...
    pairpack --workers 12 unpack --out OUT SHARD...
    pairpack verify --original DIR SHARD...
    pairpack stats SHARD...

`pack` writes each shard to `OUT` with the derived column stored as edits, and `unpack` restores the derived column in full and removes `edits`. `verify` compares each packed shard with the shard of the same name in `DIR`, and `stats` reports the bytes of each column. Shards run in parallel on the pool that `--workers` sets, 8 by default, and the rows of each row group are spread across the same pool.

`tools/make_fixtures.py` writes the fixture shards in `tests/fixtures/` with pyarrow, and every row in them is fabricated.

## 2. Directory Tree

    src/                  the library and the command line
    tests/                the tests of the shard commands and of the binary
    tests/fixtures/       three small shards of fabricated rows
    tools/                the script that writes the fixtures

## 3. Concepts

### 3.1 Kept and Derived Columns

The kept column stays in full in every row, while the derived column is rebuilt from it, and the edits column follows the derived column in the schema. Either column can serve as the kept one. For a passage and its correction, keeping the correction is the better choice, as a row whose answer is a short reply then retains its passage in full while every other row reduces to a handful of edits.

### 3.2 Edits

Each row holds a list of edits, and each edit is a struct of three fields:

    edits: list<struct<gap: int32, length: int32, replacement: string>>

Offsets count Unicode code points, the units of a Python string. `gap` is the distance from the end of the previous edit, and `length` is the span of the kept string that `replacement` takes the place of. An edit holds only the characters that change, so "were" to "where" reduces to an insert of "h".

### 3.3 Rebuild Rule

| Derived column | Edits column | Derived value |
|---|---|---|
| a string | null | the string |
| null | a list | the kept string with the edits applied |
| null | null | null |

The same rule in Python, for a row read through pyarrow or the `datasets` library:

    def rebuild(keep, derived, edits):
        if derived is not None:
            return derived
        if edits is None:
            return None
        out, at = [], 0
        for e in edits:
            start = at + e["gap"]
            out += [keep[at:start], e["replacement"]]
            at = start + e["length"]
        return "".join(out) + keep[at:]

### 3.4 Footer

A packed shard keeps every footer key of its source and adds five of its own, which `unpack` removes again:

| Key | Value |
|---|---|
| `pairpack_format` | the version of the packed format, `1` |
| `pairpack_keep` | the name of the kept column |
| `pairpack_derive` | the name of the derived column |
| `pairpack_edits` | the name of the edits column |
| `pairpack_version` | the version of the tool that packed the shard |

### 3.5 Choice of Form

A row stores its edits only when they cost less than the derived string, where an edit costs its replacement plus 6 code points, close to the size of two integers after encoding and compression. Edits separated by fewer than 6 code points merge into one, as the gap costs less as text than as a second edit.

The diff operates on tokens, each a run of whitespace, a run of word characters, or a single other character. It removes the common prefix and suffix before the Myers search, which keeps the cost proportional to the changed region alone, and a row that needs more than 2,048 token changes keeps its derived string in full.

## 4. Rules

**Never pack a shard in place.** The tool refuses an output directory that already holds a shard of the same name, which leaves the sources intact after a failed run.

**Move the source shards only after every shard reports `rows verified`.** A shard that fails its check keeps only its `.partial` file.

**Give any change to the format a new `pairpack_format`.** Readers decide by that key, and `unpack` refuses a version that it does not know.

**Write parquet at zstd level 9.** `ZSTD_LEVEL` in `src/shard.rs` holds the level, as a shard is written once and read many times, and higher levels cost write time for little further gain.

## 5. Outputs

Each command prints one JSON line per shard as the shard finishes. A line for `pack` reports the rows, the rows from edits, the rows stored whole, the null rows, the rows verified, the bytes before and after, and the seconds. A failed shard reports an `error` field, and the command exits with a failure once the remaining shards finish.

## 6. Limitations

| Limitation | Reason |
|---|---|
| Both columns must be string columns, and the derived column must be nullable | A packed row stores null in the derived column |
| A row with more than 2,048 token changes keeps its derived string in full | The cost and the memory of the diff grow with the square of the changes |
| A reader must apply the rebuild rule to obtain the derived column | The derived column holds null wherever the edits rebuild it |

## 7. Benchmarks

Each shard is about 430 MB, packed with 8 workers on an Apple M5 Pro with 24 GB of memory, and each time includes the check of every row.

| Derived column | Rows | Before | After | Smaller | Seconds |
|---|---|---|---|---|---|
| a passage with errors, kept beside its correction | 195,961 | 425.6 MB | 240.3 MB | 43.6% | 12.7 |
| a passage with its figures written as words | 203,982 | 431.2 MB | 237.9 MB | 44.8% | 10.5 |
| a short document with its personal data masked | 1,950,000 | 448.8 MB | 431.0 MB | 4.0% | 22.6 |

In the first shard the derived column fell from 228.0 MB to 34.7 MB, against 8.0 MB of edits. Counterintuitively, the masked shard rebuilds 80 percent of its rows from edits yet gains the least of the three. This cannot be attributed to the diff. Rather, it reflects the size of the derived column, which occupied only 42.1 MB of the shard before packing.

Two whole sets of the first two kinds, packed with 12 workers on the same machine:

| Derived column | Shards | Rows | Before | After | Smaller | Minutes |
|---|---|---|---|---|---|---|
| a passage with errors, kept beside its correction | 213 | 67,679,660 | 92.12 GB | 52.19 GB | 43.4% | 25.0 |
| a passage with its figures written as words | 99 | 23,937,561 | 42.36 GB | 22.69 GB | 46.4% | 6.7 |

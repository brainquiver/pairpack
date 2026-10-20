"""Write the fixture shards of the integration tests to tests/fixtures. Every row is made up for the tests.

    corrections.parquet   a passage with errors as `text` and its correction as `response`, or a short reply for
                          a passage that reads correctly
    spoken.parquet        a passage as `text` and the same passage with its figures spoken as `response`
    edge.parquet          null values, empty strings, characters outside ASCII, a full rewrite, and a large string
                          column type

Each shard has several small row groups, and the tests then cross a row group boundary.

    python tools/make_fixtures.py
"""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
LEVEL = 9
FOOTER = {"fixture_note": "made-up rows for the pairpack tests"}

CORRECTIONS = [
    ("corrected", "We was going to the market on monday, but it rained.", "We were going to the market on Monday, but it rained."),
    ("corrected", "Their is a new libary in the town hall.", "There is a new library in the town hall."),
    ("corrected", "The report were sent too the board last week.", "The report was sent to the board last week."),
    ("already_correct", "The bridge opened in 1932 and still carries traffic.", "I checked the passage, and it reads correctly."),
    ("corrected", "She dont like coffee , she prefer tea.", "She does not like coffee, she prefers tea."),
    ("already_correct", "Rain is likely in the north tomorrow.", "The passage reads correctly as it is."),
    ("corrected", "Its been a busy week, and its not over yet.", "It's been a busy week, and it's not over yet."),
    ("corrected", "The commitee will meet on the 3rd of may.", "The committee will meet on the 3rd of May."),
]

SPOKEN = [
    ("normalized", "The train leaves at 09:45 from platform 3.", "The train leaves at nine forty-five from platform three."),
    ("normalized", "It cost 25 pounds on 3 May 2024.", "It cost twenty-five pounds on the third of May twenty twenty-four."),
    ("unchanged", "The garden has a pond and an old oak tree.", "The garden has a pond and an old oak tree."),
    ("normalized", "Call 0161 496 0000 before 5pm.", "Call oh one six one, four nine six, oh oh oh oh before five p m."),
    ("normalized", "The shop sold 1,250 books in 2 days.", "The shop sold one thousand two hundred and fifty books in two days."),
    ("unchanged", "", ""),
]

EDGE = [
    ("null derived", "A kept passage with no derived value.", None),
    ("null kept", None, "A derived passage with no kept value."),
    ("both null", None, None),
    ("empty derived", "Something to keep.", ""),
    ("empty kept", "", "Something new."),
    ("accents", "Le café est près de la gare.", "Le café est tout près de la gare."),
    ("emoji", "Good work \U0001F600 on the launch.", "Great work \U0001F601 on the launch!"),
    ("full rewrite", "Short note.", "A completely different and much longer passage of text."),
    ("whitespace", "Line one\nLine two\n\n  Line three", "Line one\n\nLine two\n  Line three\n"),
]


def write(name, rows, columns, types, row_group_size):
    arrays = [pa.array([r[i] for r in rows], type=t) for i, t in enumerate(types)]
    schema = pa.schema([pa.field(c, t) for c, t in zip(columns, types)], metadata=FOOTER)
    table = pa.Table.from_arrays(arrays, schema=schema)
    OUT.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, OUT / name, compression="zstd", compression_level=LEVEL, row_group_size=row_group_size)


if __name__ == "__main__":
    s = pa.string()
    write("corrections.parquet", CORRECTIONS, ["kind", "text", "response"], [s, s, s], 3)
    write("spoken.parquet", SPOKEN, ["kind", "text", "response"], [s, s, s], 4)
    write("edge.parquet", EDGE, ["kind", "text", "response"], [s, pa.large_string(), pa.large_string()], 2)

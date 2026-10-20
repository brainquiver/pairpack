//! Pack, verify and unpack the fixture shards, and check the refusals of each command.

use std::fs::File;
use std::path::{Path, PathBuf};

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use pairpack::shard;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

/// The batches of a shard, one for each row group.
fn batches(path: &Path) -> Vec<RecordBatch> {
    let groups = row_groups(path).len();
    (0..groups)
        .map(|g| {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
            let mut reader = builder.with_row_groups(vec![g]).with_batch_size(usize::MAX).build().unwrap();
            reader.next().unwrap().unwrap()
        })
        .collect()
}

fn footer(path: &Path) -> Vec<(String, String)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let kv = reader.metadata().file_metadata().key_value_metadata().cloned().unwrap_or_default();
    kv.into_iter().filter(|kv| kv.key != "ARROW:schema").map(|kv| (kv.key, kv.value.unwrap_or_default())).collect()
}

fn row_groups(path: &Path) -> Vec<i64> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    reader.metadata().row_groups().iter().map(|g| g.num_rows()).collect()
}

/// Pack the fixture, then unpack it, and give the paths of both.
fn round_trip(name: &str, keep: &str, derive: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let (packed_dir, unpacked_dir) = (dir.path().join("packed"), dir.path().join("unpacked"));
    std::fs::create_dir_all(&packed_dir).unwrap();
    std::fs::create_dir_all(&unpacked_dir).unwrap();
    shard::pack(&fixture(name), &packed_dir, keep, derive, "edits").unwrap();
    let packed = packed_dir.join(name);
    shard::unpack(&packed, &unpacked_dir).unwrap();
    (dir, packed, unpacked_dir.join(name))
}

#[test]
fn unpacks_every_fixture_to_its_original_values() {
    for (name, keep, derive) in [
        ("corrections.parquet", "response", "text"),
        ("spoken.parquet", "text", "response"),
        ("edge.parquet", "text", "response"),
    ] {
        let (_dir, packed, unpacked) = round_trip(name, keep, derive);
        let (want, got) = (batches(&fixture(name)), batches(&unpacked));
        assert_eq!(want.len(), got.len(), "{name}");
        for (w, g) in want.iter().zip(&got) {
            assert_eq!(w, g, "{name}");
        }
        shard::verify(&packed, &fixture(name)).unwrap();
    }
}

#[test]
fn keeps_the_row_groups_and_the_footer_keys() {
    let (_dir, packed, unpacked) = round_trip("corrections.parquet", "response", "text");
    assert_eq!(row_groups(&packed), row_groups(&fixture("corrections.parquet")));
    let packed_footer = footer(&packed);
    for pair in footer(&fixture("corrections.parquet")) {
        assert!(packed_footer.contains(&pair), "{pair:?}");
    }
    assert!(packed_footer.contains(&("pairpack_format".into(), "1".into())));
    assert!(packed_footer.contains(&("pairpack_keep".into(), "response".into())));
    assert!(packed_footer.contains(&("pairpack_derive".into(), "text".into())));
    assert_eq!(footer(&unpacked), footer(&fixture("corrections.parquet")));
}

#[test]
fn places_the_edits_column_after_the_derived_column() {
    let (_dir, packed, _) = round_trip("corrections.parquet", "response", "text");
    let schema = batches(&packed)[0].schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["kind", "text", "edits", "response"]);
}

#[test]
fn stores_a_corrected_row_as_edits_and_a_short_reply_whole() {
    let dir = tempfile::tempdir().unwrap();
    let report = shard::pack(&fixture("corrections.parquet"), dir.path(), "response", "text", "edits").unwrap();
    assert_eq!(report["rows from edits"], 6);
    assert_eq!(report["rows stored whole"], 2);
    assert_eq!(report["rows verified"], 8);
}

#[test]
fn keeps_null_values_null() {
    let dir = tempfile::tempdir().unwrap();
    let report = shard::pack(&fixture("edge.parquet"), dir.path(), "text", "response", "edits").unwrap();
    assert_eq!(report["rows null"], 2);
}

#[test]
fn refuses_to_pack_a_packed_shard() {
    let (dir, packed, _) = round_trip("spoken.parquet", "text", "response");
    let again = dir.path().join("again");
    std::fs::create_dir_all(&again).unwrap();
    let err = shard::pack(&packed, &again, "text", "response", "edits").unwrap_err();
    assert!(err.to_string().contains("packed already"), "{err}");
}

#[test]
fn refuses_to_overwrite_a_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(fixture("spoken.parquet"), dir.path().join("spoken.parquet")).unwrap();
    let err = shard::pack(&fixture("spoken.parquet"), dir.path(), "text", "response", "edits").unwrap_err();
    assert!(err.to_string().contains("does not overwrite"), "{err}");
}

#[test]
fn refuses_a_missing_or_wrong_column() {
    let dir = tempfile::tempdir().unwrap();
    let missing = shard::pack(&fixture("spoken.parquet"), dir.path(), "text", "answer", "edits").unwrap_err();
    assert!(missing.to_string().contains("no column answer"), "{missing}");
    let taken = shard::pack(&fixture("spoken.parquet"), dir.path(), "text", "response", "kind").unwrap_err();
    assert!(taken.to_string().contains("column kind already"), "{taken}");
    let same = shard::pack(&fixture("spoken.parquet"), dir.path(), "text", "text", "edits").unwrap_err();
    assert!(same.to_string().contains("same column"), "{same}");
}

#[test]
fn refuses_to_unpack_a_plain_shard() {
    let dir = tempfile::tempdir().unwrap();
    let err = shard::unpack(&fixture("spoken.parquet"), dir.path()).unwrap_err();
    assert!(err.to_string().contains("not a packed shard"), "{err}");
}

#[test]
fn finds_a_shard_with_other_row_groups() {
    let (_dir, packed, _) = round_trip("spoken.parquet", "text", "response");
    let err = shard::verify(&packed, &fixture("corrections.parquet")).unwrap_err();
    assert!(err.to_string().contains("row groups"), "{err}");
}

#[test]
fn finds_one_derived_value_that_differs() {
    let (dir, packed, _) = round_trip("spoken.parquet", "text", "response");
    let changed = dir.path().join("changed.parquet");
    let source = batches(&fixture("spoken.parquet"));
    let mut writer = ArrowWriter::try_new(File::create(&changed).unwrap(), source[0].schema(), None).unwrap();
    for (k, batch) in source.iter().enumerate() {
        let mut columns = batch.columns().to_vec();
        if k == 0 {
            let response = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            let values: Vec<_> =
                (0..response.len()).map(|i| if i == 1 { "A changed value." } else { response.value(i) }).collect();
            columns[2] = Arc::new(StringArray::from(values));
        }
        writer.write(&RecordBatch::try_new(batch.schema(), columns).unwrap()).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    let err = shard::verify(&packed, &changed).unwrap_err();
    assert_eq!(err.to_string(), "row 1 of row group 0 rebuilds to a different value");
}

#[test]
fn reports_the_bytes_of_each_column() {
    let (_dir, packed, _) = round_trip("corrections.parquet", "response", "text");
    let report = shard::stats(&packed).unwrap();
    assert_eq!(report["rows"], 8);
    assert_eq!(report["packed"], true);
    for column in ["kind", "text", "edits", "response"] {
        assert!(report["columns"][column]["compressed"].as_i64().unwrap() > 0, "{column}");
    }
}

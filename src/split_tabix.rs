use csv::Writer;
use flate2::{bufread, write, Compression};
use niffler::{compression, Error};
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TabixRecord {
    chrom: String,
    start: u64,
    end: u64,
    barcode: String,
    count: u64,
}

#[derive(Debug)]
pub struct FragmentStats {
    fragments_total: u64,
    fragments_written: HashMap<String, u64>,
}

impl FragmentStats {
    pub fn new() -> FragmentStats {
        FragmentStats {
            fragments_total: 0,
            fragments_written: HashMap::new(),
        }
    }
}

pub fn split_tabix_by_barcode(
    filename: &str,
    barcodes: &HashMap<String, HashSet<String>>,
) -> Result<FragmentStats, std::io::Error> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .comment(Some(b'#'))
        .from_reader(
            niffler::from_path(&filename)
                .expect("Error opening fragments file")
                .0,
        );

    let mut writers: HashMap<String, Writer<Box<dyn std::io::Write>>> = barcodes
        .keys()
        .map(|name| format!("{}.tsv.gz", name))
        .map(|filename| {
            csv::WriterBuilder::new().delimiter(b'\t').from_writer(
                niffler::to_path(&filename, compression::Format::Gzip, niffler::Level::Five)
                    .expect("Error opening output"),
            )
        })
        .zip(barcodes.keys())
        .fold(HashMap::new(), |mut acc, (handle, name)| {
            acc.entry(name.to_string()).or_insert(handle);
            acc
        });

    let mut stats = FragmentStats::new();

    for (ii, result) in reader.records().enumerate() {
        stats.fragments_total += 1;

        match result {
            Ok(r) => {
                let record: TabixRecord = r.deserialize(None).unwrap();
                for (barcodes_name, barcode_set) in barcodes {
                    if barcode_set.contains(&record.barcode) {
                        let writer = writers.get_mut(barcodes_name).unwrap();
                        writer
                            .serialize(&record)
                            .expect(&format!("Failed to write record number: {}", ii));
                        stats
                            .fragments_written
                            .entry(barcodes_name.to_string())
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                    }
                }
            }
            Err(_res) => continue,
        }
    }

    Ok(stats)
}

pub fn split_tabix_by_fragment_size(
    filename: &str,
    bins: &HashMap<String, Vec<u64>>,
) -> Result<FragmentStats, std::io::Error> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .comment(Some(b'#'))
        .from_reader(
            niffler::from_path(&filename)
                .expect("Error opening fragments file")
                .0,
        );

    let mut writers: HashMap<String, Writer<Box<dyn std::io::Write>>> = bins
        .keys()
        .map(|name| format!("{}.tsv.gz", name))
        .map(|filename| {
            csv::WriterBuilder::new().delimiter(b'\t').from_writer(
                niffler::to_path(&filename, compression::Format::Gzip, niffler::Level::Five)
                    .expect("Error opening output"),
            )
        })
        .zip(bins.keys())
        .fold(HashMap::new(), |mut acc, (handle, name)| {
            acc.entry(name.to_string()).or_insert(handle);
            acc
        });

    let mut stats = FragmentStats::new();

    for (ii, result) in reader.records().enumerate() {
        stats.fragments_total += 1;

        match result {
            Ok(r) => {
                let record: TabixRecord = r.deserialize(None).unwrap();
                let record_length = record.end - record.start;

                for (bin_name, bin_range) in bins {
                    if bin_range[0] < record_length && record_length < bin_range[1] {
                        let writer = writers.get_mut(bin_name).unwrap();
                        writer
                            .serialize(&record)
                            .expect(&format!("Failed to write record number: {}", ii));
                        stats
                            .fragments_written
                            .entry(bin_name.to_string())
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                    }
                }
            }
            Err(_res) => continue,
        }
    }

    Ok(stats)
}

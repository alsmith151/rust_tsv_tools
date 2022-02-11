use csv::Writer;
use flate2::{bufread, write, Compression};
use niffler::{compression, Error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TabixRecord {
    chrom: String,
    start: String,
    end: String,
    barcode: String,
    count: u64,
}

#[derive(Debug)]
pub struct BarcodeStats {
    fragments_total: u64,
    fragments_written: HashMap<String, u64>,
}

impl BarcodeStats {
    pub fn new() -> BarcodeStats {
        BarcodeStats {
            fragments_total: 0,
            fragments_written: HashMap::new(),
        }
    }
}

// fn get_reader_handle(path: &str) -> Box<dyn io::Read> {
//     if path.ends_with(".gz") {
//         let f = File::open(path).unwrap();
//         Box::new(bufread::GzDecoder::new(io::BufReader::new(f)))
//     } else {
//         Box::new(File::open(path).unwrap())
//     }
// }

// fn get_writer_handle(path: &str) -> Box<dyn io::Write> {
//     let f = File::create(path).expect("Cannot open output file");
//     if path.ends_with(".gz") {
//         Box::new(io::BufWriter::new(write::GzEncoder::new(
//             f,
//             Compression::default(),
//         )))
//     } else {
//         Box::new(io::BufWriter::new(f))
//     }
// }

pub fn split_tabix_by_barcode(
    filename: &str,
    barcodes: &HashMap<String, HashSet<String>>,
) -> Result<BarcodeStats, std::io::Error> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
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
            csv::Writer::from_writer(
                niffler::to_path(&filename, compression::Format::Gzip, niffler::Level::Five)
                    .expect("Error opening output"),
            )
        })
        .zip(barcodes.keys())
        .fold(HashMap::new(), |mut acc, (handle, name)| {
            acc.entry(name.to_string()).or_insert(handle);
            acc
        });

    let mut stats = BarcodeStats::new();

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

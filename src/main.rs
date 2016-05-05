#![feature(btree_range, collections_bound)]

extern crate memchr;

pub mod sparsebuf;

use std::fs::File;
use std::io::Write;

fn main() {
    let f = File::open("test.log").unwrap();
    let meta = f.metadata().unwrap();
    let mut buf = sparsebuf::SparseBuf::new(f, meta.len());

    for line in buf.read_lines(10).iter() {
        std::io::stdout().write(line.as_bytes()).unwrap();
    }
    // loop {
    //     let mut b: [u8; 125000] = [0; 125000];
    //     // let mut line: Vec<u8> = Vec::new();
    //     // let count = buf.read_until(b'\n', &mut line).unwrap();
    //     let count = buf.read(&mut b).unwrap();
    //
    //     std::io::stdout().write(&b[..count+1]).unwrap();
    //
    //     if count < 125000 {
    //         break;
    //     }
    // }

    println!("done!");
}

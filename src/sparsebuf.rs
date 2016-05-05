use std::collections::BTreeMap;
use std::collections::Bound::Included;
use std::io::{Read, Seek, SeekFrom};
use std::fmt::{Display, Formatter};
use memchr::memchr;
use std;

const CHUNK_SIZE_BYTES: usize = 125000;
const LINE_SEEK_SIZE_BYTES: usize = 1024;

struct Chunk {
    start: u64,
    size: usize,
    buf: [u8; CHUNK_SIZE_BYTES]
}

impl Display for Chunk {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let buf = String::from_utf8_lossy(&self.buf[0..100]);
        write!(f, "start: {}, size: {}, buf: `{}`...", self.start, self.size, buf)
    }
}

pub struct SparseBuf<F: Seek + Read> {
    pos: u64,
    end: u64,
    source: F,
    chunks: BTreeMap<u64, Chunk>,
}

impl<F: Seek + Read> Display for SparseBuf<F> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "pos: {}\n", self.pos).unwrap();

        for (_, c) in self.chunks.iter() {
            write!(f, "{}\n", c).unwrap();
        }

        write!(f, "===")
    }
}

impl<F: Seek + Read> Seek for SparseBuf<F> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.source.seek(pos)
    }
}

impl<F: Seek + Read> SparseBuf<F> {
    pub fn new(f: F, len: u64) -> SparseBuf<F> {
        SparseBuf {
            pos: 0,
            end: len,
            source: f,
            chunks: BTreeMap::new(),
        }
    }

    fn fetch_chunk(&mut self, aligned_start: u64) {
        if self.chunks.contains_key(&aligned_start) {
            // already loaded
            return
        }

        self.source.seek(SeekFrom::Start(aligned_start)).unwrap();

        let mut chunk = Chunk {
            start: aligned_start,
            size: 0,
            buf: [0; CHUNK_SIZE_BYTES],
        };

        // TODO - handle errors here more sensibly
        let read_bytes = self.source.read(&mut chunk.buf).unwrap();
        chunk.size = read_bytes;

        self.chunks.insert(aligned_start, chunk);
    }

    fn fetch_chunks(&mut self, unaligned_start: u64, size: usize) {
        let chunk_size = CHUNK_SIZE_BYTES as u64;
        let read_size = size as u64;
        let unaligned_end = unaligned_start + read_size;

        let first_offset = unaligned_start - (unaligned_start % chunk_size);
        let last_offset = unaligned_end - (unaligned_end % chunk_size);
        let chunks_to_read = (last_offset - first_offset) / chunk_size + 1;

        for i in 0..chunks_to_read {
            self.fetch_chunk(first_offset+i*chunk_size);
        }
    }

    pub fn read_lines(&mut self, num_lines: u64) -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        for _ in 0..num_lines {
            let mut line: Vec<u8> = Vec::new();
            let bytes_read = self.read_until(b'\n', &mut line).unwrap();

            if bytes_read == 0 {
                break
            }

            lines.push(String::from_utf8(line).unwrap());
        }

        return lines
    }

    fn read_until(&mut self, delim: u8, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let start = self.pos;

        let mut tmp: [u8; LINE_SEEK_SIZE_BYTES] = [0; LINE_SEEK_SIZE_BYTES];
        let mut total: usize = 0;

        loop {
            let bytes_read = self.read(&mut tmp).unwrap();

            match memchr(delim, &tmp) {
                Some(i) => {
                    total += i;
                    buf.extend_from_slice(&tmp[..i+1]);
                    break
                },
                None => {
                    total += bytes_read;
                    buf.extend_from_slice(&tmp);

                    if bytes_read < LINE_SEEK_SIZE_BYTES {
                        break
                    }
                }
            }
        }

        self.pos = 1 + start + total as u64;
        return Ok(total)
    }

    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let position = self.pos;
        self.source.seek(SeekFrom::Start(position)).unwrap();
        self.fetch_chunks(position, buf.len());

        let chunk_size = CHUNK_SIZE_BYTES as u64;
        let unaligned_start = position;
        let unaligned_end = position + buf.len() as u64;
        let aligned_start = unaligned_start - (unaligned_start % chunk_size);
        let aligned_end = unaligned_end - (unaligned_end % chunk_size);

        let mut bytes_read: usize = 0;
        let mut offset_in_source = unaligned_start;
        let mut offset_in_buf = 0;
        for (_, ref chunk) in self.chunks.range(Included(&aligned_start), Included(&aligned_end)) {
            let offset_in_chunk = (offset_in_source % chunk_size) as usize;
            for i in offset_in_chunk..chunk.size {
                buf[offset_in_buf] = chunk.buf[i];

                offset_in_buf += 1;
                offset_in_source += 1;
                bytes_read += 1;

                if bytes_read == buf.len() {
                    self.pos += bytes_read as u64;
                    return Ok(bytes_read);
                }
            }
        }

        self.pos += bytes_read as u64;
        Ok(bytes_read)
    }
}

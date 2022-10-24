use crossbeam_utils::sync::WaitGroup;
use std::env;
use std::os::macos::fs::MetadataExt;
use std::path::Path;
use std::fs;
use std::fs::{File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use crossbeam_channel::Sender;
use chrono::prelude::Local;

// const FILENAME:&str = "./small_int_set";
const FILENAME:&str = "./huge_int_set";
const INDEX_FILE:&str = "./index";

static mut ROUTINE_COUNT:u64 = 10;

struct AvgNums {
    avg: f64,
    n:u64,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() == 3 {
        if &args[1] == "index" {
            let rc: &i32 = &args[2].parse::<i32>().unwrap();
            build_index(*rc);
        }
        return;
    }
    if args.len() == 2 {
        let rc: &u64 = &args[1].parse::<u64>().unwrap();
        unsafe { ROUTINE_COUNT = *rc; }
    }

    let mut use_index = false;
    let mut indexes: Vec<u64> = Vec::new();

    if Path::new(INDEX_FILE).exists() {
        let str = fs::read_to_string(INDEX_FILE).unwrap();

        let mut _list = str.split("\n").filter(|it|it.trim() != "").collect::<Vec<_>>();
        indexes = _list.iter().map(|it|(*it).parse::<u64>().unwrap()).collect::<Vec<u64>>();
        use_index = true;
        unsafe { ROUTINE_COUNT = indexes.len() as u64; }
    }

    let (_sender, _receiver) = crossbeam_channel::unbounded::<AvgNums>();

    let path = Path::new(FILENAME);
    let meta = fs::metadata(path).unwrap();
    let total_size = meta.st_size();
    let block_size = ((total_size as f64)/(unsafe { ROUTINE_COUNT } as f64)).ceil() as u64;

    let _start = Local::now().timestamp_millis();

    let r0 = _receiver.clone();
    let sp = tokio::spawn(async move {
        let mut av = 0.0;
        let mut n = 1.0;
        while let Some(item) = r0.recv().ok() {
            av = (n-1.0)/(n+(item.n as f64)-1.0)*av+item.avg*((item.n as f64)/(n+(item.n as f64)-1.0));
            n += item.n as f64;
        }
        return (av, (n - 1.0) as u64);
    });

    let wg = WaitGroup::new();

    let mut current: u64 = 0;
    let mut limit_size: u64;

    let count = unsafe { ROUTINE_COUNT };
    for _n in 0..(count-1) as usize {
        if use_index {
            limit_size = indexes[_n+1] - indexes[_n];
        } else {
            limit_size = block_size;
        }
        if limit_size <= 0 {
            break;
        }

        let w0 = wg.clone();
        let s0 = _sender.clone();

        tokio::spawn(async move {
            read(_n as i32, current, limit_size, s0);
            drop(w0);
        });

        // 507229825 / -11135.900658

        if use_index {
            current = indexes[_n+1];
        } else {
            current += block_size + 1;
        }
    }

    wg.wait();

    drop(_sender);
    drop(_receiver);

    let (av, n) = sp.await.unwrap();

    let _end = Local::now().timestamp_millis();

    println!("numbers = {}, avg = {}, time = {}", n, av, _end - _start);
}

fn read(/*idx*/ _:i32, offset:u64, limit:u64, ch: Sender<AvgNums>) {
    let mut av = 0.0;
    let mut n = 1.0;

    let mut file = File::open(FILENAME).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let f0 = file.try_clone().unwrap();
    let mut reader = BufReader::new(f0);

    let mut cummulative_size: u64 = 0;
    loop {
        if cummulative_size >= limit {
            break;
        }
        let mut b = String::new();
        let sz = reader.read_line(&mut b).unwrap();
        if sz <= 0 {
            break;
        }
        cummulative_size += sz as u64;
        let f = b.trim().parse::<f64>().unwrap();
        av = (n-1.0)/n*av + f/n;
        n += 1.0;
    }
    drop(file);
    ch.send(AvgNums { avg: av, n: (n - 1.0) as u64 }).unwrap();
}

fn build_index(_rc:i32) {
    let path = Path::new(FILENAME);
    let meta = fs::metadata(path).unwrap();
    let total_size = meta.st_size();
    let block_size = ((total_size as f64)/(_rc as f64)).ceil() as u64;

    let mut file = File::open(FILENAME).unwrap();
    let mut sp: Vec<u64> = Vec::new();
    sp.push(0);

    let mut current = block_size;
    for _i in 1.._rc {
        file.seek(SeekFrom::Start(current)).unwrap();
        let f0 = file.try_clone().unwrap();
        let reader = BufReader::new(f0);
        let mut offset: u64 = 0;
        let mut has_return: bool = false;
        for b in reader.bytes() {
            offset += 1;
            if b.is_ok() {
                if b.unwrap() == 10 {
                    // 如果读到一个回车，直接记录上一个的结束位置
                    sp.push(current + offset);
                    has_return = true;
                    break;
                }
            } else {
                break;
            }
        }
        if !has_return {
            // 一行到最后都没读到回车，也算是一行
            sp.push(current + offset + 1);
        }
        current += offset + block_size + 1;
        if current >= total_size {
            break;
        }
    }

    sp.push(total_size);
    let sp1 = sp.iter().map(|it| it.to_string()).collect::<Vec<String>>();
    let str = sp1.join("\n");
    drop(file);
    fs::write(INDEX_FILE,  str).unwrap();
}

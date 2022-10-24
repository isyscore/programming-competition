use std::f64;
use std::time::Instant;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::str::FromStr;
use rayon::prelude::*;

fn main() {
    let t_start = Instant::now();
    let filename = "./huge_int_set";
    let ifile = File::open(filename).unwrap();
    let f0 = ifile.try_clone();
    let buffered = BufReader::new(ifile);
    let vec: Vec<f64> = buffered.lines().map(|line| f64::from_str(&line.unwrap()).unwrap()).collect();
    drop(f0);
    let num = vec.len()  as f64;
    let sum: f64 = vec.par_iter().sum();

    println!("Sum = {}, and Avg = {}", sum , (sum/num) as f64);
    println!("time elapsed = {}ms", t_start.elapsed().as_millis());
}

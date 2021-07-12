use std::collections::{HashMap, HashSet};
use std::fs;
use std::time::SystemTime;
use std::*;

//test
fn main() {
    let now = SystemTime::now();

    let slow_pair: sync::Arc<(
        sync::Mutex<HashMap<String, HashMap<String, String>>>,
        sync::Condvar,
    )> = sync::Arc::new((sync::Mutex::new(HashMap::new()), sync::Condvar::new()));
    let slow_pair_thr = sync::Arc::clone(&slow_pair);

    let bo_pair: sync::Arc<(sync::Mutex<HashMap<String, String>>, sync::Condvar)> =
        sync::Arc::new((sync::Mutex::new(HashMap::new()), sync::Condvar::new()));
    let bo_pair_thr = sync::Arc::clone(&bo_pair);

    let check_pair: sync::Arc<(sync::Mutex<HashMap<String, Vec<String>>>, sync::Condvar)> =
        sync::Arc::new((sync::Mutex::new(HashMap::new()), sync::Condvar::new()));
    let check_pair_thr = sync::Arc::clone(&check_pair);

    let thr3 = std::thread::spawn(move || {
        let contents =
            fs::read_to_string("ZASILENIE.txt").expect("Something went wrong reading the file");
        let mut lines = contents.split("\r\n").collect::<Vec<&str>>();
        lines.remove(0);
        lines.remove(lines.len() - 1);
        lines.retain(|&x| if &x[..2] != "0;" { true } else { false });

        let (mtx, cvar) = &*check_pair_thr;
        let result = cvar
            .wait_while(
                mtx.lock().unwrap(),
                |data: &mut HashMap<String, Vec<String>>| data.len() == 0,
            )
            .unwrap();

        let mut res_string: String = String::new();

        for line in &lines {
            let tmp_iter = line.split(';');
            let tmp_array = tmp_iter.collect::<Vec<&str>>();

            res_string.push_str(line);
            res_string.push(';');
            res_string.push_str(&*result.get(tmp_array[0]).unwrap()[5]);
            res_string.push(';');
            res_string.push_str(&*result.get(tmp_array[0]).unwrap()[3]);
            res_string.push('\n');
        }
        fs::write("check.txt", res_string).unwrap();
    });

    let thr2 = std::thread::spawn(move || {
        let (mtx, cvar) = &*bo_pair_thr;
        let mut data = mtx.lock().unwrap();

        let contents = fs::read_to_string("BO.txt").expect("Something went wrong reading the file");
        let mut lines = contents.split("\r\n").collect::<Vec<&str>>();
        lines.remove(0);
        lines.remove(lines.len() - 1);

        for line in lines {
            let tmp_iter = line.split(';');
            let tmp_array = tmp_iter.collect::<Vec<&str>>();

            data.insert(tmp_array[0].to_string(), tmp_array[1].to_string());
        }
        cvar.notify_one();
    });

    let thr1 = std::thread::spawn(move || {
        let (mtx, cvar) = &*slow_pair_thr;
        let mut data = mtx.lock().unwrap();

        let contents =
            fs::read_to_string("SLOW.txt").expect("Something went wrong reading the file");
        let mut lines = contents.split("\r\n").collect::<Vec<&str>>();
        lines.remove(0);
        lines.remove(lines.len() - 1);

        for line in lines {
            let tmp_iter = line.split(';');
            let tmp_array = tmp_iter.collect::<Vec<&str>>();

            match data.get(tmp_array[2]) {
                Some(tmp_map) => {
                    let mut new_map = tmp_map.clone();
                    new_map.insert(tmp_array[0].to_string(), tmp_array[1].to_string());
                    data.insert(tmp_array[2].to_string(), new_map);
                }
                None => {
                    let mut tmp_map: HashMap<String, String> = HashMap::new();
                    tmp_map.insert(tmp_array[0].to_string(), tmp_array[1].to_string());
                    data.insert(tmp_array[2].to_string(), tmp_map);
                }
            }
        }
        cvar.notify_one();
    });

    let contents_data =
        fs::read_to_string("ZASILENIE.txt").expect("Something went wrong reading the file");

    let mut zasil_contents = contents_data.split("\r\n").collect::<Vec<&str>>();
    zasil_contents.remove(0);
    zasil_contents.remove(&zasil_contents.len() - 1);
    zasil_contents.retain(|&x| if &x[..2] != "0;" { true } else { false });

    let mut map: HashMap<String, Vec<String>> = HashMap::new(); //ile loginów posiada klient
    let mut map2: HashMap<String, HashSet<String>> = HashMap::new(); //ile klientow posiada login
    let mut clients: HashMap<String, Vec<String>> = HashMap::new(); //info o klientach

    for line in &zasil_contents {
        let tmp = line.split(';').collect::<Vec<&str>>();
        if tmp.len() > 2 {
            match map2.get(&tmp[1].to_string()) {
                Some(value) => {
                    let mut new_vec = value.clone();
                    new_vec.insert(tmp[0].to_string());
                    map2.insert(tmp[1].to_string(), new_vec);
                }
                None => {
                    let mut new_vec = HashSet::new();
                    new_vec.insert(tmp[0].to_string());
                    map2.insert(tmp[1].to_string(), new_vec);
                }
            }

            match map.get(&tmp[0].to_string()) {
                Some(value) => {
                    let mut new_vec = value.clone();
                    new_vec.push(tmp[1].to_string());
                    map.insert(tmp[0].to_string(), new_vec);
                }
                None => {
                    let mut new_vec = Vec::new();
                    new_vec.push(tmp[1].to_string());
                    map.insert(tmp[0].to_string(), new_vec);
                }
            }
        }
        //                                        oddzial                kategoria          wyl
        clients.insert(
            tmp[0].to_string(),
            vec![tmp[2].to_string(), tmp[3].to_string(), tmp[4].to_string()],
        );
    }

    let (mtx, cvar) = &*slow_pair;
    let slow_data = cvar
        .wait_while(
            mtx.lock().unwrap(),
            |data: &mut HashMap<String, HashMap<String, String>>| data.len() == 0,
        )
        .unwrap();
    thr1.join().unwrap();

    let (mtx, cvar) = &*bo_pair;
    let bo_data = cvar
        .wait_while(mtx.lock().unwrap(), |data: &mut HashMap<String, String>| {
            data.len() == 0
        })
        .unwrap();
    thr2.join().unwrap();

    let (mtx, cvar) = &*check_pair;
    let mut result = mtx.lock().unwrap();

    for (c, loginy) in map {
        let len = loginy.len();
        if len < 3 {
            let mut byl = false;
            for log in loginy {
                match map2.get(&log) {
                    Some(value) => {
                        if value.len() > 1 {
                            byl = true;
                        }
                    }
                    None => {}
                }
            }
            if byl {
                let c_vec = &clients.get(&c).unwrap();
                let mut tmp_vec = Vec::new();
                tmp_vec.push(c_vec[0].clone()); //oddzial
                tmp_vec.push(c.clone()); //modulo
                match bo_data.get(&c.clone()) {
                    //stare
                    Some(symbol) => {
                        tmp_vec.push(symbol.to_string());
                    }
                    None => {
                        tmp_vec.push("brak".to_string());
                    }
                }
                tmp_vec.push(
                    slow_data
                        .get("5")
                        .unwrap()
                        .get(&c_vec[1])
                        .unwrap()
                        .to_string(),
                ); //nowe
                tmp_vec.push(c_vec[2].clone());
                tmp_vec.push("5".to_string());

                result.insert(c.clone(), tmp_vec);
            } else {
                let c_vec = &clients.get(&c).unwrap();
                let mut tmp_vec = Vec::new();
                tmp_vec.push(c_vec[0].clone()); //oddzial
                tmp_vec.push(c.clone()); //modulo
                match bo_data.get(&c.clone()) {
                    //stare
                    Some(symbol) => {
                        tmp_vec.push(symbol.to_string());
                    }
                    None => {
                        tmp_vec.push("brak".to_string());
                    }
                }
                tmp_vec.push("brak".to_string()); //nowe
                tmp_vec.push(c_vec[2].clone());
                tmp_vec.push("brak".to_string());

                result.insert(c.clone(), tmp_vec);
            }
        } else if len < 6 {
            let c_vec = &clients.get(&c).unwrap();
            let mut tmp_vec = Vec::new();
            tmp_vec.push(c_vec[0].clone()); //oddzial
            tmp_vec.push(c.clone()); //modulo
            match bo_data.get(&c.clone()) {
                //stare
                Some(symbol) => {
                    tmp_vec.push(symbol.to_string());
                }
                None => {
                    tmp_vec.push("brak".to_string());
                }
            }
            tmp_vec.push(
                slow_data
                    .get("5")
                    .unwrap()
                    .get(&c_vec[1])
                    .unwrap()
                    .to_string(),
            ); //nowe
            tmp_vec.push(c_vec[2].clone());
            tmp_vec.push("5".to_string());

            result.insert(c.clone(), tmp_vec);
        } else {
            let c_vec = &clients.get(&c).unwrap();
            let mut tmp_vec = Vec::new();
            tmp_vec.push(c_vec[0].clone()); //oddzial
            tmp_vec.push(c.clone()); //modulo
            match bo_data.get(&c.clone()) {
                //stare
                Some(symbol) => {
                    tmp_vec.push(symbol.to_string());
                }
                None => {
                    tmp_vec.push("brak".to_string());
                }
            }
            tmp_vec.push(
                slow_data
                    .get("20")
                    .unwrap()
                    .get(&c_vec[1])
                    .unwrap()
                    .to_string(),
            ); //nowe
            tmp_vec.push(c_vec[2].clone());
            tmp_vec.push("20".to_string());

            result.insert(c.clone(), tmp_vec);
        }
    }

    let mut res_string: String = String::new();

    for (k, v) in &*result {
        if (&v[2] != &v[3]) && (&v[4] == "0") {
            res_string.push_str(&v[0]);
            res_string.push(';');
            res_string.push_str(&k);
            res_string.push(';');
            res_string.push_str(&v[2]);
            res_string.push(';');
            res_string.push_str(&v[3]);
            res_string.push(';');
            res_string.push('\n');
        }
    }

    drop(result);
    cvar.notify_one();

    fs::write("wynik.txt", res_string).unwrap();

    thr3.join().unwrap();

    let end = now.elapsed().unwrap();

    println!("{} ms", end.as_millis());
    println!("{} ns", end.as_nanos());
}

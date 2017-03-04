extern crate rayon;
extern crate pbr;

use std::time::Instant;
use std::path::Path;
use std::io::Write;
use std::io::stderr;
use std::io::BufReader;
use std::io::BufRead;
use std::fs::File;
use std::env;
use std::collections::HashSet;
use rayon::prelude::*;
use pbr::{ProgressBar, Units};

type Id = usize;

#[derive(Debug)]
struct Video {
    size: u32,
}

#[derive(Debug)]
struct Endpoint {
    latency: u32,
    cache_connections: Vec<(Id, u32)>,
}

#[derive(Debug)]
struct Cache {}

#[derive(Debug, Clone)]
struct RequestDescription {
    amount: u32,
    video_id: Id,
    endpoint_id: Id,
}

// https://github.com/gsingh93/rust-graph/blob/master/src/graph.rs visualize
#[derive(Debug)]
struct Input {
    videos: Vec<Video>,
    endpoints: Vec<Endpoint>,
    caches: Vec<Cache>,
    cache_size: u32,
    request_descriptions: Vec<RequestDescription>,
}

fn parse_input<P>(filename: P, debug: bool) -> Input
    where P: AsRef<Path>
{
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(&file);
    let mut lines = reader.lines();
    let number_of_endpoints: usize;
    let number_of_caches: usize;
    let cache_size: u32;
    let mut videos = Vec::new();
    let mut endpoints = Vec::new();
    let mut caches = Vec::new();
    let mut request_descriptions = Vec::new();

    {
        let line = lines.next().unwrap().unwrap();
        let mut parts = line.split_whitespace();
        let _number_of_videos = parts.next().unwrap();
        number_of_endpoints = parts.next().unwrap().parse().unwrap();
        let _number_of_request_descriptions = parts.next().unwrap();
        number_of_caches = parts.next().unwrap().parse().unwrap();
        cache_size = parts.next().unwrap().parse().unwrap();
        if debug {
            println!("{} videos, {} endpoints, {} request descriptions, {} caches {}MB each.",
                     _number_of_videos,
                     number_of_endpoints,
                     _number_of_request_descriptions,
                     number_of_caches,
                     cache_size);
        }
    }

    for _id in 0..number_of_caches {
        caches.push(Cache {})
    }

    {
        let line = lines.next().unwrap().unwrap();
        let parts = line.split_whitespace();
        for (id, size) in parts.enumerate() {
            let size: u32 = size.parse().unwrap();
            if debug {
                println!("Video #{}: {}MB", id, size);
            }
            videos.push(Video { size: size });
        }
    }

    {
        for endpoint_id in 0..number_of_endpoints {
            let line = lines.next().unwrap().unwrap();
            let latency: u32;
            let number_of_caches: usize;
            let mut cache_connections = Vec::new();

            {
                let mut parts = line.split_whitespace();
                latency = parts.next().unwrap().parse().unwrap();
                number_of_caches = parts.next().unwrap().parse().unwrap();
                if debug {
                    println!("Endpoint {} has {}ms datacenter latency and is connected to {} \
                              caches:",
                             endpoint_id,
                             latency,
                             number_of_caches);
                }
            }

            for _ in 0..number_of_caches {
                let line = lines.next().unwrap().unwrap();
                let mut parts = line.split_whitespace();
                let cache_id: usize = parts.next().unwrap().parse().unwrap();
                let cache_latency: u32 = parts.next().unwrap().parse().unwrap();
                cache_connections.push((cache_id, cache_latency));
                if debug {
                    println!{"The latency (of endpoint {}) to cache {} is {}ms.", endpoint_id, cache_id, cache_latency};
                }
            }

            cache_connections.sort_by(|a, b| a.1.cmp(&b.1));

            endpoints.push(Endpoint {
                latency: latency,
                cache_connections: cache_connections,
            });
        }
    }

    for line in lines {
        let mut parts = line.as_ref().unwrap().split_whitespace();
        let video_id: usize = parts.next().unwrap().parse().unwrap();
        let endpoint_id: usize = parts.next().unwrap().parse().unwrap();
        let amount: u32 = parts.next().unwrap().parse().unwrap();
        if debug {
            println!("{} requests for video {} coming from endpoint {}.",
                     amount,
                     video_id,
                     endpoint_id);
        }

        request_descriptions.push(RequestDescription {
            amount: amount,
            video_id: video_id,
            endpoint_id: endpoint_id,
        });
    }

    Input {
        videos: videos,
        endpoints: endpoints,
        caches: caches,
        cache_size: cache_size,
        request_descriptions: request_descriptions,
    }
}

#[derive(Debug)]
struct State<'a> {
    cached_videos: Vec<HashSet<Id>>,
    cache_usage: Vec<u32>,
    input: &'a Input,
}

impl<'a> State<'a> {
    fn new(input: &Input) -> State {
        State {
            cached_videos: vec![HashSet::new(); input.caches.len()],
            cache_usage: vec![0; input.caches.len()],
            input: input,
        }
    }

    fn cache_usage(&self, cache_id: Id) -> u32 {
        self.cache_usage[cache_id]
    }

    fn is_caching(&self, endpoint_id: Id, video_id: Id) -> bool {
        self.input.endpoints[endpoint_id]
            .cache_connections
            .iter()
            .any(|&(cache_id, _)| self.cached_videos[cache_id].contains(&video_id))
    }

    fn insert_video_in_cache(&mut self, cache_id: Id, video_id: Id) {
        self.cached_videos[cache_id].insert(video_id);
        self.cache_usage[cache_id] += self.input.videos[video_id].size;
    }

    #[allow(dead_code)]
    fn score(&self) -> u64 {
        let mut sum_latency = 0;
        let mut sum_requests = 0;

        for request_description in self.input.request_descriptions.iter() {
            let ref endpoint = self.input.endpoints[request_description.endpoint_id];
            let mut latency = None;

            sum_requests += request_description.amount;

            for &(cache_id, cache_latency) in endpoint.cache_connections.iter() {
                if self.cached_videos[cache_id].contains(&request_description.video_id) {
                    latency = Some(cache_latency);
                    break;
                }
            }

            if let Some(latency) = latency {
                sum_latency += (endpoint.latency - latency) * request_description.amount;
            }
        }

        ((sum_latency as f64 / sum_requests as f64) * 1000.0).floor() as u64
    }

    fn output(&self) -> String {
        let mut buffer = self.input.caches.len().to_string();
        buffer.push_str("\n");

        for (cache_id, videos) in self.cached_videos.iter().enumerate() {
            buffer.push_str(cache_id.to_string().as_str());

            for video_id in videos {
                buffer.push(' ');
                buffer.push_str(video_id.to_string().as_str());
            }

            buffer.push_str("\n");
        }

        buffer
    }
}

fn greedy_next(state: &State) -> Option<(u32, (Id, Id))> {
    state.input
        .request_descriptions
        .par_iter()
        .filter_map(|request_description| {
            let ref endpoint = state.input.endpoints[request_description.endpoint_id];
            let ref video = state.input.videos[request_description.video_id];

            if let Some(&(cache_id, cache_latency)) =
                {
                    if state.is_caching(request_description.endpoint_id,
                                        request_description.video_id) {
                        return None;
                    }

                    endpoint.cache_connections
                        .iter()
                        .find(|&&(cache_id, _)| {
                            state.input.cache_size as i32 - state.cache_usage(cache_id) as i32 >=
                            video.size as i32
                        })
                } {
                Some(((endpoint.latency - cache_latency) * request_description.amount,
                      (request_description.video_id, cache_id)))
            } else {
                None
            }
        })
        .max_by_key(|x| x.0)
}

fn greedy<T: Write>(state: &mut State, pb: &mut ProgressBar<T>) {
    while let Some((_, (video_id, cache_id))) = greedy_next(state) {
        state.insert_video_in_cache(cache_id, video_id);
        pb.add(state.input.videos[video_id].size as u64 * 1_048_576);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let input = parse_input(&args[1], false);
    let mut state = State::new(&input);

    let mut pb = ProgressBar::on(stderr(),
                                 input.caches.len() as u64 * input.cache_size as u64 * 1_048_576);
    pb.set_units(Units::Bytes);
    let now = Instant::now();

    greedy(&mut state, &mut pb);

    writeln!(stderr(),
             "\nTime: {}s\nScore: {}",
             Instant::now().duration_since(now).as_secs(),
             state.score())
        .unwrap();
    print!("{}", state.output())
}

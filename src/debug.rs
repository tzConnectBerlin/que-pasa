use crate::itertools::Itertools;

pub(crate) fn pp_depth<T>(depth: usize, t: T) -> String
where
    T: std::fmt::Debug,
{
    let s = format!("{:#?}", t);
    let depth_cutoff: String = " ".repeat(depth * 4 + 1);
    s.split('\n')
        .filter(|line| !line.starts_with(&depth_cutoff))
        .join("\n")
}

/// Load from the ../test directory, only for testing
#[cfg(test)]
pub(crate) fn load_test(name: &str) -> String {
    std::fs::read_to_string(std::path::Path::new(name)).unwrap()
}

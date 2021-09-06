use crate::itertools::Itertools;

pub(crate) fn pp_depth<T>(depth: usize, t: T) -> String
where
    T: std::fmt::Debug,
{
    let s = format!("{:#?}", t);
    let depth_spacing: String = std::iter::repeat(' ')
        .take(depth * 4 + 1)
        .collect();
    s.split('\n')
        .filter(|line| !line.starts_with(&depth_spacing))
        .join("\n")
}

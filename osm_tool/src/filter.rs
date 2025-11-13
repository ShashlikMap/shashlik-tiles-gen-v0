use std::collections::HashMap;

pub struct TagFilter {
    filter: Vec<(u32, Option<u32>)>,
}

impl TagFilter {
    pub fn new(string_table: &[String], filter_tags: &[(&str, Option<&str>)]) -> Self {
        let tag_map: HashMap<&str, u32> = string_table
            .iter()
            .enumerate()
            .filter_map(|(i, s)| {
                let i = i as u32;
                let s = s.as_str();

                for (k, v) in filter_tags {
                    if *k == s {
                        return Some((s, i));
                    }

                    if let Some(v) = v {
                        if *v == s {
                            return Some((s, i));
                        }
                    }
                }

                None
            })
            .collect();

        let filter = filter_tags
            .iter()
            .filter_map(|(k, v)| {
                let k = match tag_map.get(*k) {
                    Some(k) => *k,
                    _ => return None,
                };

                let v = match v {
                    Some(v) => match tag_map.get(*v) {
                        Some(v) => Some(*v),
                        _ => return None,
                    },
                    _ => None,
                };

                Some((k, v))
            })
            .collect();

        Self { filter }
    }

    pub fn filter<'a>(
        &self,
        string_table: &'a [String],
        tags: &HashMap<u32, u32>,
    ) -> Option<(&'a str, &'a str)> {
        for (k, v) in tags {
            for (fk, fv) in &self.filter {
                if *k == *fk {
                    if let Some(fv) = fv {
                        if *v != *fv {
                            continue;
                        }
                    }

                    return Some((&string_table[*k as usize], &string_table[*v as usize]));
                }
            }
        }
        None
    }

    pub fn filter_all<'a>(
        &self,
        string_table: &'a [String],
        tags: &HashMap<u32, u32>,
    ) -> Vec<(&'a str, &'a str)> {
        let mut result = Vec::new();
        for (k, v) in tags {
            for (fk, fv) in &self.filter {
                if *k == *fk {
                    if let Some(fv) = fv {
                        if *v != *fv {
                            continue;
                        }
                    }
                    result.push((
                        string_table[*k as usize].as_str(),
                        string_table[*v as usize].as_str(),
                    ));
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::TagFilter;

    #[test]
    fn test_tag_filter() {
        let string_table = [
            "a".to_owned(),
            "b".to_owned(),
            "c".to_owned(),
            "d".to_owned(),
        ];

        let tags1: HashMap<u32, u32> = [(0, 1), (2, 3)].into_iter().collect();
        let tags2: HashMap<u32, u32> = [(0, 2), (2, 0)].into_iter().collect();
        let tags3: HashMap<u32, u32> = [(1, 2), (2, 0)].into_iter().collect();

        let tag_filter = TagFilter::new(&string_table, &[("a", Some("b")), ("b", None)]);

        assert_eq!(tag_filter.filter(&string_table, &tags1), Some(("a", "b")));
        assert_eq!(tag_filter.filter(&string_table, &tags2), None);
        assert_eq!(tag_filter.filter(&string_table, &tags3), Some(("b", "c")));
    }
}

use std::{collections::HashMap, iter::Iterator};

/// OSM packed tag iterator
pub struct TagIterator<I> {
    iter: I,
    tags: HashMap<u32, u32>,
}

impl<I: Iterator<Item = i32>> Iterator for TagIterator<I> {
    type Item = HashMap<u32, u32>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let key = match self.iter.next() {
                Some(key) => key as u32,
                _ => {
                    if self.tags.is_empty() {
                        return None;
                    } else {
                        return Some(std::mem::take(&mut self.tags));
                    }
                }
            };

            if key == 0 {
                return Some(std::mem::take(&mut self.tags));
            }

            let val = self.iter.next()? as u32;

            self.tags.insert(key, val);
        }
    }
}

pub trait IntoTagIterator<I> {
    fn tags(self) -> TagIterator<I>;
}

impl<I: Iterator<Item = i32>> IntoTagIterator<I> for I {
    fn tags(self) -> TagIterator<I> {
        TagIterator {
            iter: self,
            tags: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::IntoTagIterator;
    use std::collections::HashMap;

    #[test]
    fn test_tag_iterator() {
        let packed_tags = [1, 2, 3, 4, 0, 2, 1];

        let tags: Vec<HashMap<u32, u32>> = packed_tags.into_iter().tags().collect();

        assert_eq!(tags.len(), 2, "Expect 2 sets of tags");
        assert_eq!(tags[0].len(), 2, "Expect 2 tags in first set");
        assert_eq!(tags[0].get(&1), Some(&2));
        assert_eq!(tags[0].get(&3), Some(&4));
        assert_eq!(tags[1].len(), 1, "Expect 1 tag in second set");
        assert_eq!(tags[1].get(&2), Some(&1));
    }
}

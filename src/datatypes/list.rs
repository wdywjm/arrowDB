use bytes::Bytes;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Default)]
pub struct List {
    items: HashMap<String, VecDeque<Bytes>>,
}

impl List {
    pub(crate) fn new() -> Self {
        List {
            items: HashMap::new(),
        }
    }

    pub(crate) fn lpush(&mut self, key: &str, values: Vec<Bytes>) -> Option<usize> {
        let list = self.items.entry(key.into()).or_default();
        for v in values.iter() {
            list.push_front(Bytes::from(v.clone()));
        }
        Some(values.len())
    }

    pub(crate) fn lpushx(&mut self, key: &str, values: Vec<Bytes>) -> Option<usize> {
        let mut num = 0;
        if let Some(item) = self.items.get_mut(key) {
            for v in values.iter() {
                item.push_front(Bytes::from(v.clone()));
            }
            num = values.len();
        }
        Some(num)
    }

    pub(crate) fn lpop(&mut self, key: &str) -> Option<Bytes> {
        self.items
            .get_mut(key)
            .unwrap_or(&mut VecDeque::new())
            .pop_front()
            .map(|bytes| bytes)
    }

    pub(crate) fn rpush(&mut self, key: &str, values: Vec<Bytes>) -> Option<usize> {
        let list = self.items.entry(key.into()).or_insert(VecDeque::new());
        for v in values.iter() {
            list.push_back(Bytes::from(v.clone()));
        }
        Some(values.len())
    }

    pub(crate) fn rpushx(&mut self, key: &str, values: Vec<Bytes>) -> Option<usize> {
        let mut num = 0;
        if let Some(item) = self.items.get_mut(key) {
            for v in values.iter() {
                item.push_back(Bytes::from(v.clone()));
            }
            num = values.len();
        }
        Some(num)
    }

    pub(crate) fn rpop(&mut self, key: &str) -> Option<Bytes> {
        self.items
            .get_mut(key)
            .unwrap_or(&mut VecDeque::new())
            .pop_back()
            .map(|bytes| bytes)
    }

    pub(crate) fn llen(&self, key: &str) -> Option<usize> {
        Some(self.items.get(key).unwrap_or(&VecDeque::default()).len())
    }

    pub(crate) fn lindex(&self, key: &str, index: usize) -> Option<Bytes> {
        if !self.items.contains_key(key) {
            return None;
        }
        let list = self.items.get(key).unwrap();
        if index >= list.len() {
            return None;
        }
        let bytes_mut = list.get(index).unwrap();
        Some(Bytes::copy_from_slice(bytes_mut.as_ref()))
    }

    pub(crate) fn lpos(&self, key: &str, value: &Bytes) -> Option<usize> {
        if !self.items.contains_key(key) {
            return None;
        }
        let list = self.items.get(key).unwrap();
        list.into_iter().position(|item| item.eq(value))
    }

    pub(crate) fn lset(&mut self, key: &str, index: usize, value: Bytes) -> Option<usize> {
        if !self.items.contains_key(key) {
            return None;
        }
        self.items.get_mut(key).and_then(|x| {
            if index >= x.len() {
                return None;
            }
            if let Some(item) = x.get_mut(index) {
                *item = Bytes::from(value.clone());
            };
            return Some(1);
        })
    }

    pub(crate) fn lrange(&self, key: &str, start: usize, end: usize) -> Option<Vec<Bytes>> {
        if !self.items.contains_key(key) {
            return None;
        }
        let mut res = vec![];
        for (index, item) in self.items.get(key).unwrap().iter().enumerate() {
            if index < start {
                continue;
            }
            if index > end {
                break;
            }
            res.push(Bytes::copy_from_slice(item));
        }
        Some(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lpush() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = list.lpush("key1", values.clone());
        assert_eq!(result, Some(values.len()));
        let list_items = list.items.get("key1").unwrap();
        assert_eq!(list_items.len(), values.len());
        for i in 0..values.len() {
            assert_eq!(
                list_items[i],
                Bytes::from(values[values.len() - i - 1][..].to_vec())
            );
        }
    }

    #[test]
    fn test_lpushx() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = list.lpushx("key1", values.clone());
        assert_eq!(result, Some(0));
        list.lpush("key1", values.clone());
        let result = list.lpushx("key1", vec![Bytes::from("value4")]);
        assert_eq!(result, Some(1));
        let list_items = list.items.get("key1").unwrap();
        assert_eq!(list_items.len(), values.len() + 1);
        assert_eq!(list_items[0], Bytes::from("value4"));
    }

    #[test]
    fn test_lpop() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        list.lpush("key1", values.clone());
        let result = list.lpop("key2");
        assert_eq!(result, None);
        let result = list.lpop("key1");
        assert_eq!(result, Some(values[2].clone()));
        let list_items = list.items.get("key1").unwrap();
        assert_eq!(list_items.len(), values.len() - 1);
    }

    #[test]
    fn test_rpush() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = list.rpush("key1", values.clone());
        assert_eq!(result, Some(values.len()));
        let list_items = list.items.get("key1").unwrap();
        assert_eq!(list_items.len(), values.len());
        for i in 0..values.len() {
            assert_eq!(list_items[i], Bytes::from(values[i][..].to_vec()));
        }
    }

    #[test]
    fn test_rpushx() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = list.rpushx("key1", values.clone());
        assert_eq!(result, Some(0));

        list.rpush("key1", values.clone());
        let result = list.rpushx("key1", vec![Bytes::from("value4")]);
        assert_eq!(result, Some(1));
        let list_items = list.items.get("key1").unwrap();
        assert_eq!(list_items.len(), values.len() + 1);
        assert_eq!(list_items[values.len()], Bytes::from("value4"));
    }

    #[test]
    fn test_rpop() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        list.rpush("key1", values.clone());
        let result = list.rpop("key2");
        assert_eq!(result, None);
        let result = list.rpop("key1");
        assert_eq!(result, Some(values[2].clone()));
        let list_items = list.items.get("key1").unwrap();
        assert_eq!(list_items.len(), values.len() - 1);
    }

    #[test]
    fn test_llen() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = list.llen("key1");
        assert_eq!(result, Some(0));
        list.lpush("key1", values.clone());
        let result = list.llen("key1");
        assert_eq!(result, Some(values.len()));
    }

    #[test]
    fn test_lindex() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = list.lindex("key1", 1);
        assert_eq!(result, None);
        list.lpush("key1", values.clone());
        let result = list.lindex("key1", 1);
        assert_eq!(result, Some(values[1].clone()));
    }

    #[test]
    fn test_lpos() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
            Bytes::from("value2"),
        ];
        let result = list.lpos("key1", &Bytes::from("value4"));
        assert_eq!(result, None);
        list.lpush("key1", values);
        let result = list.lpos("key1", &Bytes::from("value2"));
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_lset() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = list.lset("key1", 1, Bytes::from("value4"));
        assert_eq!(result, None);
        list.lpush("key1", values);
        let result = list.lset("key1", 1, Bytes::from("value4"));
        assert_eq!(result, Some(1));
        let list_items = list.items.get("key1").unwrap();
        assert_eq!(list_items[1], Bytes::from("value4"));
    }

    #[test]
    fn test_lrange() {
        let mut list = List::new();
        let values = vec![
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];
        let result = list.lrange("key1", 0, 2);
        assert_eq!(result, None);
        list.lpush("key1", values.clone());
        let result = list.lrange("key1", 0, 1);
        assert_eq!(result, Some(vec![values[2].clone(), values[1].clone()]));
    }
}

use std::collections::{HashMap, HashSet};

use bytes::Bytes;

#[derive(Debug, Default)]
pub struct Set {
    items: HashMap<String, HashSet<Bytes>>,
}

impl Set {
    pub fn new() -> Self {
        Set {
            items: HashMap::new(),
        }
    }

    pub fn sadd(&mut self, key: &str, members: Vec<Bytes>) -> Option<usize> {
        self.items.entry(key.to_string()).or_default();
        let num = members.len();
        members.into_iter().for_each(|member| {
            self.items.get_mut(key).unwrap().insert(member);
        });

        Some(num)
    }

    pub fn scard(&self, key: &str) -> Option<usize> {
        if !self.items.contains_key(key) {
            return None;
        }
        Some(self.items.get(key).unwrap().len())
    }

    pub fn sdiff(&self, key: &str, keys: Vec<&str>) -> Option<Vec<Bytes>> {
        let mut res = vec![];
        let default_set = HashSet::new();
        let mut diff = self.items.get(key).unwrap_or(&default_set).clone();
        keys.into_iter().for_each(|set_name| {
            let other_set = self.items.get(set_name).unwrap_or(&default_set);
            diff = diff.difference(other_set).cloned().collect();
        });

        for d in diff.into_iter() {
            res.push(d.clone());
        }

        Some(res)
    }

    pub fn sinter(&self, key: &str, keys: Vec<&str>) -> Option<Vec<Bytes>> {
        if !self.items.contains_key(key) {
            return None;
        }
        let mut res = vec![];
        let mut intersection = self.items.get(key).unwrap().clone();
        let default_set = HashSet::<Bytes>::new();

        keys.into_iter().for_each(|set_name| {
            if intersection.is_empty() {
                return;
            }
            let other_set = self.items.get(set_name).unwrap_or(&default_set);
            intersection = intersection.intersection(other_set).cloned().collect();
        });

        for inter in intersection.into_iter() {
            res.push(inter.clone());
        }

        Some(res)
    }

    pub fn suion(&self, key: &str, keys: Vec<&str>) -> Option<Vec<Bytes>> {
        let mut res = vec![];
        let mut union = self.items.get(key).unwrap().clone();
        let default_set = HashSet::new();

        keys.into_iter().for_each(|set_name| {
            let other_set = self.items.get(set_name).unwrap_or(&default_set);
            union = union.union(other_set).cloned().collect();
        });

        for u in union.into_iter() {
            res.push(u.clone());
        }

        Some(res)
    }

    pub fn sismember(&self, key: &str, member: Bytes) -> Option<bool> {
        Some(self.items.contains_key(key) && self.items.get(key).unwrap().contains(&member))
    }

    pub fn smembers(&self, key: &str) -> Option<Vec<Bytes>> {
        if !self.items.contains_key(key) {
            return None;
        }
        let res: Vec<Bytes> = self.items.get(key).unwrap().iter().cloned().collect();

        Some(res)
    }

    pub fn srem(&mut self, key: &str, members: Vec<Bytes>) -> Option<usize> {
        if !self.items.contains_key(key) {
            return Some(0);
        }
        let mut removed: usize = 0;
        let items = self.items.get_mut(key).unwrap();
        members.into_iter().for_each(|member| {
            if (*items).remove(&member) {
                removed += 1;
            }
        });

        Some(removed)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_sadd() {
        let mut set = Set::new();
        let members = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member3"),
        ];
        let result = set.sadd("key1", members.clone());
        assert_eq!(result, Some(members.len()));
        let set_items = set.items.get("key1").unwrap();
        assert_eq!(set_items.len(), members.len());
        for member in members.iter() {
            assert!(set_items.contains(member));
        }
    }

    #[test]
    fn test_scard() {
        let mut set = Set::new();
        let members = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member3"),
        ];
        let result = set.scard("key1");
        assert_eq!(result, None);
        set.sadd("key1", members.clone());
        let result = set.scard("key1");
        assert_eq!(result, Some(members.len()));
    }

    #[test]
    fn test_sdiff() {
        let mut set = Set::new();
        let members1 = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member3"),
            Bytes::from("member4"),
        ];
        let members2 = vec![
            Bytes::from("member2"),
            Bytes::from("member4"),
            Bytes::from("member5"),
        ];
        let members3 = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member6"),
        ];
        set.sadd("key1", members1.clone());
        set.sadd("key2", members2.clone());
        set.sadd("key3", members3.clone());
        let result = set.sdiff("key1", vec!["key2", "key3"]);
        assert_eq!(result, Some(vec![members1[2].clone()]));
    }

    #[test]
    fn test_sinter() {
        let mut set = Set::new();
        let members1 = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member3"),
            Bytes::from("member4"),
        ];
        let members2 = vec![
            Bytes::from("member2"),
            Bytes::from("member4"),
            Bytes::from("member5"),
        ];
        let members3 = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member6"),
        ];
        set.sadd("key1", members1.clone());
        set.sadd("key2", members2.clone());
        set.sadd("key3", members3.clone());
        let result = set.sinter("key1", vec!["key2", "key3"]);
        assert_eq!(result, Some(vec![members1[1].clone()]));
    }

    #[test]
    fn test_suion() {
        let mut set = Set::new();
        let members1 = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member3"),
        ];
        let members2 = vec![
            Bytes::from("member2"),
            Bytes::from("member4"),
            Bytes::from("member5"),
        ];
        set.sadd("key1", members1.clone());
        set.sadd("key2", members2.clone());
        let result = set.suion("key1", vec!["key2"]);
        assert!(result
            .unwrap()
            .iter()
            .all(|item| members1.contains(item) || members2.contains(item)));
    }

    #[test]
    fn test_sismember() {
        let mut set = Set::new();
        let members = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member3"),
        ];
        set.sadd("key1", members.clone());
        let result = set.sismember("key2", Bytes::from("member1"));
        assert_eq!(result, Some(false));
        let result = set.sismember("key1", Bytes::from("member1"));
        assert_eq!(result, Some(true));
    }

    #[test]
    fn test_smembers() {
        let mut set = Set::new();
        let members = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member3"),
        ];
        let result = set.smembers("key1");
        assert_eq!(result, None);
        set.sadd("key1", members.clone());
        let result = set.smembers("key1");
        assert!(result.unwrap().iter().all(|item| members.contains(item)));
    }

    #[test]
    fn test_srem() {
        let mut set = Set::new();
        let members = vec![
            Bytes::from("member1"),
            Bytes::from("member2"),
            Bytes::from("member3"),
        ];
        let result = set.srem("key1", members.clone());
        assert_eq!(result, Some(0));
        set.sadd("key1", members.clone());
        let removed_members = vec![Bytes::from("member1"), Bytes::from("member3")];
        let result = set.srem("key1", removed_members.clone());
        assert_eq!(result, Some(2));
        let set_items = set.items.get("key1").unwrap();
        assert_eq!(set_items.len(), members.len() - removed_members.len());
        for member in members.iter() {
            if removed_members.contains(member) {
                assert!(!set_items.contains(member));
            } else {
                assert!(set_items.contains(member));
            }
        }
    }
}

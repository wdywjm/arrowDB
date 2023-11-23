use bytes::Bytes;
use rand::Rng;
use std::{collections::HashMap, sync::Arc};
use atomic_refcell::AtomicRefCell;

const SKIPLISTMAXLEVEL: usize = 32;
const SKIPLISTP: f64 = 0.25;

type Score = f64;
pub type ArcNode = Arc<AtomicRefCell<SortedSetNode>>;

#[derive(Default, Clone, Debug)]
pub struct SortedSetLevel {
    forward: Option<ArcNode>,
    span: usize,
}

#[derive(Default, Clone, Debug)]
pub struct SortedSetNode {
    pub key: String,
    pub value: Bytes,
    pub score: Score,
    backward: Option<ArcNode>,
    level: Vec<SortedSetLevel>,
}

#[derive(Default, Clone, Debug)]
pub struct SortedSet {
    header: ArcNode,
    tail: Option<ArcNode>,
    length: usize,
    level: usize,
    dict: HashMap<String, ArcNode>,
}

fn new_sortedset_node(level: usize, score: Score, key: &str, value: Bytes) -> ArcNode {
    let node = SortedSetNode {
        key: key.to_string(),
        value,
        score,
        backward: None,
        level: vec![SortedSetLevel::default(); level],
    };

    Arc::new(AtomicRefCell::new(node))
}

fn random_level() -> usize {
    let mut level = 1;
    let mut rng = rand::thread_rng();
    while rng.gen::<f64>() < SKIPLISTP && level < SKIPLISTMAXLEVEL {
        level += 1
    }
    level
}

impl SortedSet {
    pub fn new() -> SortedSet {
        let header = new_sortedset_node(SKIPLISTMAXLEVEL, 0.0, "", Bytes::default());
        SortedSet {
            header,
            tail: None,
            length: 0,
            level: 1,
            dict: HashMap::new(),
        }
    }

    pub fn put(&mut self, key: &str, value: Bytes, score: Score) -> usize {
        let mut need_del = false;
        if let Some(item) = self.dict.get(key) {
            let mut item_mut = item.borrow_mut();
            if item_mut.score == score {
                item_mut.value = value;
                return 1;
            }
            need_del = true;
        }

        if need_del {
            self.delete_node(key, score);
        }
        let new_node = self.insert_sortedset_node(key, value, score);
        self.dict.insert(key.into(), new_node);

        1
    }

    pub fn remove(&mut self, key: &str) -> Option<ArcNode> {
        let mut need_del = false;
        let mut res = None;
        let mut found_score = 0.0;
        if let Some(node) = self.dict.get(key) {
            need_del = true;
            res = Some(Arc::clone(node));
            found_score = node.borrow().score;
        }

        if need_del {
            self.delete_node(key, found_score);
        }

        res
    }

    pub fn get_by_rank_range(&mut self, start: usize, end: usize, remove: bool) -> Vec<ArcNode> {
        let mut start = start;
        let mut end = end;
        if start < 1 {
            start = 1;
        }
        if end < start {
            end = start;
        }

        let mut update: Vec<ArcNode> = vec![self.header.clone(); SKIPLISTMAXLEVEL];
        let mut res: Vec<ArcNode> = vec![];

        let mut traversed: usize = 0;
        let mut x = Arc::clone(&self.header);
        for i in (0..self.level).rev() {
            loop {
                let next_node: Arc<AtomicRefCell<SortedSetNode>>;
                if let Some(ref forward) = x.borrow().level[i].forward {
                    let x_b = x.borrow();

                    if traversed + x_b.level[i].span >= start {
                        if remove {
                            update[i] = Arc::clone(&x);
                        }
                        break;
                    }

                    traversed += x_b.level[i].span;
                    next_node = Arc::clone(forward);
                } else {
                    update[i] = Arc::clone(&x);
                    break;
                }
                x = Arc::clone(&next_node);
            }

            if traversed + 1 == start && !remove {
                break;
            }
        }

        traversed += 1;
        let mut next: Arc<AtomicRefCell<SortedSetNode>>;
        loop {
            if let Some(ref node) = x.borrow().level[0].forward {
                if traversed > end {
                    break;
                }
                res.push(Arc::clone(node));
                next = Arc::clone(node);
            } else {
                break;
            }
            if remove {
                self.delete_sortedset_node(Arc::clone(&next), &mut update);
            }
            traversed += 1;
            x = next;
        }

        res
    }

    pub fn get_by_rank(&mut self, rank: usize, remove: bool) -> Option<ArcNode> {
        if rank > self.length {
            return None;
        }
        let nodes = self.get_by_rank_range(rank, rank, remove);
        match nodes.len() {
            1 => Some(Arc::clone(&nodes[0])),
            _ => None,
        }
    }

    pub fn get_by_key(&self, key: &str) -> Option<ArcNode> {
        self.dict.get(key).map(Arc::clone)
    }

    pub fn find_rank(&self, key: &str) -> Option<usize> {
        match self.dict.get(key) {
            Some(node) => {
                let mut rank: usize = 0;

                let mut x = Arc::clone(&self.header);
                for i in (0..self.level).rev() {
                    let mut next_node: Arc<AtomicRefCell<SortedSetNode>>;
                    loop {
                        if let Some(ref forward) = x.borrow().level[i].forward {
                            next_node = Arc::clone(forward);
                            let next_node_borrow = next_node.borrow();
                            let node_b = node.borrow();
                            if next_node_borrow.score < node_b.score
                                || (next_node_borrow.score == node_b.score
                                    && next_node_borrow.key.as_str() <= key)
                            {
                                rank += x.borrow().level[i].span;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                        x = Arc::clone(&next_node);
                    }
                    if x.borrow().key == node.borrow().key {
                        return Some(rank);
                    }
                }

                None
            }
            None => None,
        }
    }

    pub fn find_rev_rank(&self, key: &str) -> Option<usize> {
        self.find_rank(key).map(|rank| self.length() - rank + 1)
    }

    pub fn get_by_score_range(
        &self,
        start: Score,
        end: Score,
        limit: usize,
        exclude_start: bool,
        exclude_end: bool,
    ) -> Vec<ArcNode> {
        if self.length() == 0 {
            return vec![];
        }

        let mut limit = limit;
        let max_limit = (1 << 31) - 1;
        if limit > max_limit {
            limit = max_limit;
        }
        if start > end {
            let mut res = self.search_reverse(start, end, limit, exclude_start, exclude_end);
            res.reverse();
            res
        } else {
            self.search_forward(start, end, limit, exclude_start, exclude_end)
        }
    }

    fn insert_sortedset_node(&mut self, key: &str, value: Bytes, score: Score) -> ArcNode {
        let mut rank = vec![0; SKIPLISTMAXLEVEL];
        let mut update: Vec<ArcNode> = vec![self.header.clone(); SKIPLISTMAXLEVEL];
        let mut x = Arc::clone(&self.header);
        (0..self.level).rev().into_iter().for_each(|i| {
            rank[i] = if self.level - 1 == i { 0 } else { rank[i + 1] };
            let mut next_node: Arc<AtomicRefCell<SortedSetNode>>;
            loop {
                if let Some(ref forward) = x.borrow().level[i].forward {
                    next_node = Arc::clone(&forward);
                    let next_node_borrow = next_node.borrow();
                    if next_node_borrow.score > score
                        || (next_node_borrow.score == score && next_node_borrow.key.as_str() >= key)
                    {
                        break;
                    }
                    rank[i] += x.borrow().level[i].span;
                } else {
                    break;
                }
                x = Arc::clone(&next_node);
            }
            update[i] = x.clone();
        });

        let level = random_level();
        let header = Arc::clone(&self.header);
        if level > self.level {
            let mut header_mut = header.borrow_mut();
            for i in self.level..level {
                header_mut.level[i].span = self.length;
                update[i] = header.clone();
            }
            self.level = level;
        }

        let x = new_sortedset_node(level, score, key, value);
        for i in 0..level {
            let mut level_update = update[i].borrow_mut();
            let mut x_mut = x.borrow_mut();

            x_mut.level[i].forward = level_update.level[i].forward.clone();
            level_update.level[i].forward = Some(Arc::clone(&x));
            x_mut.level[i].span = level_update.level[i].span - (rank[0] - rank[i]);
            level_update.level[i].span = (rank[0] - rank[i]) + 1;
        }

        for i in level..self.level {
            update[i].borrow_mut().level[i].span += 1;
        }

        if Arc::ptr_eq(&update[0], &self.header) {
            x.borrow_mut().backward = None;
        } else {
            x.borrow_mut().backward = Some(Arc::clone(&update[0]));
        }

        if let Some(ref forward) = x.borrow().level[0].forward {
            forward.borrow_mut().backward = Some(Arc::clone(&x));
        } else {
            self.tail = Some(Arc::clone(&x));
        }

        self.length += 1;

        Arc::clone(&x)
    }

    fn delete_node(&mut self, key: &str, score: Score) -> Option<bool> {
        let mut update: Vec<ArcNode> = vec![self.header.clone(); SKIPLISTMAXLEVEL];
        let mut x = Arc::clone(&self.header);
        for i in (0..self.level).rev() {
            let mut next_node: Arc<AtomicRefCell<SortedSetNode>>;
            loop {
                if let Some(ref forward) = x.borrow().level[i].forward {
                    next_node = Arc::clone(forward);
                    let next_node_borrow = next_node.borrow();

                    if next_node_borrow.score > score
                        || (next_node_borrow.score == score && next_node_borrow.key.as_str() >= key)
                    {
                        break;
                    }
                } else {
                    break;
                }
                x = Arc::clone(&next_node);
            }
            update[i] = Arc::clone(&x);
        }

        let mut del_node = ArcNode::default();
        let mut need_del = false;
        if let Some(ref forward) = x.borrow().level[0].forward {
            let forward_borrow = forward.borrow();
            if forward_borrow.score == score && forward_borrow.key == key {
                need_del = true;
                del_node = Arc::clone(forward);
            }
        }

        if need_del {
            self.delete_sortedset_node(del_node, &mut update);
            return Some(true);
        }
        None
    }

    fn delete_sortedset_node(&mut self, node: ArcNode, update: &mut [ArcNode]) -> Option<ArcNode> {
        for i in 0..self.level {
            let mut update_i_mut = update[i].borrow_mut();
            if update_i_mut.level[i].forward.is_some()
                && Arc::ptr_eq(update_i_mut.level[i].forward.as_ref().unwrap(), &node)
            {
                update_i_mut.level[i].span += node.borrow().level[i].span - 1;
                update_i_mut.level[i].forward = node.borrow().level[i].forward.clone()
            } else {
                update_i_mut.level[i].span -= 1;
            }
        }

        let node_borrow = node.borrow();

        if node_borrow.level[0].forward.is_some() {
            node_borrow.level[0]
                .forward
                .as_ref()
                .unwrap()
                .borrow_mut()
                .backward = node_borrow.backward.clone()
        } else {
            self.tail = node_borrow.backward.clone();
        }

        loop {
            if self.level <= 1 || self.header.borrow().level[self.level - 1].forward.is_some() {
                break;
            }
            self.level -= 1;
        }
        self.length -= 1;
        self.dict.remove(&node_borrow.key);
        None
    }

    pub fn length(&self) -> usize {
        self.length
    }

    fn search_forward(
        &self,
        start: Score,
        end: Score,
        limit: usize,
        exclude_start: bool,
        exclude_end: bool,
    ) -> Vec<ArcNode> {
        let mut x = Arc::clone(&self.header);
        for i in (0..self.level).rev() {
            let mut next_node: Arc<AtomicRefCell<SortedSetNode>>;
            loop {
                if let Some(ref forward) = x.borrow().level[i].forward {
                    next_node = Arc::clone(forward);
                    let next_node_borrow = next_node.borrow();
                    if (next_node_borrow.score >= start && !exclude_start)
                        || (next_node_borrow.score > start && exclude_start)
                    {
                        break;
                    }
                } else {
                    break;
                }
                x = Arc::clone(&next_node);
            }
        }

        let mut next_node: Option<Arc<AtomicRefCell<SortedSetNode>>>;
        let mut x = x.borrow().level[0].forward.clone();

        let mut res: Vec<ArcNode> = vec![];
        let mut limit = limit;
        loop {
            if let Some(ref current) = x {
                if limit == 0 {
                    break;
                }
                let current_b = current.borrow();
                if (exclude_end && current_b.score >= end)
                    || (!exclude_end && current_b.score > end)
                {
                    break;
                }
                res.push(Arc::clone(current));
                limit -= 1;
                next_node = current_b.level[0].forward.clone();
            } else {
                break;
            }

            x = next_node.clone();
        }

        res
    }

    fn search_reverse(
        &self,
        start: Score,
        end: Score,
        limit: usize,
        exclude_start: bool,
        exclude_end: bool,
    ) -> Vec<ArcNode> {
        self.search_forward(end, start, limit, exclude_end, exclude_start)
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use super::SortedSet;
    use super::Score;

    #[test]
    fn test_put_remove() {
        let mut sortedset = SortedSet::new();

        sortedset.put("key1", Bytes::from("value1"), 1.0);
        sortedset.put("key2", Bytes::from("value2"), 2.0);
        sortedset.put("key3", Bytes::from("value3"), 3.0);
        assert_eq!(sortedset.length(), 3);

        assert!(sortedset.dict.get("key1").unwrap().borrow().key == "key1");
        assert!(sortedset.dict.get("key1").unwrap().borrow().value == "value1");
        assert!(sortedset.dict.get("key1").unwrap().borrow().score == 1.0);

        assert!(sortedset.dict.contains_key("key2"));
        let remove = sortedset.remove("key2");
        assert_eq!(remove.as_ref().unwrap().borrow().key, "key2");
        assert_eq!(remove.as_ref().unwrap().borrow().value, "value2");
        assert_eq!(remove.as_ref().unwrap().borrow().score, 2.0);

        assert!(sortedset.remove("key5").is_none());
    }

    #[test]
    fn test_get_by_rank_range() {
        let mut sortedset = SortedSet::new();
        sortedset.put("key1", Bytes::from("value1"), 1.0);
        sortedset.put("key2", Bytes::from("value2"), 2.0);
        sortedset.put("key3", Bytes::from("value3"), 3.0);
        sortedset.put("key4", Bytes::from("value4"), 4.0);
        sortedset.put("key5", Bytes::from("value5"), 5.0);
        sortedset.put("key6", Bytes::from("value6"), 6.0);
        sortedset.put("key0.5", Bytes::from("value1.5"), 0.5);
        sortedset.put("key0.7", Bytes::from("value0.5"), 0.7);
        let nodes = sortedset.get_by_rank_range(2, 5, false);
        assert_eq!(nodes.len(), 4);
        assert_eq!(nodes[0].borrow().key, "key0.7");
        assert_eq!(nodes[1].borrow().key, "key1");

        let mut sortedset = SortedSet::new();
        let iters = 1000;
        for i in 0..iters {
            sortedset.put(
                format!("key{}", i).as_str(),
                Bytes::from(format!("value{}", i)),
                i as f64,
            );
        }
        let range = sortedset.get_by_rank_range(1, iters, false);

        range
            .iter()
            .enumerate()
            .for_each(|(i, n)| assert_eq!(n.borrow().score, i as Score));
    }

    #[test]
    fn test_get_by_rank() {
        let mut sortedset = SortedSet::new();
        sortedset.put("key1", Bytes::from("value1"), 1.0);
        sortedset.put("key2", Bytes::from("value2"), 2.0);
        sortedset.put("key3", Bytes::from("value3"), 3.0);
        sortedset.put("key0.5", Bytes::from("value0.5"), 0.5);
        sortedset.put("key0.7", Bytes::from("value0.7"), 0.7);

        let node = sortedset.get_by_rank(2, false);
        assert!(node.is_some());
        assert_eq!(node.as_ref().unwrap().borrow().key, "key0.7");
        let node = sortedset.get_by_rank(3, true);
        assert!(node.is_some());
        assert_eq!(node.as_ref().unwrap().borrow().key, "key1");
        assert_eq!(node.as_ref().unwrap().borrow().value, "value1");
        assert_eq!(node.as_ref().unwrap().borrow().score, 1.0);
        assert!(sortedset.dict.get("key1").is_none());
        let node = sortedset.get_by_rank(3, false);
        assert!(node.is_some());
        assert_eq!(node.as_ref().unwrap().borrow().key, "key2");
        assert_eq!(node.as_ref().unwrap().borrow().value, "value2");
        assert_eq!(node.as_ref().unwrap().borrow().score, 2.0);
        let node = sortedset.get_by_rank(6, false);
        assert!(node.is_none());
    }

    #[test]
    fn test_get_by_key() {
        let mut sortedset = SortedSet::new();
        sortedset.put("key1", Bytes::from("value1"), 1.0);
        sortedset.put("key2", Bytes::from("value2"), 2.0);
        sortedset.put("key3", Bytes::from("value3"), 3.0);
        let node = sortedset.get_by_key("key2");
        assert!(node.is_some());
        assert_eq!(node.unwrap().borrow().score, 2.0);
    }

    #[test]
    fn test_find_rank() {
        let mut sortedset = SortedSet::new();
        sortedset.put("key1", Bytes::from("value1"), 1.0);
        sortedset.put("key2", Bytes::from("value2"), 2.0);
        sortedset.put("key3", Bytes::from("value3"), 3.0);
        let rank = sortedset.find_rank("key2");
        assert!(rank.is_some());
        assert_eq!(rank.unwrap(), 2);
        let rank = sortedset.find_rank("key5");
        assert!(rank.is_none());
    }

    #[test]
    fn test_find_rev_rank() {
        let mut sortedset = SortedSet::new();
        sortedset.put("key1", Bytes::from("value1"), 1.0);
        sortedset.put("key2", Bytes::from("value2"), 2.0);
        sortedset.put("key3", Bytes::from("value3"), 3.0);

        let rev_rank = sortedset.find_rev_rank("key2");
        assert!(rev_rank.is_some());
        assert_eq!(rev_rank.unwrap(), 2);
        let rank = sortedset.find_rev_rank("key5");
        assert!(rank.is_none());
        let rev_rank = sortedset.find_rev_rank("key3");
        assert!(rev_rank.is_some());
        assert_eq!(rev_rank.unwrap(), 1);
    }

    #[test]
    fn test_get_by_score_range() {
        let mut sortedset = SortedSet::new();
        sortedset.put("key1", Bytes::from("value1"), 1.0);
        sortedset.put("key2", Bytes::from("value2"), 2.0);
        sortedset.put("key3", Bytes::from("value3"), 3.0);
        sortedset.put("key4", Bytes::from("value4"), 4.0);
        let nodes = sortedset.get_by_score_range(1.0, 3.0, 2, false, false);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].borrow().key, "key1");
        assert_eq!(nodes[1].borrow().key, "key2");

        let nodes = sortedset.get_by_score_range(1.0, 3.0, 3, false, false);
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].borrow().key, "key1");
        assert_eq!(nodes[1].borrow().key, "key2");

        let nodes = sortedset.get_by_score_range(1.0, 3.0, 3, true, false);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].borrow().key, "key2");
        assert_eq!(nodes[1].borrow().key, "key3");

        let nodes = sortedset.get_by_score_range(1.0, 3.0, 3, false, true);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].borrow().key, "key1");
        assert_eq!(nodes[1].borrow().key, "key2");

        let nodes = sortedset.get_by_score_range(1.0, 5.0, 10, false, true);
        assert_eq!(nodes.len(), 4);
        assert_eq!(nodes[0].borrow().key, "key1");
        assert_eq!(nodes[1].borrow().key, "key2");

        let nodes = sortedset.get_by_score_range(3.0, 1.0, 10, true, false);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].borrow().key, "key2");
        assert_eq!(nodes[1].borrow().key, "key1");
    }
}

// fn zadd(key: &str, score: Score, value: Bytes) -> Result<usize> {
//     Ok(0)
// }

// fn zcount(key: &str, min: Score, max: Score) -> Result<usize> {
//     Ok(0)
// }

// fn zincryby(key: &str, increment: Score, value: Bytes) -> Result<Score> {
//     Ok(0.0)
// }

// fn zpopmax(key: &str) -> Result<(Bytes, Score)> {
//     Ok((Bytes::new(), 0.0))
// }

// fn zpopmin(key: &str) -> Result<(Bytes, Score)> {
//     Ok((Bytes::new(), 0.0))
// }

// fn zrange(key: &str, start: isize, end: isize) -> Result<Vec<Bytes>> {
//     Ok(vec![])
// }

// fn zrange_by_score(key: &str, start: isize, end: isize) -> Result<Vec<Bytes>> {
//     Ok(vec![])
// }

// fn zrank(key: &str, value: Bytes) -> Result<usize> {
//     Ok(0)
// }

// fn zrem(key: &str, value: Bytes) -> Result<usize> {
//     Ok(0)
// }

// fn zscore(key: &str, value: Bytes) -> Result<Score> {
//     Ok(0.0)
// }

// fn zmscore(key: &str, values: Vec<Bytes>) -> Result<Vec<Score>> {
//     Ok(vec![])
// }

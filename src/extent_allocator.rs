use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

//----------------------------------------------------------------

const NULL_NODE: u8 = 255;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
struct Internal {
    holders: usize,
    nr_free_blocks: u64,
    cut: u64,
    left: u8,
    right: u8,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
struct Extent {
    begin: u64,
    end: u64,
    cursor: u64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct Leaf {
    extent: Arc<Mutex<Extent>>,
    holders: usize,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
enum Node {
    Internal(Internal),
    Leaf(Leaf),
}

struct Tree {
    nr_blocks: u64,
    nodes: Vec<Node>,
    free_nodes: Vec<u8>,
    root: u8,
}

fn dump_tree(tree: &Tree) {
    let mut stack = vec![(tree.root, 0)];
    while let Some((node_index, indent)) = stack.pop() {
        let pad = (0..indent).map(|_| ' ').collect::<String>();

        if node_index == NULL_NODE {
            println!("{}NULL", pad);
            continue;
        }

        let node = tree.read_node(node_index);
        match node {
            Node::Internal(node) => {
                println!(
                    "{}Internal: cut={} holders={} nr_free={}",
                    pad, node.cut, node.holders, node.nr_free_blocks
                );
                stack.push((node.right, indent + 8));
                stack.push((node.left, indent + 8));
            }

            Node::Leaf(node) => {
                let extent = node.extent.lock().unwrap();
                println!(
                    "{}Leaf: b={} e={} cursor={} holders={}",
                    pad, extent.begin, extent.end, extent.cursor, node.holders,
                );
            }
        }
    }
}

fn char_run(c: char, fraction: f64, width: usize) -> String {
    let nr_chars = (fraction * width as f64) as usize;
    let mut s = String::new();
    for _ in 0..nr_chars {
        s.push(c);
    }
    s
}

fn draw_tree(tree: &Tree) {
    let width = 100;
    let mut deque = VecDeque::new();
    deque.push_back((tree.root, 0, tree.nr_blocks, 0, '-'));

    let mut cursor = 0;
    let mut last_level = None;
    while let Some((node_index, begin, end, level, c)) = deque.pop_front() {
        match (last_level, level) {
            (None, level) => {
                cursor = 0;
                last_level = Some(level);
            }
            (Some(last), level) => {
                if last != level {
                    println!();
                    cursor = 0;
                    last_level = Some(level);
                }
            }
        }

        // Print any padding we need.
        if begin > cursor {
            print!(
                "{} ",
                char_run(' ', (begin - cursor) as f64 / tree.nr_blocks as f64, width)
            );
        }

        let node = tree.read_node(node_index);
        match node {
            Node::Internal(node) => {
                // Print the node.
                print!(
                    "{}",
                    char_run(c, (end - begin) as f64 / tree.nr_blocks as f64, width)
                );

                cursor = end;
                if node.left != NULL_NODE {
                    deque.push_back((node.left, begin, node.cut, level + 1, '/'));
                }
                if node.right != NULL_NODE {
                    deque.push_back((node.right, node.cut, end, level + 1, '\\'));
                }
            }

            Node::Leaf(_node) => {
                // Print the node.
                print!(
                    "{}",
                    char_run(c, (end - begin) as f64 / tree.nr_blocks as f64, width)
                );

                cursor = end;
            }
        }
    }
    println!();
}

impl Node {
    fn nr_holders(&self) -> usize {
        match self {
            Node::Internal(node) => node.holders,
            Node::Leaf(node) => node.holders,
        }
    }

    fn nr_free_blocks(&self) -> u64 {
        match self {
            Node::Internal(node) => node.nr_free_blocks,
            Node::Leaf(node) => {
                let extent = node.extent.lock().unwrap();
                extent.end - extent.cursor
            }
        }
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::Internal(Internal {
            holders: 0,
            nr_free_blocks: 0,
            cut: 0,
            left: NULL_NODE,
            right: NULL_NODE,
        })
    }
}

impl Tree {
    fn new(nr_blocks: u64, nr_nodes: u8) -> Self {
        // assert!(nr_nodes <= NULL_NODE);
        let free_nodes = (0u8..nr_nodes).into_iter().collect::<Vec<u8>>();
        let mut tree = Tree {
            nr_blocks,
            nodes: vec![Node::default(); nr_nodes as usize],
            free_nodes,
            root: NULL_NODE,
        };

        tree.root = tree.alloc_node().unwrap();
        tree.nodes[tree.root as usize] = Node::Leaf(Leaf {
            extent: Arc::new(Mutex::new(Extent {
                begin: 0,
                end: nr_blocks,
                cursor: 0,
            })),
            holders: 0,
        });

        tree
    }

    fn alloc_node(&mut self) -> Option<u8> {
        self.free_nodes.pop()
    }

    fn free_node(&mut self, node: u8) {
        self.free_nodes.push(node);
    }

    fn read_node(&self, node: u8) -> Node {
        self.nodes[node as usize].clone()
    }

    fn write_node(&mut self, node: u8, node_data: Node) {
        self.nodes[node as usize] = node_data;
    }

    fn split_leaf(&mut self, node_index: u8) -> bool {
        if self.free_nodes.len() < 2 {
            return false;
        }

        let node = self.read_node(node_index);
        match node {
            Node::Internal(_) => panic!("split_leaf called on internal node"),
            Node::Leaf(leaf) => {
                // We copy the extent, because we're about to adjust it and
                // reuse for one of the children.
                let mut extent = leaf.extent.lock().unwrap();

                if extent.end - extent.cursor <= 16 {
                    // We can't split this leaf, because it's too small
                    return false;
                }

                let copy = extent.clone();
                let mid = extent.cursor + (extent.end - extent.cursor) / 2;
                extent.end = mid;
                drop(extent);

                let left_child = self.alloc_node().unwrap();
                let right_child = self.alloc_node().unwrap();

                self.write_node(
                    left_child,
                    Node::Leaf(Leaf {
                        extent: leaf.extent.clone(),
                        holders: leaf.holders,
                    }),
                );
                self.write_node(
                    right_child,
                    Node::Leaf(Leaf {
                        extent: Arc::new(Mutex::new(Extent {
                            begin: mid,
                            end: copy.end,
                            cursor: mid,
                        })),
                        holders: 0,
                    }),
                );

                // Now turn the old leaf into an internal node
                let nr_holders = leaf.holders;
                self.write_node(
                    node_index,
                    Node::Internal(Internal {
                        cut: mid,
                        holders: nr_holders,
                        nr_free_blocks: copy.end - copy.cursor,
                        left: left_child,
                        right: right_child,
                    }),
                );
            }
        }
        true
    }

    // Select a child to borrow, based on the nr of holders and the nr of free blocks
    fn select_child(&self, left: u8, right: u8) -> u8 {
        assert!(left != NULL_NODE);
        assert!(right != NULL_NODE);

        let left_node = self.read_node(left);
        let right_node = self.read_node(right);

        let left_holders = left_node.nr_holders();
        let left_free = left_node.nr_free_blocks();
        let left_score = left_free / (left_holders + 1) as u64;

        let right_holders = right_node.nr_holders();
        let right_free = right_node.nr_free_blocks();
        let right_score = right_free / (right_holders + 1) as u64;

        if left_score >= right_score {
            left
        } else {
            right
        }
    }

    fn borrow_(&mut self, node_index: u8) -> Option<Arc<Mutex<Extent>>> {
        if node_index == NULL_NODE {
            return None;
        }

        let node = self.read_node(node_index);
        match node {
            Node::Internal(node) => {
                let extent = match (node.left, node.right) {
                    (255, 255) => panic!("node with two NULLs shouldn't be possible"),
                    (255, right) => self.borrow_(right),
                    (left, 255) => self.borrow_(left),
                    (left, right) => self.borrow_(self.select_child(left, right)),
                };

                if extent.is_some() {
                    self.write_node(
                        node_index,
                        Node::Internal(Internal {
                            cut: node.cut,
                            holders: node.holders + 1,
                            nr_free_blocks: node.nr_free_blocks,
                            left: node.left,
                            right: node.right,
                        }),
                    );
                }
                return extent;
            }

            Node::Leaf(node) => {
                if node.holders > 0 {
                    // Someone is already using this extent.  See if we can split it.
                    if self.split_leaf(node_index) {
                        // Try again, now that this node is an internal node
                        return self.borrow_(node_index);
                    } else {
                        // We can't split the leaf, so we'll have to share.
                        self.write_node(
                            node_index,
                            Node::Leaf(Leaf {
                                extent: node.extent.clone(),
                                holders: node.holders + 1,
                            }),
                        );
                        return Some(node.extent.clone());
                    }
                } else {
                    // No one is using this extent, so we can just take it.
                    self.write_node(
                        node_index,
                        Node::Leaf(Leaf {
                            extent: node.extent.clone(),
                            holders: node.holders + 1,
                        }),
                    );
                    return Some(node.extent.clone());
                }
            }
        }
    }

    // Returns a region that has some free blocks.  This can
    // cause existing regions to be altered as new splits are
    // introduced to the BSP tree.
    fn borrow(&mut self) -> Option<Arc<Mutex<Extent>>> {
        self.borrow_(self.root)
    }

    fn nr_free(&self, node_index: u8) -> u64 {
        if node_index == NULL_NODE {
            return 0;
        }

        let node = self.read_node(node_index);
        node.nr_free_blocks()
    }

    // Returns the node_index of the replacement for this node (commonly the same as node_index)
    fn release_(&mut self, block: u64, begin: u64, end: u64, node_index: u8) -> u8 {
        if node_index == NULL_NODE {
            return node_index;
        }

        let node = self.read_node(node_index);

        match node {
            Node::Internal(node) => {
                assert!(node.holders > 0);
                let mut left = node.left;
                let mut right = node.right;

                // FIXME: refactor
                if block < node.cut {
                    left = self.release_(block, begin, node.cut, node.left);
                } else {
                    right = self.release_(block, node.cut, end, node.right);
                }

                if left == NULL_NODE && right == NULL_NODE {
                    // Both children are NULL, so we can free this node
                    self.free_node(node_index);
                    return NULL_NODE;
                } else {
                    self.write_node(
                        node_index,
                        Node::Internal(Internal {
                            cut: node.cut,
                            holders: node.holders - 1,
                            nr_free_blocks: self.nr_free(left) + self.nr_free(right),
                            left,
                            right,
                        }),
                    );

                    return node_index;
                }
            }

            Node::Leaf(node) => {
                assert!(node.holders > 0);

                // See if the extent is now empty
                let extent = node.extent.lock().unwrap();
                let full = extent.cursor == extent.end;
                drop(extent);

                if full {
                    // The extent is now empty, so we can free this node
                    self.free_node(node_index);
                    return NULL_NODE;
                } else {
                    self.write_node(
                        node_index,
                        Node::Leaf(Leaf {
                            extent: node.extent.clone(),
                            holders: node.holders - 1,
                        }),
                    );
                    return node_index;
                }
            }
        }
    }

    fn release(&mut self, extent: Arc<Mutex<Extent>>) {
        // eprintln!("before release:");
        // dump_tree(&self.root, 0);

        let extent = extent.lock().unwrap();
        let b = extent.begin;
        drop(extent);

        self.root = self.release_(b, 0, self.nr_blocks, self.root);

        // eprintln!("after release:");
        // dump_tree(&self.root, 0);
    }

    fn reset(&mut self) {
        todo!()
    }
}

//----------------------------------------------------------------

/*
struct Allocator {
    nr_blocks: u64,
    allocated: Mutex<RoaringBitmap>,
    extents: Tree,
}

impl Allocator {
    fn new(nr_blocks: u64, nr_nodes: u8) -> Self {
        // Create a tree that brackets the entire address space
        let extents = Tree::new(nr_blocks, nr_nodes);

        Allocator {
            nr_blocks,
            allocated: Mutex::new(RoaringBitmap::new()),
            extents,
        }
    }

    fn preallocate_random(&mut self, count: u64) {
        let mut allocated = self.allocated.lock().unwrap();
        for _ in 0..count {
            loop {
                let block = rand::random::<u64>() % self.nr_blocks;
                if !allocated.contains(block as u32) {
                    allocated.insert(block as u32);
                    break;
                }
            }
        }
    }

    // FIXME: try with an offset
    fn preallocate_linear(&mut self, count: u64, offset: u64) {
        assert!(offset + count <= self.nr_blocks);
        let mut allocated = self.allocated.lock().unwrap();
        for block in 0..count {
            allocated.insert((offset + block) as u32);
        }
    }

    fn get_extent(&mut self) -> Option<Arc<Mutex<Extent>>> {
        self.extents.borrow()
    }

    fn put_extent(&mut self, extent: Arc<Mutex<Extent>>) {
        self.extents.release(extent);
    }

    fn alloc(&mut self, extent: Arc<Mutex<Extent>>) -> Option<u64> {
        let mut extent = extent.lock().unwrap();
        let mut allocated = self.allocated.lock().unwrap();

        for block in extent.cursor..extent.end {
            if allocated.contains(block as u32) {
                continue;
            }

            allocated.insert(block as u32);
            extent.cursor = extent.cursor + 1;
            return Some(block as u64);
        }

        extent.cursor = extent.end;
        None
    }
}

//----------------------------------------------------------------

struct AllocContext {
    extent: Option<Arc<Mutex<Extent>>>,
    blocks: Vec<u64>,
}

fn to_runs(blocks: &[u64]) -> Vec<(u64, u64)> {
    let mut runs = Vec::new();
    let mut begin = blocks[0];
    let mut end = begin;
    for &block in blocks.iter().skip(1) {
        if block == end + 1 {
            end = block;
        } else {
            runs.push((begin, end));
            begin = block;
            end = block;
        }
    }
    runs.push((begin, end));
    runs
}

fn print_blocks(blocks: &[u64]) {
    let runs = to_runs(blocks);
    let mut first = true;
    print!("[");
    for (begin, end) in runs {
        if first {
            first = false;
        } else {
            print!(", ");
        }
        if begin == end {
            print!("{}", begin);
        } else {
            print!("{}..{}", begin, end);
        }
    }
    println!("]");
}

fn main() {
    // Check we can handle a non-power-of-two number of blocks
    let nr_blocks = 1024;
    let nr_nodes = 255;
    let nr_allocators = 16;

    let mut allocator = Allocator::new(nr_blocks, nr_nodes);
    allocator.preallocate_linear(nr_blocks / 5, 100);

    let mut contexts = Vec::new();
    for _i in 0..nr_allocators {
        contexts.push(AllocContext {
            extent: allocator.get_extent(),
            blocks: Vec::new(),
        });
    }

    for i in 0..(nr_blocks / 2) {
        let mut context = &mut contexts[(i % nr_allocators) as usize];
        loop {
            let extent = context.extent.as_ref().unwrap().clone();
            let block = allocator.alloc(extent.clone());
            if let Some(block) = block {
                context.blocks.push(block);
                break;
            }

            allocator.put_extent(extent.clone());
            context.extent = allocator.get_extent();
        }
    }

    //   dump_tree(&allocator.extents);
    //   draw_tree(&allocator.extents);

    for context in &mut contexts {
        let extent = context.extent.take();

        allocator.put_extent(extent.unwrap());
        print_blocks(&context.blocks);
    }

    dump_tree(&allocator.extents);
    draw_tree(&allocator.extents);
}
*/

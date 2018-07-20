extern crate petgraph;
use petgraph::graph::NodeIndex;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;

mod domains;
mod materialization;
mod network;
mod partial;
mod sharding;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ReplayPathTag(u32);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Sharding {
    None {
        forced: bool,
    },
    ByColumn {
        origin: (NodeIndex, usize),
        column: usize,
    },
    Shuffled,
}

#[derive(Clone, Debug)]
pub enum Operator<F> {
    Reader,
    Sharder,
    Egress,
    Ingress,
    Inner(F),
}

#[derive(Clone, Debug)]
pub struct DataflowNode<F> {
    operator: Operator<F>,
    indices: HashSet<Vec<usize>>, // TODO: HashSet<ArrayVec>
    partial: bool,
    src_for: HashSet<ReplayPathTag>,
    dst_for: HashSet<ReplayPathTag>,
    sharding: Sharding,
}

impl<F> Deref for DataflowNode<F> {
    type Target = Operator<F>;
    fn deref(&self) -> &Self::Target {
        &self.operator
    }
}

impl<F> From<F> for DataflowNode<F> {
    fn from(f: F) -> Self {
        DataflowNode {
            operator: Operator::Inner(f),
            indices: Default::default(),
            partial: false,
            src_for: Default::default(),
            dst_for: Default::default(),
            sharding: Sharding::None { forced: false },
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataflowGraph<F> {
    graph: petgraph::Graph<DataflowNode<F>, ()>,
}

impl<F> Default for DataflowGraph<F> {
    fn default() -> Self {
        DataflowGraph {
            graph: petgraph::Graph::default(),
        }
    }
}

pub trait DataflowOperator: fmt::Debug {
    type AncestryIter: Iterator<Item = (NodeIndex, usize)>;

    fn resolve(&self, column: usize) -> Self::AncestryIter;
    fn ancestors(&self) -> Cow<[NodeIndex]>;
}

impl<F> DataflowGraph<F>
where
    F: DataflowOperator + Clone,
{
    /// Plan what changes must be made to bring the graph state at the previous call to `plan()` in
    /// line with the current graph state.
    pub fn plan_transition(&mut self) -> Plan<F> {
        Plan::new(self)
    }

    #[cfg(test)]
    pub fn graph(&self) -> &petgraph::Graph<DataflowNode<F>, ()> {
        &self.graph
    }
}

#[derive(Debug)]
pub struct Plan<'a, F: 'a> {
    df: &'a mut DataflowGraph<F>,
    next_graph: petgraph::Graph<DataflowNode<F>, ()>,
    new: HashSet<NodeIndex>,
}

impl<'a, F> Plan<'a, F> {
    // TODO: add/remove column -- maybe always done first?
    // TODO: universes

    fn new(df: &'a mut DataflowGraph<F>) -> Self
    where
        F: Clone,
    {
        let next_graph = df.graph.clone();
        Plan {
            df,
            next_graph,
            new: Default::default(),
        }
    }

    /// Add the given node to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added node. Edges in the data
    /// flow graph are automatically added based on the node's reported `ancestors`.
    pub fn add_node<I>(&mut self, f: I) -> NodeIndex
    where
        I: Into<F>,
        F: DataflowOperator,
    {
        // TODO: on_connected
        // TODO: logging

        // TODO: do we do the add work incrementally here, or do we plan everything only once all
        // the nodes have been added? i think probably the latter.

        let f = f.into();
        let parents = f.ancestors().into_owned();
        assert!(!parents.is_empty());

        // add to the graph
        let ni = self.next_graph.add_node(DataflowNode::from(f));

        // keep track of the fact that it's new
        self.new.insert(ni);

        // add connections
        for &parent in &*parents {
            self.next_graph.add_edge(parent, ni, ());
        }

        // and tell the caller its id
        ni
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `ControllerInner::get_getter`.
    pub fn maintain<'k, K>(&mut self, n: NodeIndex, key: K)
    where
        K: Into<Cow<'k, [usize]>>,
    {
        let mut children = self
            .next_graph
            .neighbors_directed(n, petgraph::EdgeDirection::Outgoing)
            .detach();

        while let Some(child) = children.next_node(&self.next_graph) {
            let node = self
                .next_graph
                .node_weight_mut(child)
                .expect("got child node that doesn't exist");

            if let Operator::Reader = **node {
                if node.indices.insert(key.into().into_owned()) {
                    // TODO: logging
                }
                return;
            }
        }
        unimplemented!("need to add reader");
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// Returns the modifications that need to be performed to the running dataflow graph.
    pub fn commit(mut self) -> Vec<Step<F>> {
        self.plan_materialization();
        self.make_partial();
        self.shard();
        self.assign_domains();
        self.network();

        // TODO: use self.next_graph - self.df.graph to make self.steps?
        self.df.graph = self.next_graph;
        Vec::new()
    }

    pub fn proposed_graph(&self) -> &petgraph::Graph<DataflowNode<F>, ()> {
        &self.next_graph
    }
}

pub type PathIndex = u32;
pub type ReplicaIndex = u32;

#[derive(Debug, Clone)]
pub struct PathNode {
    node: NodeIndex,
    key: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum Step<N> {
    SpawnReplica {
        id: ReplicaIndex,
        shards: u32,
    },
    InstallNode {
        id: NodeIndex,
        replica: ReplicaIndex,
        node: N,
    },
    AddNodeIndex {
        replica: ReplicaIndex,
        node: NodeIndex,
        columns: Vec<usize>,
    },
    AnnouncePath {
        id: PathIndex,
        segments: Vec<(ReplicaIndex, Vec<PathNode>)>,
    },
    TriggerFullReplay {
        replica: ReplicaIndex,
        node: NodeIndex,
        path: PathIndex,
    },
    AwaitReplayCompletion {
        path: PathIndex,
    },
    ActivateNode {
        replica: ReplicaIndex,
        node: NodeIndex,
    },
}

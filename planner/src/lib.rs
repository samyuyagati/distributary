extern crate petgraph;
use petgraph::graph::{EdgeIndex, NodeIndex};
use std::fmt;

#[derive(Debug, Clone)]
pub struct DataflowPlanner<N, E> {
    graph: petgraph::Graph<N, E>,
}

impl<N, E> Default for DataflowPlanner<N, E> {
    fn default() -> Self {
        DataflowPlanner {
            graph: petgraph::Graph::default(),
        }
    }
}

use std::ops::Deref;
impl<N, E> Deref for DataflowPlanner<N, E> {
    type Target = petgraph::Graph<N, E>;
    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

// forward mutating petgraph functions that we may want to keep track of
impl<N, E> DataflowPlanner<N, E> {
    pub fn node_weight_mut(&mut self, a: NodeIndex) -> Option<&mut N> {
        self.graph.node_weight_mut(a)
    }

    pub fn add_node(&mut self, weight: N) -> NodeIndex {
        self.graph.add_node(weight)
    }

    pub fn add_edge(&mut self, a: NodeIndex, b: NodeIndex, weight: E) -> EdgeIndex {
        self.graph.add_edge(a, b, weight)
    }

    pub fn update_edge(&mut self, a: NodeIndex, b: NodeIndex, weight: E) -> EdgeIndex {
        self.graph.update_edge(a, b, weight)
    }

    pub fn edge_weight_mut(&mut self, e: EdgeIndex) -> Option<&mut E> {
        self.graph.edge_weight_mut(e)
    }

    pub fn remove_node(&mut self, a: NodeIndex) -> Option<N> {
        self.graph.remove_node(a)
    }

    pub fn remove_edge(&mut self, e: EdgeIndex) -> Option<E> {
        self.graph.remove_edge(e)
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

#[derive(Debug, Clone)]
pub struct Plan<N> {
    steps: Vec<Step<N>>,
}

impl<N> Default for Plan<N> {
    fn default() -> Self {
        Plan {
            steps: Default::default(),
        }
    }
}

pub trait DataflowOperator: fmt::Debug {
    type AncestryIter: Iterator<Item = (NodeIndex, usize)>;

    fn resolve(&self, column: usize) -> Self::AncestryIter;
}

impl<N, E> DataflowPlanner<N, E>
where
    N: DataflowOperator,
{
    /// Plan what changes must be made to bring the graph state at the previous call to `plan()` in
    /// line with the current graph state.
    pub fn plan(&mut self) -> Plan<N> {
        let plan = Plan::default();
        // TODO
        plan
    }
}

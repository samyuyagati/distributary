use petgraph;
use std::cell;

// core types
pub use basics::*;
pub use ops::NodeOperator;
pub use processing::Ingredient;
pub(crate) use processing::{Miss, ProcessingResult, RawProcessingResult, ReplayContext};

// graph types
pub use node::Node;
pub type Edge = ();
pub type Graph = petgraph::Graph<Node, Edge>;

// dataflow types
pub use api::debug::trace::{Event, PacketEvent, Tracer};
pub use api::Input;
pub use payload::{Packet, ReplayPathSegment, SourceChannelIdentifier};
pub use Sharding;

// domain local state
pub use state::{LookupResult, MemoryState, PersistentState, RecordResult, Row, State};
pub type StateMap = map::Map<Box<State>>;
pub type DomainNodes = Map<cell::RefCell<Node>>;
pub type ReplicaAddr = (DomainIndex, usize);

// persistence configuration
pub use DurabilityMode;
pub use PersistenceParameters;

// channel related types
use channel;
/// Channel coordinator type specialized for domains
pub type ChannelCoordinator = channel::ChannelCoordinator<(DomainIndex, usize)>;
pub trait Executor {
    fn send_back(&mut self, SourceChannelIdentifier, ());
}

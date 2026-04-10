/*!
# cuda-fleet-mesh

Fleet mesh networking — the connective tissue of the agent fleet.

Every agent is a node. The mesh provides:
- Service discovery (find agents by capability)
- Health checking (are they alive?)
- Load balancing (distribute work)
- Topology (ring, mesh, tree, star)
- Gossip protocol (propagate state changes)

No central coordinator. The mesh IS the network.
*/

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A node in the fleet mesh
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MeshNode {
    pub id: String,
    pub address: String,
    pub capabilities: Vec<String>,
    pub health: HealthStatus,
    pub load: f64,          // [0,1] current workload
    pub trust_score: f64,   // [0,1] reputation
    pub last_heartbeat: u64,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
    Dead,
}

/// Mesh topology
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Topology {
    Full,     // every node connected to every node
    Ring,     // circular chain
    Star,     // one hub, all others connect to hub
    Tree,     // hierarchical
    Random,   // each node connects to K random neighbors
}

/// The fleet mesh
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FleetMesh {
    pub nodes: HashMap<String, MeshNode>,
    pub edges: HashSet<(String, String)>, // undirected: always store (min, max) alphabetically
    pub topology: Topology,
    pub heartbeat_interval_ms: u64,
    pub dead_threshold_ms: u64,
    pub max_connections_per_node: usize,
}

impl FleetMesh {
    pub fn new() -> Self {
        FleetMesh { nodes: HashMap::new(), edges: HashSet::new(), topology: Topology::Full, heartbeat_interval_ms: 5000, dead_threshold_ms: 30000, max_connections_per_node: 10 }
    }

    pub fn with_topology(topo: Topology) -> Self {
        let mut mesh = Self::new();
        mesh.topology = topo;
        mesh
    }

    /// Register a node
    pub fn register(&mut self, node: MeshNode) {
        let id = node.id.clone();
        self.nodes.insert(id.clone(), node);
        self.rebuild_edges();
    }

    /// Remove a node
    pub fn unregister(&mut self, id: &str) {
        self.nodes.remove(id);
        self.edges.retain(|(a, b)| a != id && b != id);
    }

    /// Heartbeat from a node
    pub fn heartbeat(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.last_heartbeat = now();
            node.health = HealthStatus::Healthy;
        }
    }

    /// Check health of all nodes, mark dead ones
    pub fn health_check(&mut self) -> Vec<String> {
        let mut dead = vec![];
        let now_ms = now();
        for node in self.nodes.values_mut() {
            let age = now_ms.saturating_sub(node.last_heartbeat);
            if age > self.dead_threshold_ms {
                if node.health != HealthStatus::Dead {
                    dead.push(node.id.clone());
                }
                node.health = HealthStatus::Dead;
            } else if age > self.dead_threshold_ms / 2 {
                node.health = HealthStatus::Degraded;
            }
        }
        dead
    }

    /// Rebuild edges based on topology
    fn rebuild_edges(&mut self) {
        self.edges.clear();
        let node_ids: Vec<String> = self.nodes.keys().cloned().collect();
        let n = node_ids.len();
        if n < 2 { return; }

        match self.topology {
            Topology::Full => {
                for i in 0..n {
                    for j in (i+1)..n {
                        self.edges.insert(edge_key(&node_ids[i], &node_ids[j]));
                    }
                }
            }
            Topology::Ring => {
                for i in 0..n {
                    let j = (i + 1) % n;
                    self.edges.insert(edge_key(&node_ids[i], &node_ids[j]));
                }
            }
            Topology::Star => {
                if let Some(hub) = node_ids.first() {
                    for i in 1..n {
                        self.edges.insert(edge_key(hub, &node_ids[i]));
                    }
                }
            }
            Topology::Tree => {
                // Simple binary tree layout
                for i in 1..n {
                    let parent = ((i - 1) / 2).min(n - 1);
                    self.edges.insert(edge_key(&node_ids[parent], &node_ids[i]));
                }
            }
            Topology::Random => {
                // Each node connects to max_connections_per_node random neighbors
                let k = self.max_connections_per_node.min(n - 1);
                for i in 0..n {
                    // Connect to next K nodes (simple deterministic "random")
                    for offset in 1..=k {
                        let j = (i + offset) % n;
                        self.edges.insert(edge_key(&node_ids[i], &node_ids[j]));
                    }
                }
            }
        }
    }

    /// Get neighbors of a node
    pub fn neighbors(&self, id: &str) -> Vec<String> {
        self.edges.iter()
            .filter(|(a, b)| a == id || b == id)
            .map(|(a, b)| if a == id { b.clone() } else { a.clone() })
            .collect()
    }

    /// Discover nodes by capability
    pub fn discover(&self, capability: &str) -> Vec<&MeshNode> {
        self.nodes.values()
            .filter(|n| n.capabilities.iter().any(|c| c == capability))
            .filter(|n| n.health == HealthStatus::Healthy)
            .collect()
    }

    /// Least-loaded node with a capability
    pub fn least_loaded(&self, capability: &str) -> Option<&MeshNode> {
        let mut candidates: Vec<_> = self.discover(capability);
        candidates.sort_by(|a, b| a.load.partial_cmp(&b.load).unwrap());
        candidates.first().copied()
    }

    /// Most trusted node with a capability
    pub fn most_trusted(&self, capability: &str) -> Option<&MeshNode> {
        let mut candidates: Vec<_> = self.discover(capability);
        candidates.sort_by(|a, b| b.trust_score.partial_cmp(&a.trust_score).unwrap());
        candidates.first().copied()
    }

    /// Gossip: propagate a message through the mesh
    /// Returns nodes that received the message
    pub fn gossip(&self, from: &str, message: &[u8], visited: &mut HashSet<String>, max_hops: usize) -> Vec<String> {
        if max_hops == 0 { return vec![]; }
        visited.insert(from.to_string());
        let mut received = vec![];
        for neighbor in self.neighbors(from) {
            if visited.contains(&neighbor) { continue; }
            visited.insert(neighbor.clone());
            received.push(neighbor.clone());
            // Recurse
            let mut sub_visited = visited.clone();
            let sub = self.gossip(&neighbor, message, &mut sub_visited, max_hops - 1);
            received.extend(sub);
        }
        received
    }

    /// Count connected components (should be 1 for a healthy mesh)
    pub fn connected_components(&self) -> usize {
        let mut visited = HashSet::new();
        let mut components = 0;
        for node_id in self.nodes.keys() {
            if visited.contains(node_id) { continue; }
            components += 1;
            let mut stack = vec![node_id.clone()];
            while let Some(current) = stack.pop() {
                if visited.contains(&current) { continue; }
                visited.insert(current.clone());
                for neighbor in self.neighbors(&current) {
                    if !visited.contains(&neighbor) {
                        stack.push(neighbor);
                    }
                }
            }
        }
        components
    }

    /// Mesh statistics
    pub fn stats(&self) -> MeshStats {
        let total = self.nodes.len();
        let healthy = self.nodes.values().filter(|n| n.health == HealthStatus::Healthy).count();
        let degraded = self.nodes.values().filter(|n| n.health == HealthStatus::Degraded).count();
        let dead = self.nodes.values().filter(|n| n.health == HealthStatus::Dead).count();
        let avg_load = if total > 0 { self.nodes.values().map(|n| n.load).sum::<f64>() / total as f64 } else { 0.0 };
        let avg_trust = if total > 0 { self.nodes.values().map(|n| n.trust_score).sum::<f64>() / total as f64 } else { 0.0 };
        MeshStats { total_nodes: total, healthy, degraded, dead, edges: self.edges.len(), components: self.connected_components(), avg_load, avg_trust }
    }
}

#[derive(Clone, Debug)]
pub struct MeshStats {
    pub total_nodes: usize,
    pub healthy: usize,
    pub degraded: usize,
    pub dead: usize,
    pub edges: usize,
    pub components: usize,
    pub avg_load: f64,
    pub avg_trust: f64,
}

fn edge_key(a: &str, b: &str) -> (String, String) {
    if a < b { (a.to_string(), b.to_string()) } else { (b.to_string(), a.to_string()) }
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, caps: Vec<&str>) -> MeshNode {
        MeshNode { id: id.to_string(), address: format!("ws://{}", id), capabilities: caps.iter().map(|s| s.to_string()).collect(), health: HealthStatus::Healthy, load: 0.5, trust_score: 0.5, last_heartbeat: now(), metadata: HashMap::new() }
    }

    #[test]
    fn test_full_mesh() {
        let mut mesh = FleetMesh::with_topology(Topology::Full);
        mesh.register(make_node("a", vec!["nav"]));
        mesh.register(make_node("b", vec!["nav"]));
        mesh.register(make_node("c", vec!["cam"]));
        assert_eq!(mesh.neighbors("a").len(), 2); // connected to b and c
        assert_eq!(mesh.edges.len(), 3); // a-b, a-c, b-c
    }

    #[test]
    fn test_ring_mesh() {
        let mut mesh = FleetMesh::with_topology(Topology::Ring);
        for i in 0..5 { mesh.register(make_node(&format!("n{}", i), vec![])); }
        assert_eq!(mesh.edges.len(), 5);
        assert_eq!(mesh.neighbors("n0").len(), 2);
    }

    #[test]
    fn test_star_mesh() {
        let mut mesh = FleetMesh::with_topology(Topology::Star);
        mesh.register(make_node("hub", vec![]));
        mesh.register(make_node("s1", vec![]));
        mesh.register(make_node("s2", vec![]));
        assert_eq!(mesh.edges.len(), 2);
        assert_eq!(mesh.neighbors("hub").len(), 2);
    }

    #[test]
    fn test_tree_mesh() {
        let mut mesh = FleetMesh::with_topology(Topology::Tree);
        for i in 0..7 { mesh.register(make_node(&format!("n{}", i), vec![])); }
        // Binary tree: 7 nodes, 6 edges
        assert_eq!(mesh.edges.len(), 6);
    }

    #[test]
    fn test_discover() {
        let mut mesh = FleetMesh::with_topology(Topology::Full);
        mesh.register(make_node("a", vec!["nav", "cam"]));
        mesh.register(make_node("b", vec!["nav"]));
        mesh.register(make_node("c", vec!["cam"]));
        let nav_nodes = mesh.discover("nav");
        assert_eq!(nav_nodes.len(), 2);
    }

    #[test]
    fn test_least_loaded() {
        let mut mesh = FleetMesh::with_topology(Topology::Full);
        let mut a = make_node("a", vec!["nav"]); a.load = 0.9;
        let mut b = make_node("b", vec!["nav"]); b.load = 0.2;
        mesh.register(a); mesh.register(b);
        let chosen = mesh.least_loaded("nav").unwrap();
        assert_eq!(chosen.id, "b");
    }

    #[test]
    fn test_health_check_dead() {
        let mut mesh = FleetMesh::new();
        let mut node = make_node("a", vec![]);
        node.last_heartbeat = 0; // very old
        mesh.register(node);
        let dead = mesh.health_check();
        assert!(dead.contains(&"a".to_string()));
    }

    #[test]
    fn test_gossip() {
        let mut mesh = FleetMesh::with_topology(Topology::Full);
        mesh.register(make_node("a", vec![]));
        mesh.register(make_node("b", vec![]));
        mesh.register(make_node("c", vec![]));
        let mut visited = HashSet::new();
        let reached = mesh.gossip("a", b"hello", &mut visited, 3);
        assert_eq!(reached.len(), 2); // b and c
    }

    #[test]
    fn test_connected_components() {
        let mut mesh = FleetMesh::with_topology(Topology::Full);
        mesh.register(make_node("a", vec![]));
        mesh.register(make_node("b", vec![]));
        assert_eq!(mesh.connected_components(), 1);
    }

    #[test]
    fn test_unregister() {
        let mut mesh = FleetMesh::with_topology(Topology::Full);
        mesh.register(make_node("a", vec![]));
        mesh.register(make_node("b", vec![]));
        assert_eq!(mesh.edges.len(), 1);
        mesh.unregister("a");
        assert_eq!(mesh.edges.len(), 0);
    }

    #[test]
    fn test_stats() {
        let mut mesh = FleetMesh::with_topology(Topology::Full);
        mesh.register(make_node("a", vec![]));
        mesh.register(make_node("b", vec![]));
        let stats = mesh.stats();
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.healthy, 2);
        assert_eq!(stats.components, 1);
    }

    #[test]
    fn test_most_trusted() {
        let mut mesh = FleetMesh::with_topology(Topology::Full);
        let mut a = make_node("a", vec!["nav"]); a.trust_score = 0.3;
        let mut b = make_node("b", vec!["nav"]); b.trust_score = 0.9;
        mesh.register(a); mesh.register(b);
        let trusted = mesh.most_trusted("nav").unwrap();
        assert_eq!(trusted.id, "b");
    }

    #[test]
    fn test_node_metadata() {
        let mut node = make_node("a", vec![]);
        node.metadata.insert("version".to_string(), "1.0".to_string());
        let mut mesh = FleetMesh::new();
        mesh.register(node);
        assert_eq!(mesh.nodes["a"].metadata.get("version").unwrap(), "1.0");
    }
}

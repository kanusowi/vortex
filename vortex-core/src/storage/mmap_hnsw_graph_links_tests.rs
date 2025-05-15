#![cfg(test)]

use super::mmap_hnsw_graph_links::*;
use crate::error::VortexError;
// std::fs removed as it's not directly used
use std::path::Path;
use tempfile::tempdir;

const TEST_NAME: &str = "test_graph";
const NUM_NODES: u64 = 100;
const INITIAL_NUM_LAYERS: u16 = 5; // Max layers to allocate
const ACTUAL_NUM_LAYERS_TO_USE: u16 = 3; // Layers we'll actually test with, less than or equal to INITIAL_NUM_LAYERS
const ENTRY_POINT: u64 = 0;
const M0: u32 = 16; // Max connections for layer 0
const M: u32 = 8;   // Max connections for other layers

fn setup_graph_links(base_path: &Path) -> Result<MmapHnswGraphLinks, VortexError> {
    MmapHnswGraphLinks::new(
        base_path,
        TEST_NAME,
        NUM_NODES,
        INITIAL_NUM_LAYERS,
        ENTRY_POINT,
        M0,
        M,
    )
}

#[test]
fn test_new_open_flush_consistency() -> Result<(), VortexError> {
    let dir = tempdir()?;
    let base_path = dir.path();

    // Test new()
    let graph_new = setup_graph_links(base_path)?;
    assert_eq!(graph_new.get_entry_point_node_id(), ENTRY_POINT);
    assert_eq!(graph_new.get_num_layers(), std::cmp::max(INITIAL_NUM_LAYERS, 1)); // num_layers in header is allocated layers
    assert_eq!(graph_new.get_max_connections(0)?, M0);
    if INITIAL_NUM_LAYERS > 1 || INITIAL_NUM_LAYERS == 0 { // if 0, allocates 1 layer (layer 0)
         assert_eq!(graph_new.get_max_connections(1)?, M);
    }


    // Test flush()
    graph_new.flush()?;

    // Test open()
    let graph_opened = MmapHnswGraphLinks::open(base_path, TEST_NAME)?;
    assert_eq!(graph_opened.get_entry_point_node_id(), ENTRY_POINT);
    assert_eq!(graph_opened.get_num_layers(), std::cmp::max(INITIAL_NUM_LAYERS, 1));
    assert_eq!(graph_opened.get_max_connections(0)?, M0);
    if INITIAL_NUM_LAYERS > 1 || INITIAL_NUM_LAYERS == 0 {
        assert_eq!(graph_opened.get_max_connections(1)?, M);
    }

    // Check header fields directly from the opened graph's internal header
    // This assumes the `header` field in MmapHnswGraphLinks is pub(crate) or we have accessors
    // For now, we rely on the getters.

    dir.close()?;
    Ok(())
}

#[test]
fn test_get_max_connections_logic() -> Result<(), VortexError> {
    let dir = tempdir()?;
    let graph = setup_graph_links(dir.path())?;

    assert_eq!(graph.get_max_connections(0)?, M0);
    if INITIAL_NUM_LAYERS > 1 || INITIAL_NUM_LAYERS == 0 { // If only 1 layer allocated (layer 0), layer 1 is out of bounds
        for i in 1..std::cmp::max(INITIAL_NUM_LAYERS, 1) {
            assert_eq!(graph.get_max_connections(i)?, M);
        }
    }

    // Test out of bounds
    assert!(graph.get_max_connections(INITIAL_NUM_LAYERS).is_err());
     assert!(graph.get_max_connections(INITIAL_NUM_LAYERS + 1).is_err());


    dir.close()?;
    Ok(())
}

#[test]
fn test_set_get_connections_basic() -> Result<(), VortexError> {
    let dir = tempdir()?;
    let base_path = dir.path();
    let mut graph = setup_graph_links(base_path)?;

    let connections_l0_node5: Vec<u64> = (10..10 + (M0 / 2) as u64).collect(); // Half of max
    let connections_l1_node10: Vec<u64> = (20..20 + (M / 2) as u64).collect();

    // Set for layer 0
    graph.set_connections(5, 0, &connections_l0_node5)?;
    // Set for layer 1 (if layers > 0 allocated)
    if INITIAL_NUM_LAYERS > 1 || INITIAL_NUM_LAYERS == 0 {
         graph.set_connections(10, 1, &connections_l1_node10)?;
    }


    // Check immediately
    assert_eq!(graph.get_connections(5, 0), Some(connections_l0_node5.as_slice()));
    if INITIAL_NUM_LAYERS > 1 || INITIAL_NUM_LAYERS == 0 {
        assert_eq!(graph.get_connections(10, 1), Some(connections_l1_node10.as_slice()));
    }

    // Test uninitialized node/layer
    assert_eq!(graph.get_connections(0, 0), Some(&[] as &[u64])); // Should be empty

    graph.flush()?;
    drop(graph); // Explicitly drop to release mmap

    let mut graph_reopened = MmapHnswGraphLinks::open(base_path, TEST_NAME)?;

    assert_eq!(graph_reopened.get_connections(5, 0), Some(connections_l0_node5.as_slice()));
     if INITIAL_NUM_LAYERS > 1 || INITIAL_NUM_LAYERS == 0 {
        assert_eq!(graph_reopened.get_connections(10, 1), Some(connections_l1_node10.as_slice()));
    }
    assert_eq!(graph_reopened.get_connections(0, 0), Some(&[] as &[u64]));

    // Test setting connections again and overwriting
    let new_connections_l0_node5: Vec<u64> = (100..100 + (M0 / 3) as u64).collect();
    graph_reopened.set_connections(5, 0, &new_connections_l0_node5)?;
    assert_eq!(graph_reopened.get_connections(5, 0), Some(new_connections_l0_node5.as_slice()));

    graph_reopened.flush()?;
    drop(graph_reopened);

    let graph_final_check = MmapHnswGraphLinks::open(base_path, TEST_NAME)?;
    assert_eq!(graph_final_check.get_connections(5, 0), Some(new_connections_l0_node5.as_slice()));


    dir.close()?;
    Ok(())
}

#[test]
fn test_set_connections_empty_and_full() -> Result<(), VortexError> {
    let dir = tempdir()?;
    let mut graph = setup_graph_links(dir.path())?;

    // Set empty connections
    graph.set_connections(1, 0, &[])?;
    assert_eq!(graph.get_connections(1, 0), Some(&[] as &[u64]));

    // Set full connections
    let full_connections_l0: Vec<u64> = (0..M0 as u64).collect();
    graph.set_connections(2, 0, &full_connections_l0)?;
    assert_eq!(graph.get_connections(2, 0), Some(full_connections_l0.as_slice()));

    if INITIAL_NUM_LAYERS > 1 || INITIAL_NUM_LAYERS == 0 {
        let full_connections_l1: Vec<u64> = (0..M as u64).collect();
        graph.set_connections(3, 1, &full_connections_l1)?;
        assert_eq!(graph.get_connections(3, 1), Some(full_connections_l1.as_slice()));
    }

    dir.close()?;
    Ok(())
}


#[test]
fn test_set_get_connections_bounds_errors() -> Result<(), VortexError> {
    let dir = tempdir()?;
    let mut graph = setup_graph_links(dir.path())?;
    let connections: Vec<u64> = vec![1, 2, 3];

    // Invalid node_id for set
    assert!(matches!(
        graph.set_connections(NUM_NODES, 0, &connections),
        Err(VortexError::InvalidArgument(_))
    ));
    assert!(matches!(
        graph.set_connections(NUM_NODES + 1, 0, &connections),
        Err(VortexError::InvalidArgument(_))
    ));

    // Invalid layer_index for set
    assert!(matches!(
        graph.set_connections(0, INITIAL_NUM_LAYERS, &connections), // INITIAL_NUM_LAYERS is out of bounds (0-indexed)
        Err(VortexError::InvalidArgument(_)) // Error from set_connections directly
        // Or could be StorageError from get_max_connections if that's hit first
    ));
     assert!(matches!(
        graph.set_connections(0, INITIAL_NUM_LAYERS + 1, &connections),
        Err(VortexError::InvalidArgument(_))
    ));


    // Connections too long for set
    let too_many_connections_l0: Vec<u64> = (0..(M0 + 1) as u64).collect();
    assert!(matches!(
        graph.set_connections(0, 0, &too_many_connections_l0),
        Err(VortexError::InvalidArgument(_))
    ));

    if INITIAL_NUM_LAYERS > 1 || INITIAL_NUM_LAYERS == 0 {
        let too_many_connections_l1: Vec<u64> = (0..(M + 1) as u64).collect();
        assert!(matches!(
            graph.set_connections(0, 1, &too_many_connections_l1),
            Err(VortexError::InvalidArgument(_))
        ));
    }


    // Invalid node_id for get
    assert_eq!(graph.get_connections(NUM_NODES, 0), None);
    assert_eq!(graph.get_connections(NUM_NODES + 1, 0), None);

    // Invalid layer_index for get
    assert_eq!(graph.get_connections(0, INITIAL_NUM_LAYERS), None);
    assert_eq!(graph.get_connections(0, INITIAL_NUM_LAYERS + 1), None);

    dir.close()?;
    Ok(())
}

#[test]
fn test_multiple_nodes_and_layers() -> Result<(), VortexError> {
    let dir = tempdir()?;
    let base_path = dir.path();
    let mut graph = setup_graph_links(base_path)?;

    for layer in 0..ACTUAL_NUM_LAYERS_TO_USE {
        let max_conns = if layer == 0 { M0 } else { M };
        for node_id in (0..NUM_NODES).step_by(5) { // Test a subset of nodes
            let num_actual_conns = (node_id % (max_conns as u64 + 1)) as usize;
            let connections: Vec<u64> = (0..num_actual_conns as u64).map(|i| node_id + i + layer as u64 * 1000).collect();
            graph.set_connections(node_id, layer, &connections)?;
        }
    }

    graph.flush()?;
    drop(graph);

    let graph_reopened = MmapHnswGraphLinks::open(base_path, TEST_NAME)?;
    for layer in 0..ACTUAL_NUM_LAYERS_TO_USE {
        let max_conns = if layer == 0 { M0 } else { M };
        for node_id in (0..NUM_NODES).step_by(5) {
            let num_actual_conns = (node_id % (max_conns as u64 + 1)) as usize;
            let expected_connections: Vec<u64> = (0..num_actual_conns as u64).map(|i| node_id + i + layer as u64 * 1000).collect();
            assert_eq!(
                graph_reopened.get_connections(node_id, layer),
                Some(expected_connections.as_slice()),
                "Mismatch for node {}, layer {}", node_id, layer
            );
        }
         // Test a node that wasn't set, should be empty
        assert_eq!(graph_reopened.get_connections(1, layer), Some(&[] as &[u64]), "Expected empty for unset node 1, layer {}", layer);
    }


    dir.close()?;
    Ok(())
}

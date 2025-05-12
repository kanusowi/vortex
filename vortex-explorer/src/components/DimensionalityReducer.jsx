import React, { useEffect, useMemo } from 'react'; // Removed useState
import { useSelector, useDispatch } from 'react-redux';
import { UMAP } from 'umap-js';
import { 
    selectRawVectors, 
    selectVectorDataStatus, 
    setReducedCoordinates,
    setUmapStatus,         // Import action to set UMAP status
    selectUmapStatus,      // Import selector for UMAP status
    selectUmapError        // Import selector for UMAP error
} from '../features/vectors/vectorsSlice';

// Default UMAP parameters - could be made configurable later
const DEFAULT_UMAP_PARAMS = {
    nNeighbors: 15,
    minDist: 0.1,
    nComponents: 2, // Reduce to 2 dimensions for plotting
    // spread: 1.0, // Default
    // random: Math.random, // Default uses Math.random
};

function DimensionalityReducer({ indexName }) {
    const dispatch = useDispatch();
    
    // Selectors need the indexName passed to them
    const rawVectorsData = useSelector(selectRawVectors(indexName)); 
    const vectorFetchStatus = useSelector(selectVectorDataStatus(indexName));
    // Use Redux state for UMAP status and error
    const umapStatus = useSelector(selectUmapStatus(indexName));
    const umapError = useSelector(selectUmapError(indexName));

    // Memoize the vectors array to avoid unnecessary re-renders/recalculations
    const vectors = useMemo(() => {
        if (!rawVectorsData || rawVectorsData.length === 0) return null;
        // Extract just the vector arrays for UMAP
        return rawVectorsData.map(item => item.vector); 
    }, [rawVectorsData]);

    useEffect(() => {
        // Only run reduction if:
        // 1. An index is selected (indexName is not null).
        // 2. Vector fetching for this index succeeded.
        // 3. We have vectors to reduce.
        // 4. UMAP isn't already running or succeeded for this data.
        if (indexName && vectorFetchStatus === 'succeeded' && vectors && umapStatus === 'idle') {
            
            const reduce = async () => {
                // Dispatch action to set status to 'reducing'
                dispatch(setUmapStatus({ indexName, status: 'reducing' }));
                console.log(`Starting UMAP reduction for index "${indexName}" with ${vectors.length} vectors...`);

                // Dynamically adjust nNeighbors
                // nNeighbors must be less than the number of samples
                const numSamples = vectors.length;
                let nNeighbors = DEFAULT_UMAP_PARAMS.nNeighbors;
                if (numSamples <= nNeighbors) {
                    // Set nNeighbors to be numSamples - 1, but at least 2 if possible
                    // UMAP typically requires at least 2 neighbors.
                    nNeighbors = Math.max(2, numSamples - 1); 
                }

                // but it's better to be explicit or handle the edge case.
                if (numSamples < 2) { 
                    const errorMsg = 'Not enough data points for UMAP (minimum 2 required).';
                    // Dispatch action to set status to 'failed' with error
                    dispatch(setUmapStatus({ indexName, status: 'failed', error: errorMsg }));
                    // setReducedCoordinates is implicitly handled by setUmapStatus('failed')
                    return; // Exit early
                }
                
                const umapParams = {
                    ...DEFAULT_UMAP_PARAMS,
                    nNeighbors: nNeighbors,
                };
                console.log(`Using UMAP params:`, umapParams);


                try {
                    const umap = new UMAP(umapParams);
                    
                    // UMAP expects an array of arrays (vectors)
                    const embedding = await umap.fitAsync(vectors); 
                    
                    console.log(`UMAP reduction complete for index "${indexName}".`);
                    // Dispatch the results to the store. This action now also sets umapStatus to 'succeeded'.
                    dispatch(setReducedCoordinates({ indexName, coordinates: embedding }));
                    // No need to set local status

                } catch (error) {
                    const errorMsg = error.message || 'UMAP failed';
                    console.error(`UMAP reduction failed for index "${indexName}":`, error);
                    // Dispatch action to set status to 'failed' with error
                    dispatch(setUmapStatus({ indexName, status: 'failed', error: errorMsg }));
                }
            };

            // Run async reduction
            reduce();
        } 
        // Note: No explicit reset needed here, as status is managed per-index in Redux
        // and reset when fetching starts or selection changes.

    // Dependencies: Run effect when indexName, vector fetch status, or the memoized vectors array changes.
    // Also depends on dispatch and current umapStatus to avoid re-running unnecessarily.
    }, [indexName, vectorFetchStatus, vectors, umapStatus, dispatch]); 

    // This component doesn't render anything itself, it just triggers the reduction.
    // The status display will be handled in VisualizationWorkspace using the Redux state.
    return null; 
}

export default DimensionalityReducer;

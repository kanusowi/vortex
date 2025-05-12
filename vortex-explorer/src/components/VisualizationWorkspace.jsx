import React, { useEffect } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { Box, Typography, Paper, CircularProgress, Alert } from '@mui/material'; // Import MUI components
import { selectSelectedIndices } from '../features/indices/indicesSlice';
import { 
    fetchVectors, 
    selectVectorDataStatus, 
    selectVectorDataError, 
    selectDataForIndex,
    selectUmapStatus,      
    selectUmapError
} from '../features/vectors/vectorsSlice';
import DimensionalityReducer from './DimensionalityReducer';
import Plotter from './Plotter';

// Configuration for fetching vectors (e.g., sample size)
const VECTOR_FETCH_LIMIT = 1000; // Fetch up to 1000 vectors for visualization

function VisualizationWorkspace() {
    const dispatch = useDispatch();
    const selectedIndex = useSelector(selectSelectedIndices); 

    const vectorStatus = useSelector(selectVectorDataStatus(selectedIndex));
    const vectorError = useSelector(selectVectorDataError(selectedIndex));
    const umapStatus = useSelector(selectUmapStatus(selectedIndex)); 
    const umapError = useSelector(selectUmapError(selectedIndex));   
    const indexData = useSelector(selectDataForIndex(selectedIndex)); 

    useEffect(() => {
        // Fetch vectors only if:
        // 1. An index is selected.
        // 2. We don't already have data for it (or status is 'idle'/'failed').
        //    Avoid re-fetching if data is already 'succeeded' or 'loading'.
        if (selectedIndex && (!indexData || indexData.status === 'idle' || indexData.status === 'failed')) {
             console.log(`Triggering vector fetch for index: ${selectedIndex}`);
             dispatch(fetchVectors({ indexName: selectedIndex, limit: VECTOR_FETCH_LIMIT }));
        }
        // No cleanup needed here for this effect
    }, [selectedIndex, dispatch, indexData]); // Re-run when selectedIndex or indexData changes

    // Determine overall status message using MUI components
    let statusContent = null;
    if (vectorStatus === 'loading') {
        statusContent = <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}><CircularProgress size={16} /><Typography variant="body2" color="text.secondary">Loading vector data...</Typography></Box>;
    } else if (vectorStatus === 'failed') {
        statusContent = <Alert severity="error" variant="outlined" sx={{ py: 0.5 }}>{vectorError || 'Failed to load vectors'}</Alert>;
    } else if (vectorStatus === 'succeeded') {
        if (umapStatus === 'reducing') {
            statusContent = <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}><CircularProgress size={16} /><Typography variant="body2" color="text.secondary">Reducing dimensions (UMAP)...</Typography></Box>;
        } else if (umapStatus === 'failed') {
            statusContent = <Alert severity="error" variant="outlined" sx={{ py: 0.5 }}>{umapError || 'Dimensionality reduction failed'}</Alert>;
        } else if (umapStatus === 'succeeded' && !indexData?.reducedCoords) {
             statusContent = <Alert severity="warning" variant="outlined" sx={{ py: 0.5 }}>Reduction complete, but no coordinates generated.</Alert>;
        }
        // Optionally add success message:
        // else if (showPlot) {
        //     statusContent = <Alert severity="success" variant="outlined" sx={{ py: 0.5 }}>Plot rendered.</Alert>;
        // }
    }

    // Render logic based on selection
    if (!selectedIndex) {
        return (
            // Placeholder using Paper for consistent look
            <Paper 
                variant="outlined" 
                sx={{ 
                    height: '100%', 
                    display: 'flex', 
                    alignItems: 'center', 
                    justifyContent: 'center', 
                    textAlign: 'center',
                    p: 3,
                    borderStyle: 'dashed',
                    bgcolor: 'action.hover' 
                }}
            >
                <Typography variant="body1" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                    Select an index from the sidebar to visualize its vectors.
                </Typography>
            </Paper>
        );
    }

    // Determine if plot should be rendered
    const showPlot = vectorStatus === 'succeeded' && umapStatus === 'succeeded' && indexData?.reducedCoords;

    return (
        // Use Box for main container
        <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}> 
            {/* Title */}
            <Typography variant="h5" gutterBottom component="h2">
                Visualization: <Typography component="span" variant="h5" color="primary" sx={{ fontWeight: 'bold' }}>{selectedIndex}</Typography>
            </Typography>
            
            {/* Status Message Area */}
            <Box sx={{ mb: 2, minHeight: '40px' }}> {/* Consistent height for status */}
                {statusContent}
            </Box>
            
            {/* Dimensionality Reducer (runs in background, no UI) */}
            <DimensionalityReducer indexName={selectedIndex} />

            {/* Plotter Area using Paper */}
            <Paper 
                variant="outlined" 
                sx={{ 
                    flexGrow: 1, // Take remaining space
                    p: 1, // Padding around the plot
                    display: 'flex', 
                    alignItems: 'center', 
                    justifyContent: 'center',
                    minHeight: 400 // Ensure minimum height
                }}
            >
                {showPlot ? (
                    // Ensure Plotter fills the Paper
                    <Box sx={{ width: '100%', height: '100%' }}> 
                         <Plotter indexName={selectedIndex} />
                    </Box>
                ) : (
                    // Show placeholder only if no status message is active
                    !statusContent && 
                    <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                        Waiting for data or reduction...
                    </Typography>
                )}
            </Paper>
        </Box>
    );
}

export default VisualizationWorkspace;

import React, { useEffect } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { Box, Typography, Paper, CircularProgress, Alert, LinearProgress } from '@mui/material'; // Import MUI components
import { selectSelectedIndices } from '../features/indices/indicesSlice';
import { 
    fetchVectors, 
    selectVectorDataStatus, 
    selectVectorDataError, 
    selectDataForIndex,
    selectUmapStatus,      
    selectUmapError,
    selectSearchResults, // Added
    selectSearchStatus   // Added
} from '../features/vectors/vectorsSlice';
import DimensionalityReducer from './DimensionalityReducer';
import Plotter from './Plotter';
import PointDetailsPanel from './PointDetailsPanel'; 
import PlotControls from './PlotControls'; // Import PlotControls
import { List, ListItem, Collapse, IconButton, Tooltip } from '@mui/material'; 
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'; 
import ExpandLessIcon from '@mui/icons-material/ExpandLess'; // Added
import { styled } from '@mui/material/styles'; // Added for styling expand icon

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
    const searchResults = useSelector(selectSearchResults(selectedIndex)); // Added
    const searchStatus = useSelector(selectSearchStatus(selectedIndex));   // Added

    // State for expanding/collapsing search results metadata
    const [expandedResult, setExpandedResult] = React.useState(null);

    const handleExpandResult = (resultId) => {
        setExpandedResult(expandedResult === resultId ? null : resultId);
    };

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
            <Box sx={{ mb: 2, minHeight: '40px' }}> 
                {statusContent}
            </Box>

            {/* Plot Controls - only show if an index is selected and data might be available */}
            {selectedIndex && vectorStatus !== 'loading' && <PlotControls />}
            
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

            {/* Search Results Section */}
            {searchStatus === 'succeeded' && searchResults && searchResults.length > 0 && (
                <Paper variant="outlined" sx={{ mt: 2, p: 2 }}>
                    <Typography variant="h6" gutterBottom component="h3">
                        Search Results ({searchResults.length})
                    </Typography>
                    <List dense sx={{ maxHeight: 300, overflow: 'auto' }}>
                        {searchResults.map((item) => (
                            <React.Fragment key={item.id}>
                                <ListItem
                                    disablePadding
                                    secondaryAction={
                                        item.metadata && (
                                            <Tooltip title={expandedResult === item.id ? "Collapse metadata" : "Expand metadata"}>
                                                <IconButton edge="end" aria-label="expand" onClick={() => handleExpandResult(item.id)}>
                                                    {expandedResult === item.id ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                                                </IconButton>
                                            </Tooltip>
                                        )
                                    }
                                    sx={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', pt: 1, pb: 1 }}
                                >
                                    {/* Display ID */}
                                    <Typography variant="subtitle1" component="div" sx={{ fontWeight: 'medium' }}>
                                        ID: {item.id}
                                    </Typography>
                                    {/* Box for Score text and LinearProgress bar */}
                                    <Box sx={{ width: 'calc(100% - 40px)', display: 'flex', alignItems: 'center', mb: item.metadata && Object.keys(item.metadata).length > 0 ? 0.5 : 0, pr: '40px' }}>
                                        <Typography variant="body2" color="text.secondary" sx={{ minWidth: '90px' }}>
                                            Score: {item.score.toFixed(4)}
                                        </Typography>
                                        <LinearProgress
                                            variant="determinate"
                                            value={Math.max(0, Math.min(100, item.score * 100))} // Assuming score is 0-1 similarity
                                            sx={{ flexGrow: 1, height: 8, borderRadius: 4, ml: 1 }}
                                            color={item.score > 0.75 ? "success" : item.score > 0.5 ? "warning" : "error"} // Color based on score
                                        />
                                    </Box>
                                    {/* Display first few metadata entries */}
                                    {item.metadata && Object.entries(item.metadata).slice(0, 2).map(([key, value]) => (
                                        <Typography
                                            component="div"
                                            variant="caption"
                                            display="block"
                                            key={key}
                                            sx={{
                                                color: 'text.secondary',
                                                pl: 0,
                                                fontSize: '0.75rem',
                                                mt: 0.5,
                                                whiteSpace: 'nowrap',
                                                overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                                maxWidth: 'calc(100% - 40px)'
                                            }}
                                        >
                                            {`${key}: ${String(value)}`}
                                        </Typography>
                                    ))}
                                </ListItem>
                                {item.metadata && (
                                    <Collapse in={expandedResult === item.id} timeout="auto" unmountOnExit>
                                        <Box sx={{ pl: 2, pb:1, pt:0.5, backgroundColor: 'action.hover', borderRadius: 1 }}>
                                            <Typography variant="caption" component="pre" sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all', display: 'block' }}>
                                                {JSON.stringify(item.metadata, null, 2)}
                                            </Typography>
                                        </Box>
                                    </Collapse>
                                )}
                            </React.Fragment>
                        ))}
                    </List>
                </Paper>
            )}
             {searchStatus === 'searching' && (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 2 }}>
                    <CircularProgress size={16} />
                    <Typography variant="body2" color="text.secondary">Searching...</Typography>
                </Box>
            )}

            {/* Point Details Panel */}
            <PointDetailsPanel />
        </Box>
    );
}

export default VisualizationWorkspace;

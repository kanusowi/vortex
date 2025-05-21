import React from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { Box, Typography, Paper, IconButton, Tooltip } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import { 
    selectSelectedPlotPointId, 
    selectDataForIndex,
    setSelectedPlotPointId
} from '../features/vectors/vectorsSlice';
import { selectSelectedIndices } from '../features/indices/indicesSlice';

const PointDetailsPanel = () => {
    const dispatch = useDispatch();
    const selectedIndexName = useSelector(selectSelectedIndices);
    const selectedPlotPointId = useSelector(selectSelectedPlotPointId(selectedIndexName));
    const indexData = useSelector(selectDataForIndex(selectedIndexName));

    if (!selectedPlotPointId || !indexData || !indexData.rawVectors) {
        return null; // Don't render if no point is selected or data is unavailable
    }

    const point = indexData.rawVectors.find(p => p.id === selectedPlotPointId);

    if (!point) {
        return null; // Point not found in current rawVectors, perhaps data changed
    }

    const handleClose = () => {
        dispatch(setSelectedPlotPointId({ indexName: selectedIndexName, pointId: null }));
    };

    return (
        <Paper 
            elevation={3} 
            sx={{ 
                p: 2, 
                mt: 2, // Add some margin if it's stacked in VisualizationWorkspace
                // Or, if positioned absolutely/fixed, define position, width, height, zIndex etc.
                // For now, let's assume it's part of the normal flow and will be positioned by parent.
                maxHeight: '400px', // Example max height, adjust as needed
                overflowY: 'auto',
                position: 'relative', // For absolute positioning of the close button
            }}
        >
            <Tooltip title="Close details">
                <IconButton 
                    onClick={handleClose}
                    sx={{ 
                        position: 'absolute', 
                        top: 8, 
                        right: 8 
                    }}
                    size="small"
                >
                    <CloseIcon fontSize="small" />
                </IconButton>
            </Tooltip>
            <Typography variant="h6" gutterBottom component="h3">
                Selected Point Details
            </Typography>
            <Typography variant="subtitle1" gutterBottom sx={{ wordBreak: 'break-all' }}>
                ID: {point.id}
            </Typography>

            <Box sx={{ mt: 1, mb: 1 }}>
                <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 'medium' }}>
                    Raw Vector:
                </Typography>
                <Paper variant="outlined" sx={{ p: 1, maxHeight: '100px', overflowY: 'auto', backgroundColor: 'action.hover' }}>
                    <Typography component="pre" variant="caption" sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                        {JSON.stringify(point.vector, null, 2)}
                    </Typography>
                </Paper>
            </Box>

            {point.metadata && Object.keys(point.metadata).length > 0 && (
                <Box sx={{ mt: 1 }}>
                    <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 'medium' }}>
                        Metadata:
                    </Typography>
                    <Paper variant="outlined" sx={{ p: 1, maxHeight: '150px', overflowY: 'auto', backgroundColor: 'action.hover' }}>
                        <Typography component="pre" variant="caption" sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                            {JSON.stringify(point.metadata, null, 2)}
                        </Typography>
                    </Paper>
                </Box>
            )}
             {!point.metadata || Object.keys(point.metadata).length === 0 && (
                <Typography variant="caption" color="text.secondary">
                    No metadata available for this point.
                </Typography>
            )}
        </Paper>
    );
};

export default PointDetailsPanel;

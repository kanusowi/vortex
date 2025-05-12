import React, { useEffect } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { 
    Box, 
    FormControl, 
    InputLabel, 
    Select, 
    MenuItem, 
    CircularProgress, 
    Typography, 
    Card, 
    CardContent, 
    List, 
    ListItem, 
    ListItemText,
    Alert,
    AlertTitle
} from '@mui/material';
import { 
    fetchIndices, 
    fetchIndexStats, 
    selectIndex, 
    selectAllIndices, 
    selectSelectedIndices, 
    selectIndicesStatus, 
    selectIndicesError,
    selectIndexStats,      
    selectIndexStatsStatus,
    selectIndexStatsError
} from '../features/indices/indicesSlice';

function IndexSelector() {
    const dispatch = useDispatch();
    const indexList = useSelector(selectAllIndices);
    const selectedIndex = useSelector(selectSelectedIndices);
    const indicesStatus = useSelector(selectIndicesStatus); 
    const indicesError = useSelector(selectIndicesError);   

    const stats = useSelector(selectIndexStats);
    const statsStatus = useSelector(selectIndexStatsStatus);
    const statsError = useSelector(selectIndexStatsError);

    useEffect(() => {
        if (indicesStatus === 'idle') {
            dispatch(fetchIndices());
        }
    }, [indicesStatus, dispatch]);

    useEffect(() => {
        if (selectedIndex) {
            dispatch(fetchIndexStats(selectedIndex));
        }
    }, [selectedIndex, dispatch]);

    const handleSelectionChange = (event) => {
        const newSelection = event.target.value;
        // MUI Select uses empty string for "none" or placeholder value
        dispatch(selectIndex(newSelection === "" ? null : newSelection)); 
    };

    // Stats display content using MUI components
    let statsContent;
    if (!selectedIndex) {
        statsContent = <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>Select an index to view stats.</Typography>;
    } else if (statsStatus === 'loading') {
        statsContent = <Box sx={{ display: 'flex', justifyContent: 'center', my: 2 }}><CircularProgress size={24} /></Box>;
    } else if (statsStatus === 'succeeded' && stats) {
        statsContent = (
            <List dense disablePadding>
                <ListItem disableGutters>
                    <ListItemText primary="Vectors" secondary={stats.vector_count ?? 'N/A'} />
                </ListItem>
                <ListItem disableGutters>
                    <ListItemText primary="Dimensions" secondary={stats.dimensions ?? 'N/A'} />
                </ListItem>
                <ListItem disableGutters>
                    <ListItemText primary="Metric" secondary={stats.metric ? stats.metric.charAt(0).toUpperCase() + stats.metric.slice(1) : 'N/A'} />
                </ListItem>
                 {/* Config could be added here */}
            </List>
        );
    } else if (statsStatus === 'failed') {
        statsContent = <Alert severity="error" sx={{ mt: 1 }}>{statsError || 'Failed to load stats'}</Alert>;
    } else {
        statsContent = <Typography variant="body2" color="text.secondary">No stats available.</Typography>;
    }

    return (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}> {/* Use Box for layout and gap */}
            {/* Index Selection Dropdown */}
            <FormControl fullWidth variant="outlined" size="small" disabled={indicesStatus === 'loading' || indicesStatus === 'failed'}>
                <InputLabel id="index-select-label">Select Index</InputLabel>
                <Select
                    labelId="index-select-label"
                    id="index-select"
                    value={selectedIndex || ""} // Use empty string for placeholder
                    label="Select Index"
                    onChange={handleSelectionChange}
                    startAdornment={indicesStatus === 'loading' ? <CircularProgress size={20} sx={{ mr: 1 }} /> : null}
                >
                    {/* Placeholder */}
                    <MenuItem value="" disabled={indicesStatus === 'loading'}>
                        <em>{indicesStatus === 'loading' ? 'Loading...' : '-- Select an Index --'}</em>
                    </MenuItem>
                    
                    {/* Index List */}
                    {indicesStatus === 'succeeded' && indexList.map((indexName) => (
                        <MenuItem key={indexName} value={indexName}>
                            {indexName}
                        </MenuItem>
                    ))}
                </Select>
                {indicesStatus === 'failed' && <Alert severity="error" sx={{ mt: 1 }}>{indicesError || 'Failed to load indices'}</Alert>}
            </FormControl>

            {/* Index Stats Card */}
            <Card variant="outlined">
                <CardContent>
                    <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 'medium' }}>
                        Index Statistics
                    </Typography>
                    {statsContent}
                </CardContent>
            </Card>
        </Box>
    );
}

export default IndexSelector;

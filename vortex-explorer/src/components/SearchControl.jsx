import React, { useState, useEffect, useMemo } from 'react'; // Added useState, useEffect, useMemo
import { useForm, Controller, useWatch } from 'react-hook-form'; // Added useWatch
import { useDispatch, useSelector } from 'react-redux';
import { Box, TextField, Button, CircularProgress, Typography, Alert, ToggleButtonGroup, ToggleButton, FormControlLabel, Switch, Divider } from '@mui/material'; // Added ToggleButton
import SearchIcon from '@mui/icons-material/Search';
import CodeIcon from '@mui/icons-material/Code'; // For JSON view
import BuildIcon from '@mui/icons-material/Build'; // For Builder view
import { searchVectors, clearSearchResults, selectSearchStatus, selectSearchError } from '../features/vectors/vectorsSlice';
import { selectSelectedIndices } from '../features/indices/indicesSlice';
import QueryBuilderChunk1 from './QueryBuilderChunk1'; // Import the new builder

function SearchControl() {
    const dispatch = useDispatch();
    const selectedIndex = useSelector(selectSelectedIndices);
    const searchStatus = useSelector(selectSearchStatus(selectedIndex));
    const searchError = useSelector(selectSearchError(selectedIndex));

    const [filterViewMode, setFilterViewMode] = useState('builder'); // 'builder' or 'json'
    
    // We still use react-hook-form for other fields and to hold the stringified filter
    const { handleSubmit, control, formState: { errors }, reset, setError, setValue, watch } = useForm({
        defaultValues: {
            queryVectorString: '',
            k: 10,
            filterString: '{}', // Default to empty JSON object string
        },
    });

    const watchedFilterString = watch('filterString');

    const handleFilterViewChange = (event, newView) => {
        if (newView !== null) {
            setFilterViewMode(newView);
        }
    };

    const handleBuilderFilterChange = React.useCallback((newFilterObject) => {
        try {
            setValue('filterString', JSON.stringify(newFilterObject, null, 2), { shouldValidate: true, shouldDirty: true });
        } catch (e) {
            // This should not happen if newFilterObject is always a valid object
            console.error("Error stringifying builder filter:", e);
        }
    }, [setValue]);
    
    const initialBuilderFilter = useMemo(() => {
        try {
            const parsed = JSON.parse(watchedFilterString || '{}');
            if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) {
                return parsed;
            }
        } catch (e) {
            // Fallback on parse error
        }
        return {}; // Default to empty object
    }, [watchedFilterString]);


    const onSubmit = (data) => {
        if (!selectedIndex) {
            console.error("No index selected for search.");
            return;
        }
        dispatch(clearSearchResults(selectedIndex)); 

        let queryVector;
        try {
            queryVector = data.queryVectorString.split(',')
                .map(s => s.trim())
                .filter(s => s !== '')
                .map(s => {
                    const num = parseFloat(s);
                    if (isNaN(num)) throw new Error(`Invalid number: "${s}"`);
                    return num;
                });
            if (queryVector.length === 0) throw new Error("Query vector empty.");
        } catch (e) {
            setError('queryVectorString', { type: 'manual', message: e.message || 'Invalid vector format.' });
            return;
        }

        const kValue = parseInt(data.k, 10);
        if (isNaN(kValue) || kValue <= 0) {
            setError('k', { type: 'manual', message: 'K must be positive.' });
            return;
        }

        let filterToSubmit = {};
        // filterString is kept in sync by QueryBuilder or manual JSON edit
        if (data.filterString && data.filterString.trim() !== '{}' && data.filterString.trim() !== '') {
            try {
                filterToSubmit = JSON.parse(data.filterString);
                 if (typeof filterToSubmit !== 'object' || filterToSubmit === null || Array.isArray(filterToSubmit)) {
                    throw new Error('Filter must be a JSON object.');
                }
            } catch (e) {
                setError('filterString', { type: 'manual', message: e.message || 'Invalid JSON for filter.' });
                // Also display this error near the JSON input if in JSON mode
                return;
            }
        }
        
        // If builder produced an empty object, and filterString was also effectively empty, submit undefined
        if (Object.keys(filterToSubmit).length === 0) {
            filterToSubmit = undefined;
        }

        console.log(`Dispatching search for index: ${selectedIndex}, k: ${kValue}, vector:`, queryVector, "filter:", filterToSubmit);
        dispatch(searchVectors({ indexName: selectedIndex, queryVector, k: kValue, filter: filterToSubmit }));
    };

    useEffect(() => {
        reset({ queryVectorString: '', k: 10, filterString: '{}' });
        // setFilterViewMode('builder'); // Optionally reset view mode
    }, [selectedIndex, reset]);

    return (
        <Box component="form" onSubmit={handleSubmit(onSubmit)} sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 3 }}>
            <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 'medium' }}>
                Search Neighbors (k-NN)
            </Typography>
            
            <Controller
                name="queryVectorString"
                control={control}
                rules={{ required: 'Query vector is required.' }}
                render={({ field }) => (
                    <TextField
                        {...field}
                        label="Query Vector (comma-separated)"
                        variant="outlined"
                        size="small"
                        multiline
                        rows={3}
                        fullWidth
                        error={!!errors.queryVectorString}
                        helperText={errors.queryVectorString?.message}
                        disabled={!selectedIndex || searchStatus === 'searching'}
                        placeholder="e.g., 0.1, -0.5, 1.2, ..."
                    />
                )}
            />

            <Controller
                name="k"
                control={control}
                rules={{ 
                    required: 'Number of neighbors (k) is required.',
                    min: { value: 1, message: 'k must be at least 1' },
                    pattern: { value: /^[1-9]\d*$/, message: 'k must be a positive integer' } 
                }}
                render={({ field }) => (
                    <TextField
                        {...field}
                        label="Number of Neighbors (k)"
                        type="number"
                        variant="outlined"
                        size="small"
                        fullWidth
                        error={!!errors.k}
                        helperText={errors.k?.message}
                        disabled={!selectedIndex || searchStatus === 'searching'}
                        InputProps={{ inputProps: { min: 1 } }} 
                    />
                )}
            />
            
            <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 1 }}>
                <ToggleButtonGroup
                    value={filterViewMode}
                    exclusive
                    onChange={handleFilterViewChange}
                    aria-label="filter view mode"
                    size="small"
                >
                    <ToggleButton value="builder" aria-label="builder mode" disabled={!selectedIndex || searchStatus === 'searching'}>
                        <BuildIcon fontSize="small" sx={{mr: 0.5}}/> Builder
                    </ToggleButton>
                    <ToggleButton value="json" aria-label="json mode" disabled={!selectedIndex || searchStatus === 'searching'}>
                        <CodeIcon fontSize="small" sx={{mr: 0.5}}/> JSON
                    </ToggleButton>
                </ToggleButtonGroup>
            </Box>

            {filterViewMode === 'builder' && (
                <QueryBuilderChunk1 
                    onFilterChange={handleBuilderFilterChange} 
                    initialFilter={initialBuilderFilter}
                />
            )}

            {filterViewMode === 'json' && (
                <Controller
                    name="filterString"
                    control={control}
                    render={({ field }) => (
                        <TextField
                            {...field}
                            label="Metadata Filter (JSON, optional)"
                            variant="outlined"
                            size="small"
                            multiline
                            rows={3}
                            fullWidth
                            error={!!errors.filterString}
                            helperText={errors.filterString?.message}
                            disabled={!selectedIndex || searchStatus === 'searching'}
                            placeholder='e.g., {"category": "books", "year": 2023}'
                        />
                    )}
                />
            )}
            <Divider sx={{mt:1, mb:1}} />

            <Button
                type="submit"
                variant="contained"
                startIcon={searchStatus === 'searching' ? <CircularProgress size={20} color="inherit" /> : <SearchIcon />}
                disabled={!selectedIndex || searchStatus === 'searching'}
            >
                {searchStatus === 'searching' ? 'Searching...' : 'Search'}
            </Button>

            {searchStatus === 'failed' && (
                <Alert severity="error" sx={{ mt: 1 }}>{searchError || 'Search failed'}</Alert>
            )}
        </Box>
    );
}

export default SearchControl;

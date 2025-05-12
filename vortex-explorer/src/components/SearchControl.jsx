import React from 'react';
import { useForm, Controller } from 'react-hook-form';
import { useDispatch, useSelector } from 'react-redux';
import { Box, TextField, Button, CircularProgress, Typography, Alert } from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import { searchVectors, clearSearchResults, selectSearchStatus, selectSearchError } from '../features/vectors/vectorsSlice';
import { selectSelectedIndices } from '../features/indices/indicesSlice';

function SearchControl() {
    const dispatch = useDispatch();
    const selectedIndex = useSelector(selectSelectedIndices);
    const searchStatus = useSelector(selectSearchStatus(selectedIndex));
    const searchError = useSelector(selectSearchError(selectedIndex));

    const { handleSubmit, control, formState: { errors }, reset, setError } = useForm({
        defaultValues: {
            queryVectorString: '',
            k: 10,
        },
    });

    const onSubmit = (data) => {
        if (!selectedIndex) {
            // Should ideally not happen if button is disabled, but good practice
            console.error("No index selected for search.");
            return; 
        }

        // Clear previous errors/results for this index
        dispatch(clearSearchResults(selectedIndex)); 

        // Validate and parse query vector string
        let queryVector;
        try {
            queryVector = data.queryVectorString.split(',')
                .map(s => s.trim())
                .filter(s => s !== '') // Handle trailing commas or empty segments
                .map(s => {
                    const num = parseFloat(s);
                    if (isNaN(num)) {
                        throw new Error(`Invalid number format: "${s}"`);
                    }
                    return num;
                });
            
            if (queryVector.length === 0) {
                 throw new Error("Query vector cannot be empty after parsing.");
            }
        } catch (e) {
            setError('queryVectorString', { type: 'manual', message: e.message || 'Invalid vector format. Use comma-separated numbers.' });
            return;
        }

        const kValue = parseInt(data.k, 10);
        if (isNaN(kValue) || kValue <= 0) {
             setError('k', { type: 'manual', message: 'K must be a positive integer.' });
             return;
        }

        console.log(`Dispatching search for index: ${selectedIndex}, k: ${kValue}, vector:`, queryVector);
        dispatch(searchVectors({ indexName: selectedIndex, queryVector, k: kValue }));
    };

    // Reset form if index changes
    React.useEffect(() => {
        reset({ queryVectorString: '', k: 10 }); 
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

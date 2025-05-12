import React, { useEffect, useState } from 'react'; // Added useState
import { useForm, Controller } from 'react-hook-form';
import { useDispatch, useSelector } from 'react-redux';
import {
    Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle,
    TextField, CircularProgress, Box, Alert, Typography, Divider, LinearProgress // Added Typography, Divider, LinearProgress
} from '@mui/material';
import AddLinkIcon from '@mui/icons-material/AddLink'; 
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome'; // For batch generate
import toast from 'react-hot-toast';
import { addVector, selectGlobalAddVectorStatus, selectGlobalAddVectorError, resetAddVectorStatus } from '../features/vectors/vectorsSlice';
import { selectSelectedIndices, fetchIndexStats, selectIndexStats } from '../features/indices/indicesSlice'; 

function AddVectorModal() {
    const dispatch = useDispatch();
    const [open, setOpen] = React.useState(false);
    
    const selectedIndex = useSelector(selectSelectedIndices);
    const indexStats = useSelector(selectIndexStats); 
    const addStatus = useSelector(selectGlobalAddVectorStatus); // For single add
    const addError = useSelector(selectGlobalAddVectorError);   // For single add

    // State for batch generation within the modal
    const [isBatchGenerating, setIsBatchGenerating] = useState(false);
    const [batchProgress, setBatchProgress] = useState(0);
    const [batchError, setBatchError] = useState(null);
    const [numVectorsToGenerate, setNumVectorsToGenerate] = useState(10); // Default for batch

    const { control, handleSubmit, formState: { errors }, reset, setError, setValue } = useForm({
        defaultValues: {
            vectorId: '', // For single add
            vectorDataString: '', // For single add
        },
    });

    // Fetch index stats when selectedIndex changes and modal might need it
    useEffect(() => {
        if (open && selectedIndex && !indexStats) {
            dispatch(fetchIndexStats(selectedIndex));
        }
    }, [open, selectedIndex, indexStats, dispatch]);
    
    // Update form if selectedIndex changes (e.g. to prefill or clear)
    useEffect(() => {
        // You could prefill index name if it were a field, or reset on index change
        // For now, just ensure form is clean if no index is selected.
        if (!selectedIndex) {
            reset({ vectorId: '', vectorDataString: '' });
        }
    }, [selectedIndex, reset]);


    const handleClickOpen = () => {
        if (!selectedIndex) {
            toast.error("Please select an index first.");
            return;
        }
        setOpen(true);
    };

    const handleClose = () => {
        setOpen(false);
        if (addStatus !== 'adding') {
            reset();
            dispatch(resetAddVectorStatus());
        }
    };

    const onSubmit = (data) => {
        if (!selectedIndex) return; // Should be disabled if no index

        let parsedVectorData;
        try {
            parsedVectorData = data.vectorDataString.split(',')
                .map(s => s.trim())
                .filter(s => s !== '')
                .map(s => {
                    const num = parseFloat(s);
                    if (isNaN(num)) throw new Error(`Invalid number: "${s}"`);
                    return num;
                });
            if (parsedVectorData.length === 0) throw new Error("Vector data cannot be empty.");
        } catch (e) {
            setError('vectorDataString', { type: 'manual', message: e.message || 'Invalid vector format.' });
            return;
        }

        const payload = {
            indexName: selectedIndex,
            vectorId: data.vectorId,
            vectorData: parsedVectorData,
        };
        
        dispatch(addVector(payload));
    };

    useEffect(() => {
        // Effect for single vector add status
        if (addStatus === 'succeeded') {
            toast.success('Vector added/updated successfully!');
            if (selectedIndex) {
                dispatch(fetchIndexStats(selectedIndex));
            }
            // Only close if not batch generating, as batch might still be in progress
            if (!isBatchGenerating) {
                handleClose();
            }
        } else if (addStatus === 'failed') {
            // This error is for the single add operation
            toast.error(`Failed to add/update vector: ${addError}`);
        }
    }, [addStatus, addError, dispatch, selectedIndex, handleClose, isBatchGenerating]);


    const handleBatchGenerate = async () => {
        if (!selectedIndex || !indexStats || !indexStats.dimensions) {
            toast.error("Index dimensions not available. Select an index first.");
            return;
        }

        setIsBatchGenerating(true);
        setBatchProgress(0);
        setBatchError(null);
        let vectorsAdded = 0;
        const totalVectors = parseInt(numVectorsToGenerate, 10);

        if (isNaN(totalVectors) || totalVectors <= 0) {
            setBatchError("Number of vectors must be a positive integer.");
            setIsBatchGenerating(false);
            return;
        }

        try {
            for (let i = 0; i < totalVectors; i++) {
                const vectorId = `synth-vec-${Date.now()}-${i}`;
                const vectorData = Array.from(
                    { length: indexStats.dimensions },
                    () => parseFloat((Math.random() * 2 - 1).toFixed(4))
                );
                // We await each dispatch to ensure they are processed sequentially by the backend
                // and to correctly update progress.
                await dispatch(addVector({ indexName: selectedIndex, vectorId, vectorData })).unwrap();
                vectorsAdded++;
                setBatchProgress(Math.round((vectorsAdded / totalVectors) * 100));
            }
            toast.success(`${vectorsAdded} synthetic vectors added to "${selectedIndex}".`);
            dispatch(fetchIndexStats(selectedIndex)); // Refresh stats
        } catch (error) {
            console.error("Batch synthetic data generation failed:", error);
            setBatchError(error.message || "Failed during batch generation.");
            toast.error(`Error during batch generation: ${error.message || 'Unknown error'}`);
        } finally {
            setIsBatchGenerating(false);
            // Do not close modal automatically after batch, user might want to add more.
        }
    };


    return (
        <div>
            <Button
                variant="outlined"
                startIcon={<AddLinkIcon />}
                onClick={handleClickOpen}
                sx={{ mt: 1, width: '100%' }}
                disabled={!selectedIndex} 
            >
                Add/Update Vector
            </Button>
            <Dialog open={open} onClose={handleClose} PaperProps={{ component: 'form', onSubmit: handleSubmit(onSubmit) }} maxWidth="md" fullWidth>
                <DialogTitle>Add or Update Vector in "{selectedIndex}"</DialogTitle>
                <DialogContent dividers> {/* Use dividers for better section separation */}
                    {/* Section 1: Manual Single Vector Entry */}
                    <Box sx={{ mb: 3 }}>
                        <DialogContentText sx={{ mb: 2 }}>
                            Manually enter Vector ID and comma-separated data for a single vector.
                        </DialogContentText>
                        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                            <Controller
                                name="vectorId"
                                control={control}
                                rules={{ required: 'Vector ID is required for manual submission.' }}
                                render={({ field }) => (
                                    <TextField {...field} label="Vector ID" fullWidth autoFocus error={!!errors.vectorId} helperText={errors.vectorId?.message} disabled={addStatus === 'adding' || isBatchGenerating} />
                                )}
                            />
                            <Controller
                                name="vectorDataString"
                                control={control}
                                rules={{ required: 'Vector data is required for manual submission.' }}
                                render={({ field }) => (
                                    <TextField 
                                        {...field} 
                                        label="Vector Data (comma-separated)" 
                                        fullWidth 
                                        multiline 
                                        rows={3} 
                                        error={!!errors.vectorDataString} 
                                        helperText={errors.vectorDataString?.message} 
                                        disabled={addStatus === 'adding' || isBatchGenerating}
                                        placeholder="e.g., 0.1, -0.5, 1.2, ..."
                                    />
                                )}
                            />
                        </Box>
                         {addStatus === 'failed' && addError && (
                            <Alert severity="error" sx={{ mt: 2 }} variant="outlined">{addError}</Alert>
                        )}
                    </Box>

                    <Divider sx={{ my: 3 }}>OR</Divider>

                    {/* Section 2: Batch Generate Synthetic Vectors */}
                    <Box>
                        <DialogContentText sx={{ mb: 2 }}>
                            Alternatively, generate and add a batch of synthetic vectors to this index. Vector IDs will be auto-generated.
                        </DialogContentText>
                        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                            <TextField
                                label="Number of Vectors to Generate"
                                type="number"
                                variant="outlined"
                                size="small"
                                value={numVectorsToGenerate}
                                onChange={(e) => setNumVectorsToGenerate(Math.max(1, parseInt(e.target.value,10) || 1))}
                                disabled={isBatchGenerating || !selectedIndex || !indexStats || addStatus === 'adding'}
                                InputProps={{ inputProps: { min: 1 } }}
                                fullWidth 
                            />
                            <Button
                                variant="contained" 
                                color="secondary" 
                                startIcon={isBatchGenerating ? <CircularProgress size={20} color="inherit" /> : <AutoAwesomeIcon />}
                                onClick={handleBatchGenerate} // Different handler for batch
                                disabled={isBatchGenerating || !selectedIndex || !indexStats || addStatus === 'adding'}
                                fullWidth 
                            >
                                {isBatchGenerating ? `Generating (${batchProgress}%)` : 'Generate & Add Batch'}
                            </Button>
                            {isBatchGenerating && (
                                <Box sx={{ width: '100%', mt: 0.5 }}>
                                    <LinearProgress variant="determinate" value={batchProgress} />
                                </Box>
                            )}
                            {batchError && (
                                <Alert severity="error" sx={{ mt: 1 }} variant="outlined">{batchError}</Alert>
                            )}
                             {!indexStats && selectedIndex && <Typography variant="caption" color="text.secondary">Loading index details for generation...</Typography>}
                        </Box>
                    </Box>
                </DialogContent>
                <DialogActions sx={{ p: '16px 24px', borderTop: (theme) => `1px solid ${theme.palette.divider}` }}>
                    <Button onClick={handleClose} disabled={addStatus === 'adding' || isBatchGenerating}>Cancel</Button>
                    <Button 
                        type="submit" // This submits the single manual vector form
                        variant="contained" 
                        disabled={addStatus === 'adding' || isBatchGenerating || !selectedIndex}
                        startIcon={addStatus === 'adding' ? <CircularProgress size={20} color="inherit" /> : null}
                    >
                        {addStatus === 'adding' ? 'Submitting...' : 'Submit Single Vector'}
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default AddVectorModal;

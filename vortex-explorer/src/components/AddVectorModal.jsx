import React, { useEffect, useState, useRef } from 'react'; // Corrected duplicate imports
import { useForm, Controller } from 'react-hook-form';
import { useDispatch, useSelector } from 'react-redux';
import {
    Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle,
    TextField, CircularProgress, Box, Alert, Typography, Divider, LinearProgress
} from '@mui/material';
import AddLinkIcon from '@mui/icons-material/AddLink';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome'; // For batch generate
import BackupTableIcon from '@mui/icons-material/BackupTable'; // For batch JSON paste
import FileUploadIcon from '@mui/icons-material/FileUpload'; // For file upload
import toast from 'react-hot-toast';
import { 
    addVector, 
    selectGlobalAddVectorStatus, 
    selectGlobalAddVectorError, 
    resetAddVectorStatus,
    batchAddVectors,
    selectGlobalBatchAddStatus,
    selectGlobalBatchAddError,
    resetBatchAddStatus
} from '../features/vectors/vectorsSlice';
import { 
    selectSelectedIndices, 
    fetchIndexStats, 
    selectIndexStats,
    selectIndexStatsStatus // Added for fix
} from '../features/indices/indicesSlice'; 

function AddVectorModal() {
    const dispatch = useDispatch();
    const [open, setOpen] = React.useState(false);
    
    const selectedIndex = useSelector(selectSelectedIndices);
    const indexStats = useSelector(selectIndexStats); 
    const indexStatsStatus = useSelector(selectIndexStatsStatus); // Added for fix
    const addStatus = useSelector(selectGlobalAddVectorStatus); 
    const addError = useSelector(selectGlobalAddVectorError);   
    const batchAddStatus = useSelector(selectGlobalBatchAddStatus);
    const batchAddError = useSelector(selectGlobalBatchAddError);

    const [isBatchGenerating, setIsBatchGenerating] = useState(false);
    const [batchProgress, setBatchProgress] = useState(0);
    const [batchGenError, setBatchGenError] = useState(null); 
    const [numVectorsToGenerate, setNumVectorsToGenerate] = useState(10);

    // State for file upload
    const fileInputRef = useRef(null);
    const [selectedFile, setSelectedFile] = useState(null);
    const [parsedFileVectors, setParsedFileVectors] = useState(null);
    const [fileError, setFileError] = useState(null);

    const { control, handleSubmit, formState: { errors }, reset, setError, setValue, getValues } = useForm({
        defaultValues: {
            vectorId: '',
            vectorDataString: '',
            metadataString: '', 
            batchJsonString: '', 
        },
    });

    useEffect(() => {
        // Fetch if modal is open, an index is selected, and
        // (we don't have stats OR the last attempt failed OR status is idle, and not currently loading)
        if (open && selectedIndex && (!indexStats || indexStatsStatus === 'idle' || indexStatsStatus === 'failed') && indexStatsStatus !== 'loading') {
            dispatch(fetchIndexStats(selectedIndex));
        }
    }, [open, selectedIndex, indexStats, indexStatsStatus, dispatch]); // Added indexStatsStatus
    
    useEffect(() => {
        if (!selectedIndex) {
            reset({ vectorId: '', vectorDataString: '', metadataString: '', batchJsonString: '' });
        } else {
             reset({ vectorId: '', vectorDataString: '', metadataString: '', batchJsonString: '' });
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
        if (addStatus !== 'adding' && batchAddStatus !== 'adding' && !isBatchGenerating) {
            reset({ vectorId: '', vectorDataString: '', metadataString: '', batchJsonString: '' });
            dispatch(resetAddVectorStatus());
            dispatch(resetBatchAddStatus());
        }
    };

    const onSingleSubmit = (data) => {
        if (!selectedIndex) return;

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

        let parsedMetadata;
        if (data.metadataString && data.metadataString.trim() !== '') {
            try {
                parsedMetadata = JSON.parse(data.metadataString);
            } catch (e) {
                setError('metadataString', { type: 'manual', message: 'Invalid JSON format for metadata.' });
                return;
            }
        }

        const payload = {
            indexName: selectedIndex,
            vectorId: data.vectorId,
            vectorData: parsedVectorData,
            metadata: parsedMetadata,
        };
        dispatch(addVector(payload));
    };
    
    useEffect(() => {
        if (addStatus === 'succeeded') {
            toast.success('Vector added/updated successfully!');
            if (selectedIndex) {
                dispatch(fetchIndexStats(selectedIndex));
            }
            if (!isBatchGenerating && batchAddStatus !== 'adding') { 
                handleClose();
            }
        } else if (addStatus === 'failed') {
            toast.error(`Failed to add/update vector: ${addError}`);
        }
    }, [addStatus, addError, dispatch, selectedIndex, isBatchGenerating, batchAddStatus]);

    const handleBatchJsonSubmit = async () => { // Removed handleSubmit wrapper
        if (!selectedIndex) return;
        const jsonData = getValues('batchJsonString');
        if (!jsonData || jsonData.trim() === '') {
            setError('batchJsonString', { type: 'manual', message: 'JSON data cannot be empty.' });
            return;
        }

        let parsedVectors;
        try {
            parsedVectors = JSON.parse(jsonData);
            if (!Array.isArray(parsedVectors)) {
                throw new Error('Input must be a JSON array of vector objects.');
            }
            if (parsedVectors.length === 0) {
                 throw new Error('JSON array cannot be empty.');
            }
            for (const item of parsedVectors) {
                if (typeof item.id !== 'string' || !Array.isArray(item.vector)) {
                    throw new Error('Each item in array must have an "id" (string) and "vector" (array).');
                }
                if (item.metadata !== undefined && (typeof item.metadata !== 'object' || item.metadata === null)) {
                    throw new Error(`Metadata for item "${item.id}" must be an object if provided.`);
                }
            }
        } catch (e) {
            setError('batchJsonString', { type: 'manual', message: e.message || 'Invalid JSON format or structure.' });
            toast.error(e.message || 'Invalid JSON format or structure.');
            return;
        }
        dispatch(batchAddVectors({ indexName: selectedIndex, vectors: parsedVectors }));
    };

    useEffect(() => {
        if (batchAddStatus === 'succeeded') {
            toast.success(`Batch add operation processed.`); 
            setValue('batchJsonString', ''); 
            dispatch(resetBatchAddStatus());
        } else if (batchAddStatus === 'failed') {
            toast.error(`Batch add failed: ${batchAddError}`);
        }
    }, [batchAddStatus, batchAddError, dispatch, setValue]);


    const handleBatchGenerate = async () => {
        if (!selectedIndex || !indexStats || !indexStats.dimensions) {
            toast.error("Index dimensions not available. Select an index first.");
            return;
        }
        setIsBatchGenerating(true);
        setBatchProgress(0);
        setBatchGenError(null);
        let vectorsAdded = 0;
        const totalVectors = parseInt(numVectorsToGenerate, 10);

        if (isNaN(totalVectors) || totalVectors <= 0) {
            setBatchGenError("Number of vectors must be a positive integer.");
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
                await dispatch(addVector({ indexName: selectedIndex, vectorId, vectorData })).unwrap();
                vectorsAdded++;
                setBatchProgress(Math.round((vectorsAdded / totalVectors) * 100));
            }
            toast.success(`${vectorsAdded} synthetic vectors added to "${selectedIndex}".`);
            dispatch(fetchIndexStats(selectedIndex));
        } catch (error) {
            console.error("Batch synthetic data generation failed:", error);
            setBatchGenError(error.message || "Failed during batch generation.");
            toast.error(`Error during batch generation: ${error.message || 'Unknown error'}`);
        } finally {
            setIsBatchGenerating(false);
        }
    };

    const anyOperationInProgress = addStatus === 'adding' || batchAddStatus === 'adding' || isBatchGenerating;

    const handleFileChange = (event) => {
        const file = event.target.files[0];
        if (!file) {
            setSelectedFile(null);
            setParsedFileVectors(null);
            setFileError(null);
            return;
        }

        if (file.type !== "application/json") {
            setFileError("Invalid file type. Please upload a .json file.");
            setSelectedFile(null);
            setParsedFileVectors(null);
            toast.error("Invalid file type. Only .json files are accepted.");
            event.target.value = null; // Clear the input
            return;
        }

        setSelectedFile(file);
        setFileError(null);
        setParsedFileVectors(null); // Reset parsed data until file is read

        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const content = JSON.parse(e.target.result);
                if (!Array.isArray(content)) {
                    throw new Error("JSON content must be an array of vector objects.");
                }
                if (content.length === 0) {
                    throw new Error("JSON array cannot be empty.");
                }
                // Basic validation for each item
                for (const item of content) {
                    if (typeof item.id !== 'string' || !Array.isArray(item.vector)) {
                        throw new Error("Each item in array must have an 'id' (string) and 'vector' (array of numbers).");
                    }
                    if (item.metadata !== undefined && (typeof item.metadata !== 'object' || item.metadata === null)) {
                        throw new Error(`Metadata for item "${item.id}" must be an object if provided.`);
                    }
                }
                setParsedFileVectors(content);
                toast.success(`File "${file.name}" read successfully. Ready to submit.`);
            } catch (err) {
                setFileError(err.message || "Invalid JSON content in file.");
                setSelectedFile(null); // Clear file if content is bad
                setParsedFileVectors(null);
                toast.error(err.message || "Invalid JSON content in file.");
            }
        };
        reader.onerror = () => {
            setFileError("Error reading file.");
            setSelectedFile(null);
            setParsedFileVectors(null);
            toast.error("Error reading file.");
        };
        reader.readAsText(file);
        event.target.value = null; // Allows selecting the same file again
    };

    const handleFileUploadSubmit = async () => {
        if (!parsedFileVectors) {
            toast.error("No valid file content to submit. Please select and ensure the JSON file is correct.");
            return;
        }
        if (!selectedIndex) {
            toast.error("No index selected for batch add.");
            return;
        }
        dispatch(batchAddVectors({ indexName: selectedIndex, vectors: parsedFileVectors }));
        // Reset file states after attempting submission, or in useEffect based on batchAddStatus
    };
    
    // Update useEffect for batchAddStatus to also reset file states
    useEffect(() => {
        if (batchAddStatus === 'succeeded') {
            // Assuming the thunk returns { response: { success_count, failure_count, message } }
            // This part needs to access the actual response from action.payload if the thunk is modified to return it.
            // For now, using a generic success message and relying on the thunk's console log for details.
            toast.success(`Batch operation processed.`); // Simplified toast
            setValue('batchJsonString', ''); // Clear paste input
            setSelectedFile(null); // Clear selected file
            setParsedFileVectors(null); // Clear parsed file content
            setFileError(null); // Clear file error
            dispatch(resetBatchAddStatus());
        } else if (batchAddStatus === 'failed') {
            toast.error(`Batch operation failed: ${batchAddError}`);
            // Do not clear file on failure, user might want to inspect
        }
    }, [batchAddStatus, batchAddError, dispatch, setValue]);


    return (
        <div>
            <Button
                variant="outlined"
                startIcon={<AddLinkIcon />}
                onClick={handleClickOpen}
                sx={{ mt: 1, width: '100%' }}
                disabled={!selectedIndex} 
            >
                Add/Update Vector(s)
            </Button>
            <Dialog open={open} onClose={handleClose} PaperProps={{ component: 'form', onSubmit: handleSubmit(onSingleSubmit) }} maxWidth="md" fullWidth>
                <DialogTitle>Add or Update Vector(s) in "{selectedIndex}"</DialogTitle>
                <DialogContent dividers>
                    {/* Section 1: Manual Single Vector Entry */}
                    <Box sx={{ mb: 3 }}>
                        <DialogContentText sx={{ mb: 2 }}>
                            Manually enter Vector ID, comma-separated data, and optional JSON metadata for a single vector.
                        </DialogContentText>
                        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                            <Controller
                                name="vectorId"
                                control={control}
                                rules={{ required: 'Vector ID is required for manual submission.' }}
                                render={({ field }) => (
                                    <TextField {...field} label="Vector ID" fullWidth autoFocus error={!!errors.vectorId} helperText={errors.vectorId?.message} disabled={anyOperationInProgress} />
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
                                        disabled={anyOperationInProgress}
                                        placeholder="e.g., 0.1, -0.5, 1.2, ..."
                                    />
                                )}
                            />
                            <Controller
                                name="metadataString"
                                control={control}
                                render={({ field }) => (
                                    <TextField 
                                        {...field} 
                                        label="Metadata (JSON format, optional)" 
                                        fullWidth 
                                        multiline 
                                        rows={3} 
                                        error={!!errors.metadataString} 
                                        helperText={errors.metadataString?.message} 
                                        disabled={anyOperationInProgress}
                                        placeholder='e.g., {"key": "value", "source": "doc_123"}'
                                    />
                                )}
                            />
                        </Box>
                        <Button 
                            type="submit" 
                            variant="contained" 
                            sx={{mt: 2}}
                            disabled={anyOperationInProgress || !selectedIndex}
                            startIcon={addStatus === 'adding' ? <CircularProgress size={20} color="inherit" /> : null}
                        >
                            {addStatus === 'adding' ? 'Submitting...' : 'Submit Single Vector'}
                        </Button>
                        {addStatus === 'failed' && addError && (
                            <Alert severity="error" sx={{ mt: 2 }} variant="outlined">{addError}</Alert>
                        )}
                    </Box>

                    <Divider sx={{ my: 3 }}>OR</Divider>

                    {/* Section 2: Batch Add Vectors (JSON Paste) */}
                    <Box sx={{ mb: 3 }}>
                        <DialogContentText sx={{ mb: 2 }}>
                            Paste a JSON array of vector objects to add multiple vectors in a batch. Each object should have `id` (string), `vector` (array of numbers), and optional `metadata` (object).
                        </DialogContentText>
                        <Controller
                            name="batchJsonString"
                            control={control}
                            render={({ field }) => (
                                <TextField
                                    {...field}
                                    label="Batch Vectors JSON Array"
                                    fullWidth
                                    multiline
                                    rows={5}
                                    error={!!errors.batchJsonString}
                                    helperText={errors.batchJsonString?.message || 'Example: [{"id":"v1", "vector":[...], "metadata":{...}}, ... ]'}
                                    disabled={anyOperationInProgress}
                                    placeholder='[{"id":"v1", "vector":[0.1,0.2], "metadata":{"type":"A"}}, {"id":"v2", "vector":[0.3,0.4]}]'
                                />
                            )}
                        />
                        <Button
                            variant="contained"
                            color="info"
                            startIcon={batchAddStatus === 'adding' ? <CircularProgress size={20} color="inherit" /> : <BackupTableIcon />}
                            onClick={handleBatchJsonSubmit} // Changed from handleSubmit(handleBatchJsonSubmit)
                            disabled={anyOperationInProgress || !selectedIndex}
                            sx={{ mt: 2 }}
                        >
                            {batchAddStatus === 'adding' ? 'Submitting Batch...' : 'Submit Batch JSON'}
                        </Button>
                        {batchAddStatus === 'failed' && batchAddError && (
                            <Alert severity="error" sx={{ mt: 2 }} variant="outlined">{batchAddError}</Alert>
                        )}
                    </Box>

                    <Divider sx={{ my: 3 }}>OR</Divider>

                    {/* Section 3: Batch Add from JSON File */}
                    <Box sx={{ mb: 3 }}>
                        <DialogContentText sx={{ mb: 2 }}>
                            Upload a JSON file containing an array of vector objects. Each object should conform to the same structure as for JSON paste.
                        </DialogContentText>
                        <input
                            type="file"
                            id="json-file-upload"
                            accept=".json"
                            style={{ display: 'none' }}
                            ref={fileInputRef}
                            onChange={handleFileChange} // To be implemented
                        />
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1 }}>
                            <Button
                                variant="outlined"
                                component="label"
                                startIcon={<FileUploadIcon />}
                                onClick={() => fileInputRef.current?.click()}
                                disabled={anyOperationInProgress || !selectedIndex}
                            >
                                Select JSON File
                            </Button>
                            {selectedFile && <Typography variant="body2" sx={{ fontStyle: 'italic' }}>{selectedFile.name}</Typography>}
                        </Box>
                        
                        {fileError && (
                            <Alert severity="error" sx={{ mt: 1, mb: 1 }} variant="outlined">{fileError}</Alert>
                        )}

                        <Button
                            variant="contained"
                            color="success" // Different color to distinguish
                            startIcon={batchAddStatus === 'adding' ? <CircularProgress size={20} color="inherit" /> : <BackupTableIcon />} // Re-use icon or new one
                            onClick={handleFileUploadSubmit} // To be implemented
                            disabled={!selectedFile || !!fileError || anyOperationInProgress || !selectedIndex || batchAddStatus === 'adding' || !parsedFileVectors}
                            sx={{ mt: 1 }}
                        >
                            {batchAddStatus === 'adding' ? 'Uploading Batch...' : 'Submit Uploaded File'}
                        </Button>
                         {/* Potential separate status for file upload if needed, or reuse batchAddStatus */}
                    </Box>
                    
                    <Divider sx={{ my: 3 }}>OR</Divider>

                    {/* Section 4: Batch Generate Synthetic Vectors */}
                    <Box>
                        <DialogContentText sx={{ mb: 2 }}>
                            Alternatively, generate and add a batch of synthetic random vectors to this index. Vector IDs will be auto-generated.
                        </DialogContentText>
                        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                            <TextField
                                label="Number of Vectors to Generate"
                                type="number"
                                variant="outlined"
                                size="small"
                                value={numVectorsToGenerate}
                                onChange={(e) => setNumVectorsToGenerate(Math.max(1, parseInt(e.target.value,10) || 1))}
                                disabled={anyOperationInProgress || !selectedIndex || !indexStats}
                                InputProps={{ inputProps: { min: 1 } }}
                                fullWidth 
                            />
                            <Button
                                variant="contained" 
                                color="secondary" 
                                startIcon={isBatchGenerating ? <CircularProgress size={20} color="inherit" /> : <AutoAwesomeIcon />}
                                onClick={handleBatchGenerate}
                                disabled={anyOperationInProgress || !selectedIndex || !indexStats}
                                fullWidth 
                            >
                                {isBatchGenerating ? `Generating (${batchProgress}%)` : 'Generate & Add Batch'}
                            </Button>
                            {isBatchGenerating && (
                                <Box sx={{ width: '100%', mt: 0.5 }}>
                                    <LinearProgress variant="determinate" value={batchProgress} />
                                </Box>
                            )}
                            {batchGenError && (
                                <Alert severity="error" sx={{ mt: 1 }} variant="outlined">{batchGenError}</Alert>
                            )}
                             {!indexStats && selectedIndex && <Typography variant="caption" color="text.secondary">Loading index details for generation...</Typography>}
                        </Box>
                    </Box>
                </DialogContent>
                <DialogActions sx={{ p: '16px 24px', borderTop: (theme) => `1px solid ${theme.palette.divider}` }}>
                    <Button onClick={handleClose} disabled={anyOperationInProgress}>Cancel</Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default AddVectorModal;

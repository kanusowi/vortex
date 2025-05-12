import React, { useEffect } from 'react';
import { useForm, Controller } from 'react-hook-form';
import { useDispatch, useSelector } from 'react-redux';
import {
    Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle,
    TextField, FormControl, InputLabel, Select, MenuItem, CircularProgress, Box, Alert, Typography // Added Typography
} from '@mui/material';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import toast from 'react-hot-toast';
import { createIndex, selectCreateIndexStatus, selectCreateIndexError, resetCreateStatus } from '../features/indices/indicesSlice';

const HNSW_DEFAULT_M = 16;
const HNSW_DEFAULT_EF_CONSTRUCTION = 200;

function CreateIndexModal() {
    const dispatch = useDispatch();
    const [open, setOpen] = React.useState(false);
    const createStatus = useSelector(selectCreateIndexStatus);
    const createError = useSelector(selectCreateIndexError);

    const { control, handleSubmit, formState: { errors }, reset } = useForm({
        defaultValues: {
            indexName: '',
            dimensions: 128,
            metric: 'cosine',
            // HNSW specific config (optional, could be hidden under "Advanced")
            m: HNSW_DEFAULT_M, 
            efConstruction: HNSW_DEFAULT_EF_CONSTRUCTION,
        },
    });

    const handleClickOpen = () => {
        setOpen(true);
    };

    const handleClose = () => {
        setOpen(false);
        // Reset form and Redux status only if not currently creating
        if (createStatus !== 'creating') {
            reset();
            dispatch(resetCreateStatus());
        }
    };

    const onSubmit = (data) => {
        // Construct the HnswConfig object with all required fields
        // Use defaults from vortex-core/src/config.rs for fields not in the form
        const mVal = parseInt(data.m, 10);
        const efConstructionVal = parseInt(data.efConstruction, 10);
        
        const hnswConfigPayload = {
            m: mVal,
            m_max0: mVal * 2, // Default heuristic from HnswConfig::new
            ef_construction: efConstructionVal,
            ef_search: 50, // Default from HnswConfig::default()
            ml: 1.0 / (mVal > 0 ? Math.log(mVal) : Math.log(16)), // Default heuristic, ensure mVal > 0 for log
            seed: null, // Corresponds to Option<u64>::None
        };

        // Map frontend metric value to backend expected value
        let backendMetric = data.metric;
        if (data.metric === 'cosine') backendMetric = 'Cosine';
        if (data.metric === 'euclidean') backendMetric = 'L2';
        // 'dot' is not directly supported by backend enum, handle or remove from FE

        const payload = {
            indexName: data.indexName,
            dimensions: parseInt(data.dimensions, 10),
            metric: backendMetric,
            config: hnswConfigPayload, 
        };
        
        console.log("Dispatching createIndex with payload:", payload);
        dispatch(createIndex(payload));
    };

    useEffect(() => {
        if (createStatus === 'succeeded') {
            toast.success('Index created successfully!');
            handleClose(); // Close modal and reset
        } else if (createStatus === 'failed') {
            toast.error(`Failed to create index: ${createError}`);
            // Don't close modal on error, let user correct and retry or cancel
        }
    }, [createStatus, createError, dispatch, reset]); // Added reset to dependencies

    return (
        <div>
            <Button
                variant="outlined"
                startIcon={<AddCircleOutlineIcon />}
                onClick={handleClickOpen}
                sx={{ mt: 2, width: '100%' }} // Make button full width of its container
            >
                Create New Index
            </Button>
            <Dialog open={open} onClose={handleClose} PaperProps={{ component: 'form', onSubmit: handleSubmit(onSubmit) }}>
                <DialogTitle>Create New Vector Index</DialogTitle>
                <DialogContent>
                    <DialogContentText sx={{ mb: 2 }}>
                        Configure the details for your new vector index.
                    </DialogContentText>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        <Controller
                            name="indexName"
                            control={control}
                            rules={{ required: 'Index name is required.' }}
                            render={({ field }) => (
                                <TextField {...field} label="Index Name" fullWidth autoFocus error={!!errors.indexName} helperText={errors.indexName?.message} disabled={createStatus === 'creating'} />
                            )}
                        />
                        <Controller
                            name="dimensions"
                            control={control}
                            rules={{ required: 'Dimensions are required.', min: { value: 1, message: 'Must be at least 1' } }}
                            render={({ field }) => (
                                <TextField {...field} label="Dimensions" type="number" fullWidth error={!!errors.dimensions} helperText={errors.dimensions?.message} disabled={createStatus === 'creating'} InputProps={{ inputProps: { min: 1 } }}/>
                            )}
                        />
                        <FormControl fullWidth error={!!errors.metric} disabled={createStatus === 'creating'}>
                            <InputLabel id="metric-select-label">Distance Metric</InputLabel>
                            <Controller
                                name="metric"
                                control={control}
                                rules={{ required: 'Metric is required.' }}
                                render={({ field }) => (
                                    <Select {...field} labelId="metric-select-label" label="Distance Metric">
                                        <MenuItem value="cosine">Cosine</MenuItem>
                                        <MenuItem value="euclidean">L2 (Euclidean)</MenuItem>
                                        {/* <MenuItem value="dot">Dot Product</MenuItem> // Dot product not directly supported by backend enum */}
                                    </Select>
                                )}
                            />
                            {errors.metric && <Typography color="error" variant="caption" sx={{ml:1.5}}>{errors.metric.message}</Typography>}
                        </FormControl>
                        
                        <Typography variant="caption" color="text.secondary" sx={{mt:1}}>HNSW Configuration (Optional):</Typography>
                        <Controller
                            name="m"
                            control={control}
                            rules={{ min: { value: 2, message: 'M must be at least 2' } }}
                            render={({ field }) => (
                                <TextField {...field} label="M (Max Connections)" type="number" fullWidth error={!!errors.m} helperText={errors.m?.message} disabled={createStatus === 'creating'} InputProps={{ inputProps: { min: 2 } }} />
                            )}
                        />
                        <Controller
                            name="efConstruction"
                            control={control}
                            rules={{ min: { value: 10, message: 'efConstruction must be at least 10' } }}
                            render={({ field }) => (
                                <TextField {...field} label="efConstruction (Search Quality)" type="number" fullWidth error={!!errors.efConstruction} helperText={errors.efConstruction?.message} disabled={createStatus === 'creating'} InputProps={{ inputProps: { min: 10 } }} />
                            )}
                        />
                    </Box>
                    {createStatus === 'failed' && createError && (
                         <Alert severity="error" sx={{ mt: 2 }}>{createError}</Alert>
                    )}
                </DialogContent>
                <DialogActions sx={{ p: '16px 24px' }}>
                    <Button onClick={handleClose} disabled={createStatus === 'creating'}>Cancel</Button>
                    <Button 
                        type="submit" 
                        variant="contained" 
                        disabled={createStatus === 'creating'}
                        startIcon={createStatus === 'creating' ? <CircularProgress size={20} color="inherit" /> : null}
                    >
                        {createStatus === 'creating' ? 'Creating...' : 'Create Index'}
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default CreateIndexModal;

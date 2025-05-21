import React, { useEffect, useState } from 'react'; // Added useState
import { useForm, Controller } from 'react-hook-form';
import { useDispatch, useSelector } from 'react-redux';
import {
    Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle,
    TextField, FormControl, InputLabel, Select, MenuItem, CircularProgress, Box, Alert, Typography,
    Accordion, AccordionSummary, AccordionDetails, Tooltip, IconButton // Added Accordion components and Tooltip
} from '@mui/material';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'; // For Accordion
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined'; // For tooltips
import toast from 'react-hot-toast';
import { createIndex, selectCreateIndexStatus, selectCreateIndexError, resetCreateStatus } from '../features/indices/indicesSlice';

const HNSW_DEFAULT_M = 16;
const HNSW_DEFAULT_EF_CONSTRUCTION = 200;
const HNSW_DEFAULT_EF_SEARCH = 50;

function CreateIndexModal() {
    const dispatch = useDispatch();
    const [open, setOpen] = useState(false); // Use useState from React
    const [advancedOpen, setAdvancedOpen] = useState(false); // State for accordion
    const createStatus = useSelector(selectCreateIndexStatus);
    const createError = useSelector(selectCreateIndexError);

    const { control, handleSubmit, formState: { errors }, reset, watch } = useForm({ // Added watch
        defaultValues: {
            indexName: '',
            dimensions: 128,
            metric: 'cosine',
            m: HNSW_DEFAULT_M,
            efConstruction: HNSW_DEFAULT_EF_CONSTRUCTION,
            // Advanced fields
            mMax0: '', // Default to empty, will derive from m if not set
            efSearch: HNSW_DEFAULT_EF_SEARCH,
            ml: '', // Default to empty, will derive from m if not set
            seed: '', // Default to empty for null/None
        },
    });

    const mValue = watch('m'); // Watch m value for dynamic defaults

    const handleClickOpen = () => {
        setOpen(true);
    };

    const handleClose = () => {
        setOpen(false);
        if (createStatus !== 'creating') {
            reset(); // Reset form to defaultValues
            dispatch(resetCreateStatus());
            setAdvancedOpen(false); // Close accordion
        }
    };

    const onSubmit = (data) => {
        const mVal = parseInt(data.m, 10);
        const efConstructionVal = parseInt(data.efConstruction, 10);

        // Advanced fields parsing
        const mMax0Val = data.mMax0 && data.mMax0.trim() !== '' ? parseInt(data.mMax0, 10) : mVal * 2;
        const efSearchVal = data.efSearch && data.efSearch.toString().trim() !== '' ? parseInt(data.efSearch, 10) : HNSW_DEFAULT_EF_SEARCH;
        const mlVal = data.ml && data.ml.trim() !== '' ? parseFloat(data.ml) : (1.0 / (mVal > 0 ? Math.log(mVal) : Math.log(HNSW_DEFAULT_M)));
        const seedVal = data.seed && data.seed.trim() !== '' ? parseInt(data.seed, 10) : null;
        
        const hnswConfigPayload = {
            vector_dim: parseInt(data.dimensions, 10), // Add vector_dim here
            m: mVal,
            m_max0: mMax0Val,
            ef_construction: efConstructionVal,
            ef_search: efSearchVal,
            ml: mlVal,
            seed: seedVal,
        };

        let backendMetric = data.metric;
        if (data.metric === 'cosine') backendMetric = 'Cosine';
        if (data.metric === 'euclidean') backendMetric = 'L2';
        // 'dot' is not directly supported by backend enum, handle or remove from FE

        const payload = {
            indexName: data.indexName,
            dimensions: parseInt(data.dimensions, 10), // Add back top-level dimensions
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
                        
                        <Typography variant="subtitle2" color="text.primary" sx={{mt: 2, mb: 1}}>Basic HNSW Configuration:</Typography>
                        <Controller
                            name="m"
                            control={control}
                            rules={{ required: 'M is required', min: { value: 2, message: 'M must be at least 2' } }}
                            render={({ field }) => (
                                <Tooltip title="Maximum number of connections per node on layers > 0. Higher values increase build time and memory but can improve recall.">
                                    <TextField {...field} label="M (Max Connections)" type="number" fullWidth error={!!errors.m} helperText={errors.m?.message} disabled={createStatus === 'creating'} InputProps={{ inputProps: { min: 2 } }} />
                                </Tooltip>
                            )}
                        />
                        <Controller
                            name="efConstruction"
                            control={control}
                            rules={{ required: 'efConstruction is required', min: { value: 10, message: 'efConstruction must be at least 10' } }}
                            render={({ field }) => (
                                <Tooltip title="Size of the dynamic candidate list during index construction. Higher values improve index quality/recall at the cost of slower build times.">
                                    <TextField {...field} label="efConstruction (Build Quality)" type="number" fullWidth error={!!errors.efConstruction} helperText={errors.efConstruction?.message} disabled={createStatus === 'creating'} InputProps={{ inputProps: { min: 10 } }} />
                                </Tooltip>
                            )}
                        />

                        <Accordion expanded={advancedOpen} onChange={() => setAdvancedOpen(!advancedOpen)} sx={{ mt: 2, boxShadow: 'none', '&:before': { display: 'none' }, border: `1px solid ${errors.mMax0 || errors.efSearch || errors.ml || errors.seed ? 'red' : 'rgba(0, 0, 0, 0.23)'}`, borderRadius: 1 }}>
                            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                                <Typography>Advanced HNSW Configuration (Optional)</Typography>
                            </AccordionSummary>
                            <AccordionDetails sx={{ display: 'flex', flexDirection: 'column', gap: 2, pt:0 }}>
                                <Controller
                                    name="mMax0"
                                    control={control}
                                    rules={{ 
                                        validate: value => (value === '' || parseInt(value,10) >= (mValue || HNSW_DEFAULT_M)) || `Must be >= M (${mValue || HNSW_DEFAULT_M})`,
                                        min: { value: 2, message: 'Must be at least 2 if set' }
                                    }}
                                    render={({ field }) => (
                                        <Tooltip title={`Max connections for layer 0. Defaults to 2 * M (currently ${ (mValue || HNSW_DEFAULT_M) * 2}). Higher values can improve recall at layer 0.`}>
                                            <TextField {...field} label="M_max0 (Layer 0 Max Connections)" placeholder={`Defaults to ${ (mValue || HNSW_DEFAULT_M) * 2}`} type="number" fullWidth error={!!errors.mMax0} helperText={errors.mMax0?.message} disabled={createStatus === 'creating'} InputProps={{ inputProps: { min: 2 } }} />
                                        </Tooltip>
                                    )}
                                />
                                <Controller
                                    name="efSearch"
                                    control={control}
                                    rules={{ required: 'efSearch is required', min: { value: 1, message: 'efSearch must be at least 1' } }}
                                    render={({ field }) => (
                                        <Tooltip title="Size of the dynamic candidate list during search. Higher values improve search recall but increase search latency. Must be >= k (number of neighbors to find).">
                                            <TextField {...field} label="efSearch (Search Quality/Speed)" type="number" fullWidth error={!!errors.efSearch} helperText={errors.efSearch?.message} disabled={createStatus === 'creating'} InputProps={{ inputProps: { min: 1 } }} />
                                        </Tooltip>
                                    )}
                                />
                                <Controller
                                    name="ml"
                                    control={control}
                                    rules={{ validate: value => (value === '' || parseFloat(value) > 0) || 'Must be > 0 if set' }}
                                    render={({ field }) => (
                                        <Tooltip title={`Normalization factor for level generation. Defaults based on M (approx ${ (1.0 / Math.log(mValue || HNSW_DEFAULT_M)).toFixed(4) }). Affects the probability distribution of node levels.`}>
                                            <TextField {...field} label="mL Factor (Level Generation)" placeholder={`Defaults based on M (approx ${ (1.0 / Math.log(mValue || HNSW_DEFAULT_M)).toFixed(4) })`} type="number" step="0.01" fullWidth error={!!errors.ml} helperText={errors.ml?.message} disabled={createStatus === 'creating'} InputProps={{ inputProps: { min: 0.0001 } }} />
                                        </Tooltip>
                                    )}
                                />
                                <Controller
                                    name="seed"
                                    control={control}
                                    rules={{ validate: value => (value === '' || Number.isInteger(Number(value))) || 'Must be an integer if set' }}
                                    render={({ field }) => (
                                        <Tooltip title="Optional integer seed for the random number generator to ensure reproducible index builds. Leave empty for random seed.">
                                            <TextField {...field} label="Seed (Optional, for Reproducibility)" placeholder="Leave empty for random" type="number" fullWidth error={!!errors.seed} helperText={errors.seed?.message} disabled={createStatus === 'creating'} />
                                        </Tooltip>
                                    )}
                                />
                            </AccordionDetails>
                        </Accordion>
                    </Box>
                    {createStatus === 'failed' && createError && (
                         <Alert severity="error" sx={{ mt: 2 }} variant="outlined">{createError}</Alert>
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

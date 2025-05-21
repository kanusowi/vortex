import React, { useMemo } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { Box, FormControl, InputLabel, Select, MenuItem, Typography } from '@mui/material';
import { selectSelectedIndices } from '../features/indices/indicesSlice';
import { 
    selectDataForIndex, 
    selectColorByMetadataField, 
    setColorByMetadataField 
} from '../features/vectors/vectorsSlice';

const PlotControls = () => {
    const dispatch = useDispatch();
    const selectedIndexName = useSelector(selectSelectedIndices);
    const indexData = useSelector(selectDataForIndex(selectedIndexName));
    const currentColorByField = useSelector(selectColorByMetadataField(selectedIndexName));

    const availableMetadataFields = useMemo(() => {
        if (!indexData || !indexData.rawVectors || indexData.rawVectors.length === 0) {
            return [];
        }
        const fieldSet = new Set();
        indexData.rawVectors.forEach(point => {
            if (point.metadata) {
                Object.keys(point.metadata).forEach(key => fieldSet.add(key));
            }
        });
        return Array.from(fieldSet).sort();
    }, [indexData]);

    const handleChange = (event) => {
        const fieldName = event.target.value;
        dispatch(setColorByMetadataField({ 
            indexName: selectedIndexName, 
            fieldName: fieldName === "none" ? null : fieldName 
        }));
    };

    if (!selectedIndexName || availableMetadataFields.length === 0) {
        return (
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                No metadata fields available for color encoding in the current sample.
            </Typography>
        );
    }

    return (
        <Box sx={{ mt: 2, mb: 1 }}>
            <FormControl fullWidth size="small">
                <InputLabel id="color-by-metadata-label">Color Points By Metadata</InputLabel>
                <Select
                    labelId="color-by-metadata-label"
                    id="color-by-metadata-select"
                    value={currentColorByField || "none"}
                    label="Color Points By Metadata"
                    onChange={handleChange}
                >
                    <MenuItem value="none">
                        <em>-- Default Color --</em>
                    </MenuItem>
                    {availableMetadataFields.map((field) => (
                        <MenuItem key={field} value={field}>
                            {field}
                        </MenuItem>
                    ))}
                </Select>
            </FormControl>
        </Box>
    );
};

export default PlotControls;

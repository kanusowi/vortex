import React, { useState, useEffect } from 'react';
import { Box, TextField, IconButton, Button, Typography, Paper, Tooltip } from '@mui/material';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import { v4 as uuidv4 } from 'uuid'; // For unique keys for conditions

const QueryBuilderChunk1 = ({ onFilterChange, initialFilter = {} }) => {
    const [conditions, setConditions] = useState([]);

    // Initialize conditions from initialFilter (e.g., when switching back from JSON view)
    // This effect runs when initialFilter (from parent, derived from watchedFilterString) changes.
    useEffect(() => {
        const newConditionsFromProp = Object.entries(initialFilter || {}).map(([field, value]) => ({
            // field and value, id will be added if we decide to update state
            field: field,
            value: String(value), 
        }));

        let areConditionsDifferent = false;
        if (newConditionsFromProp.length !== conditions.length) {
            areConditionsDifferent = true;
        } else {
            for (let i = 0; i < newConditionsFromProp.length; i++) {
                if (newConditionsFromProp[i].field !== conditions[i].field || 
                    newConditionsFromProp[i].value !== conditions[i].value) {
                    areConditionsDifferent = true;
                    break;
                }
            }
        }
        // Also handle the case where current conditions are the single empty default
        // and newConditionsFromProp is also empty (meaning initialFilter was {}).
        if (conditions.length === 1 && conditions[0].field === '' && conditions[0].value === '' && newConditionsFromProp.length === 0) {
            // If initialFilter is empty and current conditions are the default empty one, don't update.
            // This prevents a loop if the user clears the filter in JSON mode.
             areConditionsDifferent = false;
        }


        if (areConditionsDifferent) {
            // To prevent loops, only update if the stringified version of initialFilter
            // is different from the stringified version of the current conditions.
            const currentFilterForCompare = {};
            conditions.forEach(cond => {
                if (cond.field.trim() !== '') {
                    currentFilterForCompare[cond.field.trim()] = cond.value;
                }
            });

            if (JSON.stringify(initialFilter || {}) !== JSON.stringify(currentFilterForCompare)) {
                if (newConditionsFromProp.length === 0) {
                    setConditions([{ id: uuidv4(), field: '', value: '' }]);
                } else {
                    setConditions(newConditionsFromProp.map(c => ({ ...c, id: uuidv4() })));
                }
            }
        }
    }, [initialFilter]); // initialFilter is memoized in parent

    // This effect runs when conditions state (managed by this component) changes.
    // It calls onFilterChange to update the parent (SearchControl).
    useEffect(() => {
        const newFilter = {};
        conditions.forEach(cond => {
            if (cond.field.trim() !== '') { // Only include conditions with a field name
                newFilter[cond.field.trim()] = cond.value;
            }
        });
        // Only call onFilterChange if the generated filter is different from initialFilter
        // to prevent loops if initialFilter caused conditions to reset to a state
        // that would generate the same initialFilter.
        if (JSON.stringify(newFilter) !== JSON.stringify(initialFilter || {})) {
            onFilterChange(newFilter);
        }
    }, [conditions, onFilterChange, initialFilter]);

    const handleAddCondition = () => {
        setConditions([...conditions, { id: uuidv4(), field: '', value: '' }]);
    };

    const handleRemoveCondition = (id) => {
        setConditions(conditions.filter(cond => cond.id !== id));
    };

    const handleChange = (id, event) => {
        const { name, value } = event.target;
        setConditions(
            conditions.map(cond =>
                cond.id === id ? { ...cond, [name]: value } : cond
            )
        );
    };

    return (
        <Paper variant="outlined" sx={{ p: 2, mt: 1 }}>
            <Typography variant="subtitle2" gutterBottom>
                Filter Conditions (AND logic)
            </Typography>
            {conditions.map((condition, index) => (
                <Box key={condition.id} sx={{ display: 'flex', alignItems: 'center', mb: 1.5, gap: 1 }}>
                    <TextField
                        name="field"
                        label="Field Name"
                        variant="outlined"
                        size="small"
                        value={condition.field}
                        onChange={(e) => handleChange(condition.id, e)}
                        sx={{ flexGrow: 1 }}
                    />
                    <Typography variant="body2" sx={{ px: 0.5 }}>is equal to</Typography>
                    <TextField
                        name="value"
                        label="Value"
                        variant="outlined"
                        size="small"
                        value={condition.value}
                        onChange={(e) => handleChange(condition.id, e)}
                        sx={{ flexGrow: 1 }}
                    />
                    <Tooltip title="Remove condition">
                        <span>
                            <IconButton 
                                onClick={() => handleRemoveCondition(condition.id)} 
                                size="small" 
                                disabled={conditions.length === 1 && index === 0} // Disable remove for the last item if it's the only one
                            >
                                <RemoveCircleOutlineIcon />
                            </IconButton>
                        </span>
                    </Tooltip>
                </Box>
            ))}
            <Button
                variant="outlined"
                size="small"
                startIcon={<AddCircleOutlineIcon />}
                onClick={handleAddCondition}
                sx={{ mt: 1 }}
            >
                Add Condition
            </Button>
        </Paper>
    );
};

export default QueryBuilderChunk1;

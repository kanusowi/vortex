import React, { useMemo } from 'react';
import Plot from 'react-plotly.js'; 
import { useSelector, useDispatch } from 'react-redux'; // Added useDispatch
import { 
    selectReducedCoordinates, 
    selectVectorDataStatus, 
    selectRawVectors,
    selectSearchResults,
    selectSelectedPlotPointId, 
    setSelectedPlotPointId,
    selectColorByMetadataField // Added selector for color by field
} from '../features/vectors/vectorsSlice';

// Define colors for highlighting
const DEFAULT_COLOR = 'rgba(31, 119, 180, 0.7)'; 
const HIGHLIGHT_COLOR = 'rgba(255, 127, 14, 1.0)'; 
const SELECTED_COLOR = 'rgba(214, 39, 40, 1.0)'; 
const DEFAULT_SIZE = 6;
const HIGHLIGHT_SIZE = 9;
const SELECTED_SIZE = 12;
const SELECTED_BORDER_WIDTH = 2;
const SELECTED_BORDER_COLOR = 'rgba(0, 0, 0, 1.0)';
const MISSING_VALUE_COLOR = 'rgba(200, 200, 200, 0.5)';
const CATEGORICAL_PALETTE = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'];


function Plotter({ indexName }) {
    const dispatch = useDispatch(); 
    const reducedCoords = useSelector(selectReducedCoordinates(indexName)); 
    const rawVectorsData = useSelector(selectRawVectors(indexName)); 
    const searchResults = useSelector(selectSearchResults(indexName));
    const selectedPlotPointId = useSelector(selectSelectedPlotPointId(indexName));
    const colorByField = useSelector(selectColorByMetadataField(indexName));

    // Memoize plot data generation
    const plotData = useMemo(() => {
        if (!reducedCoords || reducedCoords.length === 0 || !rawVectorsData || rawVectorsData.length === 0) {
            return null;
        }

        const vectorIds = rawVectorsData.map(item => item.id);
        if (vectorIds.length !== reducedCoords.length) {
             console.warn("Mismatch between rawVectorsData IDs and reduced coordinates count.");
        }

        const searchHighlightedIds = new Set(searchResults?.map(item => item.id) || []);
        
        let pointColors = [];
        let markerSizeArray = [];
        let markerBorderWidthArray = [];
        let markerBorderColorArray = [];
        let traceMarkerConfig = {};
        let showLegendForColorBy = false;

        if (colorByField) {
            const values = rawVectorsData.map(p => p.metadata?.[colorByField]);
            const uniqueValues = [...new Set(values.filter(v => v !== undefined && v !== null))];
            
            // Simple heuristic: if more than 10 unique values or if any value is not a number, treat as categorical.
            // This can be improved.
            const isNumeric = uniqueValues.length > 0 && uniqueValues.every(v => typeof v === 'number') && uniqueValues.length > 10;

            if (isNumeric) {
                const numericValues = values.map(v => typeof v === 'number' ? v : undefined); // Keep undefined for non-numbers
                const min = Math.min(...numericValues.filter(v => v !== undefined));
                const max = Math.max(...numericValues.filter(v => v !== undefined));
                
                pointColors = numericValues.map(v => v === undefined ? MISSING_VALUE_COLOR : v); // Pass raw numbers to plotly for colorscale
                traceMarkerConfig = {
                    color: pointColors,
                    colorscale: 'Viridis', // Example colorscale
                    showscale: true, // Show color bar legend
                    colorbar: { title: colorByField, titleside: 'right' },
                };
                showLegendForColorBy = true;
            } else { // Categorical
                const valueToColorMap = new Map();
                uniqueValues.forEach((val, i) => {
                    valueToColorMap.set(val, CATEGORICAL_PALETTE[i % CATEGORICAL_PALETTE.length]);
                });
                pointColors = values.map(v => valueToColorMap.get(v) || MISSING_VALUE_COLOR);
                traceMarkerConfig = { color: pointColors };
                // For categorical, Plotly might generate a legend if we create separate traces,
                // or we might need a custom legend component. For now, hover text will be key.
                // showLegendForColorBy = uniqueValues.length <= CATEGORICAL_PALETTE.length; // Basic legend for few categories
            }
        }

        rawVectorsData.forEach((point, i) => {
            const id = point.id;
            if (id === selectedPlotPointId) {
                markerSizeArray.push(SELECTED_SIZE);
                markerBorderWidthArray.push(SELECTED_BORDER_WIDTH);
                markerBorderColorArray.push(SELECTED_BORDER_COLOR);
                if (!colorByField) pointColors.push(SELECTED_COLOR);
                else if (pointColors[i] === undefined) pointColors[i] = SELECTED_COLOR; // Ensure selected color if no metadata color
            } else if (searchHighlightedIds.has(id)) {
                markerSizeArray.push(HIGHLIGHT_SIZE);
                markerBorderWidthArray.push(1);
                markerBorderColorArray.push('rgba(0, 0, 0, 0.7)');
                if (!colorByField) pointColors.push(HIGHLIGHT_COLOR);
                else if (pointColors[i] === undefined) pointColors[i] = HIGHLIGHT_COLOR;
            } else {
                markerSizeArray.push(DEFAULT_SIZE);
                markerBorderWidthArray.push(0);
                markerBorderColorArray.push('rgba(0,0,0,0)');
                if (!colorByField) pointColors.push(DEFAULT_COLOR);
                else if (pointColors[i] === undefined) pointColors[i] = DEFAULT_COLOR;
            }
        });
        
        if (!colorByField && pointColors.length === 0 && rawVectorsData.length > 0) { // Default coloring if no colorByField and not handled above
             pointColors = rawVectorsData.map(() => DEFAULT_COLOR);
        }


        const trace = {
            x: reducedCoords.map(p => p[0]),
            y: reducedCoords.map(p => p[1]),
            mode: 'markers',
            type: 'scattergl',
            marker: {
                size: markerSizeArray,
                line: {
                    width: markerBorderWidthArray,
                    color: markerBorderColorArray,
                },
                ...traceMarkerConfig, // Spread color/colorscale config
                color: pointColors, // Ensure this is always assigned
            },
            text: rawVectorsData.map(p => {
                let hoverText = `ID: ${p.id}`;
                const searchRes = searchResults?.find(r => r.id === p.id);
                if (searchRes) {
                    hoverText += `<br>Score: ${searchRes.score.toFixed(4)}`;
                }
                if (colorByField && p.metadata?.[colorByField] !== undefined) {
                    hoverText += `<br>${colorByField}: ${p.metadata[colorByField]}`;
                }
                return hoverText;
            }),
            hoverinfo: 'text',
            name: indexName,
            customdata: vectorIds,
        };
        return [trace];
    }, [reducedCoords, rawVectorsData, indexName, searchResults, selectedPlotPointId, colorByField]);

    const layout = useMemo(() => ({
        hovermode: 'closest',
        showlegend: false, // Default to false, colorbar for numeric is part of marker
        xaxis: { title: 'UMAP Dimension 1', zeroline: false, showgrid: false },
        yaxis: { title: 'UMAP Dimension 2', zeroline: false, showgrid: false },
        margin: { l: 40, r: 20, b: 40, t: 20, pad: 4 },
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        // legend: { traceorder: 'normal' } // May need if doing categorical legend manually
    }), []);

    // Render conditions are now handled by VisualizationWorkspace
    // This component should only render the plot when data is ready.
    if (!plotData) {
        // Should ideally not be reached if VisualizationWorkspace logic is correct,
        // but return null as a fallback.
        return null; 
    }

    // Render the plot - remove wrapper div, let parent control size/style
    return (
        <Plot
            data={plotData}
            layout={layout}
            style={{ width: '100%', height: '100%' }}
            config={{
                responsive: true,
                displaylogo: false,
            }}
            onClick={(eventData) => {
                if (eventData.points.length > 0) {
                    const clickedPoint = eventData.points[0];
                    // Retrieve the actual ID from customdata
                    const clickedPointActualId = clickedPoint.customdata; 
                    
                    if (clickedPointActualId) {
                        // If the clicked point is already selected, deselect it. Otherwise, select it.
                        const newSelectedId = selectedPlotPointId === clickedPointActualId ? null : clickedPointActualId;
                        dispatch(setSelectedPlotPointId({ indexName, pointId: newSelectedId }));
                    }
                }
            }}
        />
    );
}

export default Plotter;

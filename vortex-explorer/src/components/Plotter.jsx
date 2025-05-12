import React, { useMemo } from 'react';
import Plot from 'react-plotly.js'; 
import { useSelector } from 'react-redux';
import { 
    selectReducedCoordinates, 
    selectVectorDataStatus, 
    selectRawVectors,
    selectSearchResults // Import search results selector
} from '../features/vectors/vectorsSlice';

// Define colors for highlighting
const DEFAULT_COLOR = 'rgba(31, 119, 180, 0.7)'; // Default Plotly blue with opacity
const HIGHLIGHT_COLOR = 'rgba(255, 127, 14, 1.0)'; // Default Plotly orange, opaque
const DEFAULT_SIZE = 6;
const HIGHLIGHT_SIZE = 9;

function Plotter({ indexName }) {
    const reducedCoords = useSelector(selectReducedCoordinates(indexName)); 
    const vectorFetchStatus = useSelector(selectVectorDataStatus(indexName));
    const rawVectorsData = useSelector(selectRawVectors(indexName)); 
    const searchResults = useSelector(selectSearchResults(indexName)); // Get search results {id: string, score: number}[]

    // Memoize plot data generation
    const plotData = useMemo(() => {
        if (!reducedCoords || reducedCoords.length === 0) {
            return null;
        }

        // Extract IDs from raw data, ensuring the order matches reducedCoords
        // This assumes reducedCoords maintains the original order of vectors passed to UMAP
        const vectorIds = rawVectorsData?.map(item => item.id) || []; 
        
        // Check if lengths match - they should if reduction was based on rawVectorsData
        if (vectorIds.length !== reducedCoords.length) {
             console.warn("Mismatch between vector IDs and reduced coordinates count.");
             // Fallback: generate generic hover text or disable it
             // Consider returning null or an empty trace if mismatch is critical
        }

        // Create a Set of highlighted IDs for quick lookup
        const highlightedIds = new Set(searchResults?.map(item => item.id) || []);

        // Generate marker properties based on search results
        const markerColors = vectorIds.map(id => highlightedIds.has(id) ? HIGHLIGHT_COLOR : DEFAULT_COLOR);
        const markerSizes = vectorIds.map(id => highlightedIds.has(id) ? HIGHLIGHT_SIZE : DEFAULT_SIZE);
        // Adjust opacity slightly for non-highlighted points if desired
        // const markerOpacities = vectorIds.map(id => highlightedIds.has(id) ? 1.0 : 0.7);

        const trace = {
            x: reducedCoords.map(p => p[0]),
            y: reducedCoords.map(p => p[1]),
            mode: 'markers',
            type: 'scattergl', 
            marker: { 
                size: markerSizes, 
                color: markerColors,
                // opacity: markerOpacities, // Apply opacity array if needed
                line: { // Add subtle border to highlighted points
                     width: markerSizes.map(s => s === HIGHLIGHT_SIZE ? 1 : 0),
                     color: 'rgba(0, 0, 0, 0.7)'
                }
            },
            // Add score to hover text if available
            text: vectorIds.map(id => {
                const result = searchResults?.find(r => r.id === id);
                return result ? `${id}<br>Score: ${result.score.toFixed(4)}` : id;
            }), 
            hoverinfo: 'text', 
            name: indexName, 
        };
        return [trace]; 
    }, [reducedCoords, rawVectorsData, indexName, searchResults]); // Depend on searchResults

    const layout = useMemo(() => ({
        // Removed title from layout, handled in VisualizationWorkspace
        hovermode: 'closest',
        xaxis: { title: 'UMAP Dimension 1', zeroline: false, showgrid: false }, // Hide grid lines
        yaxis: { title: 'UMAP Dimension 2', zeroline: false, showgrid: false }, // Hide grid lines
        margin: { l: 40, r: 20, b: 40, t: 20, pad: 4 }, // Tighter margins
        showlegend: false, // Hide legend for single trace
        // Ensure plot background is transparent to inherit Paper background
        paper_bgcolor: 'rgba(0,0,0,0)', 
        plot_bgcolor: 'rgba(0,0,0,0)', 
    }), []); // No dependency needed if title is removed

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
            // Let parent container (Paper in VizWorkspace) control size
                style={{ width: '100%', height: '100%' }}
                config={{ 
                    responsive: true, // Make plot responsive to container size changes
                    displaylogo: false, // Hide Plotly logo
                    // modeBarButtonsToRemove: ['select2d', 'lasso2d'] // Example: remove some buttons
                }}
            />
    ); // Removed extra closing div
}

export default Plotter;

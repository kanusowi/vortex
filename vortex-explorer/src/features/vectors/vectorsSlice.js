import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { fetchVectorsSample as fetchVectorsApi } from '../../services/vortexApi';
import { selectIndex } from '../indices/indicesSlice'; // Import action from indices slice

// Async thunk for fetching vector data for a given index
export const fetchVectors = createAsyncThunk(
  'vectors/fetchVectors',
  async ({ indexName, limit }, { rejectWithValue }) => {
    if (!indexName) {
      return rejectWithValue('No index selected');
    }
    try {
      // Using the placeholder API function - assumes it returns [{ id: string, vector: number[] }]
      const vectorsData = await fetchVectorsApi(indexName, limit); 
      return { indexName, data: vectorsData }; // Return data along with the index name it belongs to
    } catch (error) {
      return rejectWithValue(error.message || `Failed to fetch vectors for ${indexName}`);
    }
  }
);

const initialState = {
  // Data is stored per index
  dataByIndex: {}, 
  // Structure for each index:
  // { 
  //   rawVectors: Array<{id: string, vector: number[]}>, 
  //   status: 'idle'|'loading'|'succeeded'|'failed', // Status for fetching raw vectors
  //   error: string|null,                           // Error for fetching raw vectors
  //   reducedCoords: Array<[number, number]> | null, 
  //   umapStatus: 'idle'|'reducing'|'succeeded'|'failed', 
  //   umapError: string | null,
  //   searchResults: Array<{id: string, score: number}> | null,
  //   searchStatus: 'idle'|'searching'|'succeeded'|'failed',
  //   searchError: string | null
  // } 
};

// --- Async Thunks ---

// Search Vectors Thunk
export const searchVectors = createAsyncThunk(
  'vectors/searchVectors',
  async ({ indexName, queryVector, k }, { rejectWithValue, getState }) => {
    if (!indexName) return rejectWithValue('No index selected');
    if (!queryVector || queryVector.length === 0) return rejectWithValue('Query vector is empty');
    
    try {
      const { searchVectors: searchVectorsApi } = await import('../../services/vortexApi'); 
      const results = await searchVectorsApi(indexName, queryVector, k);
      return { indexName, results: results.results }; 
    } catch (error) {
      return rejectWithValue(error.message || `Search failed for ${indexName}`);
    }
  }
);

// Add Vector Thunk
export const addVector = createAsyncThunk(
  'vectors/addVector',
  async ({ indexName, vectorId, vectorData }, { dispatch, rejectWithValue }) => {
    if (!indexName) return rejectWithValue('No index selected for adding vector');
    try {
      const { addVector: addVectorApi } = await import('../../services/vortexApi');
      const response = await addVectorApi(indexName, vectorId, vectorData);
      // After adding, we might want to re-fetch vectors or stats for the current index
      // For now, just return success, let component decide on refresh
      // dispatch(fetchVectors({ indexName, limit: VECTOR_FETCH_LIMIT })); // Example re-fetch
      // dispatch(fetchIndexStats(indexName)); // Example re-fetch stats from indicesSlice
      return { indexName, vectorId, response }; // Return response for potential use
    } catch (error) {
      return rejectWithValue(error.message || `Failed to add vector ${vectorId} to ${indexName}`);
    }
  }
);


// --- Slice Definition ---

const initialStateWithAddVector = { // Renaming to avoid conflict if this block is re-run
  // Data is stored per index
  dataByIndex: {}, 
  // Structure for each index:
  // { 
  //   ... (existing fields for rawVectors, umap, search)
  //   addVectorStatus: 'idle'|'adding'|'succeeded'|'failed',
  //   addVectorError: string | null
  // } 
  // Global status for add vector if not per-index (simpler for now)
  globalAddVectorStatus: 'idle',
  globalAddVectorError: null,
  // Global status for synthetic data generation
  syntheticDataStatus: 'idle', // 'idle' | 'generating' | 'succeeded' | 'failed'
  syntheticDataError: null,
};


const vectorsSlice = createSlice({
  name: 'vectors',
  // Use the new initial state if this is the first run of this block
  initialState: initialState.dataByIndex ? initialState : initialStateWithAddVector, 
  reducers: {
    resetAddVectorStatus: (state) => {
        state.globalAddVectorStatus = 'idle';
        state.globalAddVectorError = null;
    },
    resetSyntheticDataStatus: (state) => { // Action to reset synthetic data status
        state.syntheticDataStatus = 'idle';
        state.syntheticDataError = null;
    },
    // Action to store the result of dimensionality reduction
    setReducedCoordinates: (state, action) => {
      const { indexName, coordinates } = action.payload; // Expects { indexName: string, coordinates: Array<[number, number]> }
      if (state.dataByIndex[indexName]) {
        state.dataByIndex[indexName].reducedCoords = coordinates;
        // Also update UMAP status on success
        state.dataByIndex[indexName].umapStatus = 'succeeded'; 
        state.dataByIndex[indexName].umapError = null;
      } else {
         console.warn(`Cannot set reduced coordinates for non-fetched index: ${indexName}`);
      }
    },
     // Action to explicitly set the UMAP status (e.g., 'reducing', 'failed')
    setUmapStatus: (state, action) => {
        const { indexName, status, error = null } = action.payload; // Expects { indexName: string, status: string, error?: string }
        if (state.dataByIndex[indexName]) {
            state.dataByIndex[indexName].umapStatus = status;
            state.dataByIndex[indexName].umapError = error;
            // Clear reducedCoords if starting reduction or if it failed
            if (status === 'reducing' || status === 'failed') {
                 state.dataByIndex[indexName].reducedCoords = null;
            }
        } else {
            console.warn(`Cannot set UMAP status for non-fetched index: ${indexName}`);
        }
    },
     // Action to clear search results for a specific index
    clearSearchResults: (state, action) => {
        const indexName = action.payload; // Expects index name string
        if (state.dataByIndex[indexName]) {
            state.dataByIndex[indexName].searchResults = null;
            state.dataByIndex[indexName].searchStatus = 'idle';
            state.dataByIndex[indexName].searchError = null;
        }
    },
    // Action to clear vector data for a specific index (e.g., on re-fetch or index deselection)
    clearVectorData: (state, action) => {
        const indexName = action.payload; // Expects index name string
        if (state.dataByIndex[indexName]) {
            delete state.dataByIndex[indexName]; // Deletes all data including vectors, umap, search
        }
    }
  },
  extraReducers: (builder) => {
    builder
      // --- Handle Fetching Vectors ---
      .addCase(fetchVectors.pending, (state, action) => {
        const { indexName } = action.meta.arg;
        state.dataByIndex[indexName] = { 
            ...(state.dataByIndex[indexName] || {}), // Preserve existing data like reducedCoords if needed, or reset
            rawVectors: [], 
            reducedCoords: null, 
            status: 'loading', 
            error: null,
            umapStatus: 'idle', 
            umapError: null,
            searchResults: null, // Also clear search results on new fetch
            searchStatus: 'idle',
            searchError: null
        };
      })
      .addCase(fetchVectors.fulfilled, (state, action) => {
        const { indexName, data } = action.payload;
        state.dataByIndex[indexName] = { 
            ...(state.dataByIndex[indexName] || {}),
            rawVectors: data, 
            status: 'succeeded', 
            error: null,
            // Keep umapStatus as 'idle' - reduction hasn't started yet
        };
        // Dimensionality reduction should be triggered by the component after this success
      })
      .addCase(fetchVectors.rejected, (state, action) => {
        const { indexName } = action.meta.arg;
        state.dataByIndex[indexName] = { 
            ...(state.dataByIndex[indexName] || {}),
            rawVectors: [], 
            status: 'failed', 
            error: action.payload,
            umapStatus: 'idle', 
            umapError: null,
            // Keep previous search results on fetch failure? Or clear? Let's clear.
            searchResults: null, 
            searchStatus: 'idle',
            searchError: null
        };
      })
      // --- Handle Searching Vectors ---
       .addCase(searchVectors.pending, (state, action) => {
        const { indexName } = action.meta.arg;
        if (state.dataByIndex[indexName]) {
            state.dataByIndex[indexName].searchStatus = 'searching';
            state.dataByIndex[indexName].searchError = null;
            state.dataByIndex[indexName].searchResults = null; // Clear previous results
        }
      })
      .addCase(searchVectors.fulfilled, (state, action) => {
        const { indexName, results } = action.payload;
         if (state.dataByIndex[indexName]) {
            state.dataByIndex[indexName].searchStatus = 'succeeded';
            state.dataByIndex[indexName].searchResults = results;
        }
      })
      .addCase(searchVectors.rejected, (state, action) => {
         const { indexName } = action.meta.arg;
         if (state.dataByIndex[indexName]) {
            state.dataByIndex[indexName].searchStatus = 'failed';
            state.dataByIndex[indexName].searchError = action.payload;
        }
      })
      // --- Handle Add Vector ---
      .addCase(addVector.pending, (state) => {
        state.globalAddVectorStatus = 'adding';
        state.globalAddVectorError = null;
      })
      .addCase(addVector.fulfilled, (state, action) => {
        state.globalAddVectorStatus = 'succeeded';
        // Optionally, update local data if needed, e.g., increment vector count if stats are here
        // For now, success is enough, rely on re-fetch if data needs to be current
        const { indexName, vectorId } = action.payload;
        // If adding to the currently displayed index, could mark its data as stale
        // or trigger a re-fetch of vectors/stats.
      })
      .addCase(addVector.rejected, (state, action) => {
        state.globalAddVectorStatus = 'failed';
        state.globalAddVectorError = action.payload;
      })
      // --- Placeholder for Synthetic Data Thunk (if we make one, for now status set by component) ---
      // Example if we had a thunk:
      // .addCase(generateSyntheticData.pending, (state) => {
      //   state.syntheticDataStatus = 'generating';
      //   state.syntheticDataError = null;
      // })
      // .addCase(generateSyntheticData.fulfilled, (state) => {
      //   state.syntheticDataStatus = 'succeeded';
      // })
      // .addCase(generateSyntheticData.rejected, (state, action) => {
      //   state.syntheticDataStatus = 'failed';
      //   state.syntheticDataError = action.payload;
      // })
      // --- Handle Index Selection Change ---
      .addCase(selectIndex, (state, action) => {
          if (action.payload === null) {
              // Option 1: Clear all vector data
              // state.dataByIndex = {}; 
              // Option 2: Keep data cached, do nothing here. Let components decide whether to clear/refetch.
              // For now, let's do nothing, data remains cached.
          }
          // If selection changes to a *different* index, we don't automatically clear
          // the old data here, but components might trigger fetches for the new one.
      });
  },
});

// Export actions
export const { 
    setReducedCoordinates, 
    setUmapStatus, 
    clearSearchResults, 
    clearVectorData,
    resetAddVectorStatus,
    resetSyntheticDataStatus // Export new action
} = vectorsSlice.actions;

// Export selectors
export const selectRawVectors = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.rawVectors || [];
export const selectReducedCoordinates = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.reducedCoords || null;
export const selectVectorDataStatus = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.status || 'idle';
export const selectVectorDataError = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.error || null;
// UMAP Selectors
export const selectUmapStatus = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.umapStatus || 'idle';
export const selectUmapError = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.umapError || null;
// Search Selectors
export const selectSearchResults = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.searchResults || null;
export const selectSearchStatus = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.searchStatus || 'idle';
export const selectSearchError = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.searchError || null;
// Add Vector Selectors
export const selectGlobalAddVectorStatus = (state) => state.vectors.globalAddVectorStatus;
export const selectGlobalAddVectorError = (state) => state.vectors.globalAddVectorError;
// Synthetic Data Selectors
export const selectSyntheticDataStatus = (state) => state.vectors.syntheticDataStatus;
export const selectSyntheticDataError = (state) => state.vectors.syntheticDataError;


// Selector to get all data for an index (useful for components)
export const selectDataForIndex = (indexName) => (state) => state.vectors.dataByIndex[indexName];


// Export reducer
export default vectorsSlice.reducer;

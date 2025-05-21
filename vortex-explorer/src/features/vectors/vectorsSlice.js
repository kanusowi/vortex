import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { 
    fetchVectorsSample as fetchVectorsApi,
    batchAddVectors as batchAddVectorsApi // Added for batch operations
} from '../../services/vortexApi';
import { selectIndex, fetchIndexStats } from '../indices/indicesSlice'; // Added fetchIndexStats

// Async thunk for fetching vector data for a given index
export const fetchVectors = createAsyncThunk(
  'vectors/fetchVectors',
  async ({ indexName, limit }, { rejectWithValue }) => {
    if (!indexName) {
      return rejectWithValue('No index selected');
    }
    try {
      const vectorsData = await fetchVectorsApi(indexName, limit); 
      return { indexName, data: vectorsData };
    } catch (error) {
      return rejectWithValue(error.message || `Failed to fetch vectors for ${indexName}`);
    }
  }
);

const initialState = {
  dataByIndex: {}, 
  // Structure for each index:
  // { 
  //   rawVectors: Array<{id: string, vector: number[], metadata?: object}>, 
  //   status: 'idle'|'loading'|'succeeded'|'failed',
  //   error: string|null,
  //   reducedCoords: Array<[number, number]> | null, 
  //   umapStatus: 'idle'|'reducing'|'succeeded'|'failed', 
  //   umapError: string | null,
  //   searchResults: Array<{id: string, score: number, metadata?: object}> | null,
  //   searchStatus: 'idle'|'searching'|'succeeded'|'failed',
  //   searchError: string | null,
  //   selectedPlotPointId: string | null,
  //   colorByMetadataField: string | null
  // } 
};

// --- Async Thunks ---

// Search Vectors Thunk
export const searchVectors = createAsyncThunk(
  'vectors/searchVectors',
  async ({ indexName, queryVector, k, filter }, { rejectWithValue }) => {
    if (!indexName) return rejectWithValue('No index selected');
    if (!queryVector || queryVector.length === 0) return rejectWithValue('Query vector is empty');
    
    try {
      // Using dynamic import as in the original file
      const { searchVectors: searchVectorsApiService } = await import('../../services/vortexApi'); 
      const results = await searchVectorsApiService(indexName, queryVector, k, filter);
      return { indexName, results: results.results }; 
    } catch (error) {
      return rejectWithValue(error.message || `Search failed for ${indexName}`);
    }
  }
);

// Add Vector Thunk
export const addVector = createAsyncThunk(
  'vectors/addVector',
  async ({ indexName, vectorId, vectorData, metadata }, { dispatch, rejectWithValue }) => {
    if (!indexName) return rejectWithValue('No index selected for adding vector');
    try {
      // Using dynamic import as in the original file
      const { addVector: addVectorApiService } = await import('../../services/vortexApi');
      const response = await addVectorApiService(indexName, vectorId, vectorData, metadata);
      // dispatch(fetchIndexStats(indexName)); // Consider re-fetching stats
      return { indexName, vectorId, response };
    } catch (error) {
      return rejectWithValue(error.message || `Failed to add vector ${vectorId} to ${indexName}`);
    }
  }
);

// Batch Add Vectors Thunk
export const batchAddVectors = createAsyncThunk(
  'vectors/batchAddVectors',
  async ({ indexName, vectors }, { dispatch, rejectWithValue }) => {
    if (!indexName) return rejectWithValue('No index selected for batch add');
    if (!vectors || vectors.length === 0) return rejectWithValue('No vectors provided for batch add');
    try {
      const response = await batchAddVectorsApi(indexName, vectors); // Uses direct import
      dispatch(fetchIndexStats(indexName)); // Re-fetch stats after batch operation
      return { indexName, response };
    } catch (error) {
      return rejectWithValue(error.message || `Failed to batch add vectors to ${indexName}`);
    }
  }
);


// --- Slice Definition ---

const initialStateWithAddVector = { 
  dataByIndex: {}, 
  globalAddVectorStatus: 'idle',
  globalAddVectorError: null,
  syntheticDataStatus: 'idle', 
  syntheticDataError: null,
  globalBatchAddStatus: 'idle', // Added for batch add
  globalBatchAddError: null,   // Added for batch add
};


const vectorsSlice = createSlice({
  name: 'vectors',
  initialState: initialState.dataByIndex ? initialState : initialStateWithAddVector, 
  reducers: {
    resetAddVectorStatus: (state) => {
        state.globalAddVectorStatus = 'idle';
        state.globalAddVectorError = null;
    },
    resetSyntheticDataStatus: (state) => {
        state.syntheticDataStatus = 'idle';
        state.syntheticDataError = null;
    },
    resetBatchAddStatus: (state) => { // Added for batch add
        state.globalBatchAddStatus = 'idle';
        state.globalBatchAddError = null;
    },
    setReducedCoordinates: (state, action) => {
      const { indexName, coordinates } = action.payload;
      if (state.dataByIndex[indexName]) {
        state.dataByIndex[indexName].reducedCoords = coordinates;
        state.dataByIndex[indexName].umapStatus = 'succeeded'; 
        state.dataByIndex[indexName].umapError = null;
      } else {
         console.warn(`Cannot set reduced coordinates for non-fetched index: ${indexName}`);
      }
    },
    setUmapStatus: (state, action) => {
        const { indexName, status, error = null } = action.payload;
        if (state.dataByIndex[indexName]) {
            state.dataByIndex[indexName].umapStatus = status;
            state.dataByIndex[indexName].umapError = error;
            if (status === 'reducing' || status === 'failed') {
                 state.dataByIndex[indexName].reducedCoords = null;
            }
        } else {
            console.warn(`Cannot set UMAP status for non-fetched index: ${indexName}`);
        }
    },
    clearSearchResults: (state, action) => {
        const indexName = action.payload;
        if (state.dataByIndex[indexName]) {
            state.dataByIndex[indexName].searchResults = null;
            state.dataByIndex[indexName].searchStatus = 'idle';
            state.dataByIndex[indexName].searchError = null;
        }
    },
    clearVectorData: (state, action) => {
        const indexName = action.payload;
        if (state.dataByIndex[indexName]) {
            delete state.dataByIndex[indexName];
        }
    },
    setSelectedPlotPointId: (state, action) => {
      const { indexName, pointId } = action.payload;
      if (state.dataByIndex[indexName]) {
        state.dataByIndex[indexName].selectedPlotPointId = pointId;
      } else {
        console.warn(`Cannot set selected plot point ID for non-existent index data: ${indexName}`);
      }
    },
    setColorByMetadataField: (state, action) => {
      const { indexName, fieldName } = action.payload;
      if (state.dataByIndex[indexName]) {
        state.dataByIndex[indexName].colorByMetadataField = fieldName;
      } else {
        console.warn(`Cannot set color-by-metadata-field for non-existent index data: ${indexName}`);
      }
    }
  },
  extraReducers: (builder) => {
    builder
      // --- Handle Fetching Vectors ---
      .addCase(fetchVectors.pending, (state, action) => {
        const { indexName } = action.meta.arg;
        state.dataByIndex[indexName] = { 
            ...(state.dataByIndex[indexName] || {}),
            rawVectors: [], 
            reducedCoords: null, 
            status: 'loading', 
            error: null,
            umapStatus: 'idle', 
            umapError: null,
            searchResults: null,
            searchStatus: 'idle',
            searchError: null,
            selectedPlotPointId: null, // Initialize here
            colorByMetadataField: null // Initialize here
        };
      })
      .addCase(fetchVectors.fulfilled, (state, action) => {
        const { indexName, data } = action.payload;
        state.dataByIndex[indexName] = { 
            ...(state.dataByIndex[indexName] || {}),
            rawVectors: data, 
            status: 'succeeded', 
            error: null,
        };
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
            state.dataByIndex[indexName].searchResults = null;
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
      })
      .addCase(addVector.rejected, (state, action) => {
        state.globalAddVectorStatus = 'failed';
        state.globalAddVectorError = action.payload;
      })
      // --- Handle Batch Add Vectors ---
      .addCase(batchAddVectors.pending, (state) => {
        state.globalBatchAddStatus = 'adding';
        state.globalBatchAddError = null;
      })
      .addCase(batchAddVectors.fulfilled, (state, action) => {
        state.globalBatchAddStatus = 'succeeded';
        // action.payload.response contains { success_count, failure_count, message }
      })
      .addCase(batchAddVectors.rejected, (state, action) => {
        state.globalBatchAddStatus = 'failed';
        state.globalBatchAddError = action.payload;
      })
      // --- Handle Index Selection Change ---
      .addCase(selectIndex, (state, action) => {
          // Current logic is to keep data cached, which is fine.
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
    resetSyntheticDataStatus,
    resetBatchAddStatus, // Added for batch add
    setSelectedPlotPointId,
    setColorByMetadataField
} = vectorsSlice.actions;

// Export selectors
export const selectRawVectors = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.rawVectors || [];
export const selectReducedCoordinates = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.reducedCoords || null;
export const selectVectorDataStatus = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.status || 'idle';
export const selectVectorDataError = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.error || null;
export const selectUmapStatus = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.umapStatus || 'idle';
export const selectUmapError = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.umapError || null;
export const selectSearchResults = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.searchResults || null;
export const selectSearchStatus = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.searchStatus || 'idle';
export const selectSearchError = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.searchError || null;
export const selectGlobalAddVectorStatus = (state) => state.vectors.globalAddVectorStatus;
export const selectGlobalAddVectorError = (state) => state.vectors.globalAddVectorError;
export const selectSyntheticDataStatus = (state) => state.vectors.syntheticDataStatus;
export const selectSyntheticDataError = (state) => state.vectors.syntheticDataError;
// Batch Add Selectors
export const selectGlobalBatchAddStatus = (state) => state.vectors.globalBatchAddStatus;
export const selectGlobalBatchAddError = (state) => state.vectors.globalBatchAddError;
export const selectSelectedPlotPointId = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.selectedPlotPointId || null;
export const selectColorByMetadataField = (indexName) => (state) => state.vectors.dataByIndex[indexName]?.colorByMetadataField || null;


// Selector to get all data for an index (useful for components)
export const selectDataForIndex = (indexName) => (state) => state.vectors.dataByIndex[indexName];


// Export reducer
export default vectorsSlice.reducer;

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { 
    fetchIndices as fetchIndicesApi, 
    fetchIndexStats as fetchIndexStatsApi,
    createIndex as createIndexApi // Import the create index API function
} from '../../services/vortexApi';

// --- Async Thunks ---

// Fetch Indices (existing)
export const fetchIndices = createAsyncThunk(
  'indices/fetchIndices',
  async (_, { rejectWithValue }) => {
    try {
      const indicesList = await fetchIndicesApi();
      // Assuming the API returns an array of strings (index names)
      return indicesList; 
    } catch (error) {
      return rejectWithValue(error.message || 'Failed to fetch indices');
    }
  }
);

// Fetch Index Stats (existing)
export const fetchIndexStats = createAsyncThunk(
  'indices/fetchIndexStats',
  async (indexName, { rejectWithValue }) => {
    if (!indexName) {
      // Don't fetch if no index is selected
      return null; 
    }
    try {
      const stats = await fetchIndexStatsApi(indexName);
      return stats; // Assuming API returns the stats object
    } catch (error) {
      return rejectWithValue(error.message || `Failed to fetch stats for ${indexName}`);
    }
  }
);


// Create Index Thunk
export const createIndex = createAsyncThunk(
  'indices/createIndex',
  async ({ indexName, dimensions, metric, config }, { dispatch, rejectWithValue }) => {
    try {
      const response = await createIndexApi(indexName, dimensions, metric, config);
      // After successful creation, re-fetch the list of indices to include the new one
      dispatch(fetchIndices()); 
      return response; // Contains success message
    } catch (error) {
      return rejectWithValue(error.message || 'Failed to create index');
    }
  }
);


// --- Slice Definition ---

const initialState = {
  list: [], 
  selected: null, 
  status: 'idle', 
  error: null, 
  stats: null, 
  statsStatus: 'idle', 
  statsError: null, 
  // State for create index operation
  createStatus: 'idle', // 'idle' | 'creating' | 'succeeded' | 'failed'
  createError: null,    // Error message if creation fails
};

const indicesSlice = createSlice({
  name: 'indices',
  initialState,
  reducers: {
    resetCreateStatus: (state) => { // Action to reset create status (e.g., after modal closes)
        state.createStatus = 'idle';
        state.createError = null;
    },
    // Action to set the currently selected index
    selectIndex: (state, action) => {
      // action.payload should be the name (string) of the index to select, or null
      const newSelection = action.payload;
      if (state.list.includes(newSelection) || newSelection === null) {
        state.selected = newSelection;
        // Reset stats when selection changes
        state.stats = null;
        state.statsStatus = 'idle';
        state.statsError = null;
      } else {
        console.warn(`Attempted to select non-existent index: ${newSelection}`);
      }
    },
    // Optional: Action to clear the selection
    clearSelection: (state) => {
        state.selected = null;
        // Also clear stats on manual clear
        state.stats = null;
        state.statsStatus = 'idle';
        state.statsError = null;
        state.selected = null;
    }
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchIndices.pending, (state) => {
        state.status = 'loading';
        state.error = null; // Clear previous errors
      })
      .addCase(fetchIndices.fulfilled, (state, action) => {
        state.status = 'succeeded';
        state.list = action.payload; // Replace the list with the fetched one
        // Optional: Reset selection if the previously selected index no longer exists
        if (state.selected && !action.payload.includes(state.selected)) {
            state.selected = null; 
        }
        // Optional: Auto-select the first index if none is selected and list is not empty
        // if (state.selected === null && state.list.length > 0) {
        //     state.selected = state.list[0];
        // }
      })
      .addCase(fetchIndices.rejected, (state, action) => {
        state.status = 'failed';
        state.error = action.payload; // Error message from rejectWithValue
        state.list = []; // Clear list on failure
        state.selected = null; // Clear selection on failure
        // Also clear stats if list fetch fails
        state.stats = null;
        state.statsStatus = 'idle';
        state.statsError = null;
      })
      // Handle stats fetching lifecycle
      .addCase(fetchIndexStats.pending, (state) => {
        state.statsStatus = 'loading';
        state.statsError = null;
      })
      .addCase(fetchIndexStats.fulfilled, (state, action) => {
        state.statsStatus = 'succeeded';
        state.stats = action.payload; // Store the fetched stats object
      })
      .addCase(fetchIndexStats.rejected, (state, action) => {
        state.statsStatus = 'failed';
        state.statsError = action.payload; 
        state.stats = null; 
      })
      // Handle create index lifecycle
      .addCase(createIndex.pending, (state) => {
        state.createStatus = 'creating';
        state.createError = null;
      })
      .addCase(createIndex.fulfilled, (state, action) => {
        state.createStatus = 'succeeded';
        // action.payload contains the success message from the server
        // The index list will be updated by the fetchIndices dispatch in the thunk
      })
      .addCase(createIndex.rejected, (state, action) => {
        state.createStatus = 'failed';
        state.createError = action.payload;
      });
  },
});

// Export actions
export const { selectIndex, clearSelection, resetCreateStatus } = indicesSlice.actions;

// Export selectors
export const selectAllIndices = (state) => state.indices.list;
export const selectSelectedIndices = (state) => state.indices.selected;
export const selectIndicesStatus = (state) => state.indices.status;
export const selectIndicesError = (state) => state.indices.error;
// Stats selectors
export const selectIndexStats = (state) => state.indices.stats;
export const selectIndexStatsStatus = (state) => state.indices.statsStatus;
export const selectIndexStatsError = (state) => state.indices.statsError;
// Create Index selectors
export const selectCreateIndexStatus = (state) => state.indices.createStatus;
export const selectCreateIndexError = (state) => state.indices.createError;


// Export reducer
export default indicesSlice.reducer;

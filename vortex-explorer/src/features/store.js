import { configureStore } from '@reduxjs/toolkit';
// Import slices here when they are created
import indicesReducer from './indices/indicesSlice'; 
import vectorsReducer from './vectors/vectorsSlice'; // Uncommented
// import searchReducer from './search/searchSlice';

export const store = configureStore({
  reducer: {
    // Add reducers here
    indices: indicesReducer, 
    vectors: vectorsReducer, // Added
    // search: searchReducer,
  },
  // Middleware can be added here if needed (e.g., for RTK Query)
});

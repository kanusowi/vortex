import React from 'react';
import { useSelector } from 'react-redux'; // Import useSelector
import { Box, AppBar, Toolbar, Typography, Drawer, CssBaseline, Divider, Button } from '@mui/material'; // Added Button
import IndexSelector from './components/IndexSelector';
import VisualizationWorkspace from './components/VisualizationWorkspace';
import SearchControl from './components/SearchControl';
import CreateIndexModal from './components/CreateIndexModal';
import AddVectorModal from './components/AddVectorModal';
import { Toaster } from 'react-hot-toast'; 
import { selectAllIndices } from './features/indices/indicesSlice'; // Import selector

const drawerWidth = 280; // Define drawer width

function App() {
  const indicesList = useSelector(selectAllIndices);
  const noIndicesExist = !indicesList || indicesList.length === 0;

  return (
    <Box sx={{ display: 'flex' }}>
      <CssBaseline /> 
      <Toaster position="bottom-right" reverseOrder={false} /> {/* Add Toaster */}
      
      {/* Header */}
      <AppBar 
        position="fixed" 
        sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }} // Ensure AppBar is above Drawer
      >
        <Toolbar>
          <Typography variant="h6" noWrap component="div">
            Vortex Explorer
          </Typography>
        </Toolbar>
      </AppBar>
      
      {/* Sidebar (Drawer) */}
      <Drawer
        variant="permanent" // Keep drawer always visible
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          [`& .MuiDrawer-paper`]: { width: drawerWidth, boxSizing: 'border-box' },
        }}
      >
        <Toolbar /> {/* Spacer to push content below AppBar */}
        {/* Ensure this Box can scroll its content if it overflows the Drawer's height */}
        <Box sx={{ overflow: 'auto', p: 2, height: '100%' }}> 
          {/* Sidebar Content */}
          <Typography variant="subtitle1" gutterBottom sx={{ fontWeight: 'medium', mb: 2 }}>
            Controls
          </Typography>
          <IndexSelector />
          
          {/* Moved CreateIndexModal higher for visibility, especially if no indices exist */}
          <Box sx={{ mt: 2, mb: 2 }}> {/* Add some margin for the button */}
            <CreateIndexModal />
          </Box>
          
          <Divider sx={{ my: 3 }} /> {/* Divider */}

          <SearchControl /> 

          {/* <Divider sx={{ my: 3 }} /> */}
          {/* <CreateIndexModal />  Original position commented out */}

          <Divider sx={{ my: 3 }} />

          {/* Vector Operations Section */}
          <Box sx={{ mt: 1 }}> {/* Add some top margin for the section */}
            <Typography variant="subtitle1" gutterBottom sx={{ fontWeight: 'medium', mb: 2 }}>
              Vector Operations
            </Typography>
            
            {/* Add/Update Single Vector - Button triggers modal */}
            <Box sx={{ mb: 2.5 }}> 
              <AddVectorModal /> 
            </Box>

            {/* SyntheticDataGenerator component removed from here */}
            {/* The "Generate Random Vector" button is now inside AddVectorModal */}
          </Box>
          
          {/* Add other controls here later */}
        </Box>
      </Drawer>
      
      {/* Main Content Area */}
      <Box 
        component="main" 
        sx={{ 
          flexGrow: 1, 
          p: 3, // Standard MUI padding
          bgcolor: 'background.default', // Use theme background color
          mt: '64px' // Offset content below AppBar (adjust if AppBar height changes)
        }}
      >
        {/* <Toolbar />  -- Alternative spacer if not using mt */}
        {noIndicesExist ? (
          <Box sx={{ textAlign: 'center', mt: 10 }}>
            <Typography variant="h5" gutterBottom>Welcome to Vortex Explorer!</Typography>
            <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
              No indices found. Please create an index to get started.
            </Typography>
            {/* Render CreateIndexModal's button functionality here directly for prominence */}
            {/* This requires CreateIndexModal to be refactored to allow its button to be triggered from outside,
                OR we duplicate the button logic here. For now, we rely on the sidebar button being visible.
                A simpler approach for now is to ensure the sidebar button is very visible.
            */}
             <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
              You can create an index using the "Create New Index" button in the sidebar.
            </Typography>
          </Box>
        ) : (
          <VisualizationWorkspace />
        )}
      </Box>
      
      {/* Footer can be added here if needed, potentially using another AppBar at the bottom */}
      {/* Example: 
      <AppBar position="fixed" color="primary" sx={{ top: 'auto', bottom: 0 }}>
        <Toolbar variant="dense">
           <Typography variant="caption" color="inherit" component="div">
             Status Bar
           </Typography>
        </Toolbar>
      </AppBar> 
      */}
    </Box>
  );
}

export default App;

import React from 'react';
import { Box, AppBar, Toolbar, Typography, Drawer, CssBaseline, Divider } from '@mui/material';
import IndexSelector from './components/IndexSelector';
import VisualizationWorkspace from './components/VisualizationWorkspace';
import SearchControl from './components/SearchControl';
import CreateIndexModal from './components/CreateIndexModal';
import AddVectorModal from './components/AddVectorModal';
// import SyntheticDataGenerator from './components/SyntheticDataGenerator'; // No longer needed here
import { Toaster } from 'react-hot-toast'; 
// MUI Icons can be added later if needed
// import InboxIcon from '@mui/icons-material/MoveToInbox'; 
// import MailIcon from '@mui/icons-material/Mail';

const drawerWidth = 280; // Define drawer width

function App() {
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
          
          <Divider sx={{ my: 3 }} /> {/* Divider */}

          <SearchControl /> 

          <Divider sx={{ my: 3 }} />

          <CreateIndexModal /> 

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
        <VisualizationWorkspace />
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

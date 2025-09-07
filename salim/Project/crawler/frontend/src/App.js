import React, { useState, useEffect } from 'react';
import {
    ThemeProvider,
    createTheme,
    CssBaseline,
    AppBar,
    Toolbar,
    Typography,
    Container,
    Card,
    CardContent,
    List,
    ListItem,
    ListItemText,
    ListItemIcon,
    Chip,
    Box,
    IconButton,
    Alert
} from '@mui/material';
import { styled } from '@mui/material/styles';
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile';
import RefreshIcon from '@mui/icons-material/Refresh';

const theme = createTheme({
    palette: {
        mode: 'light',
        primary: {
            main: '#1976d2',
        },
    },
});

const StyledCard = styled(Card)(({ theme }) => ({
    margin: theme.spacing(2, 0),
    borderRadius: theme.spacing(2),
}));

const StyledListItem = styled(ListItem)(({ theme }) => ({
    borderRadius: theme.spacing(1),
    marginBottom: theme.spacing(1),
    '&:hover': {
        backgroundColor: theme.palette.action.hover,
    },
}));

const HeaderBox = styled(Box)(({ theme }) => ({
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: theme.spacing(2),
}));

function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDate(dateString) {
    return new Date(dateString).toLocaleString();
}

function App() {
    const [files, setFiles] = useState([]);
    const [bucket, setBucket] = useState('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');

    const fetchFiles = async () => {
        setLoading(true);
        setError('');
        try {
            const response = await fetch('http://localhost:8080/files');
            const data = await response.json();

            if (response.ok) {
                setFiles(data.files || []);
                setBucket(data.bucket || '');
            } else {
                setError(data.error || 'Failed to fetch files');
            }
        } catch (err) {
            setError('Failed to connect to server');
        }
        setLoading(false);
    };

    useEffect(() => {
        fetchFiles();
        const interval = setInterval(fetchFiles, 10000);
        return () => clearInterval(interval);
    }, []);

    return (
        <ThemeProvider theme={theme}>
            <CssBaseline />
            <AppBar position="static">
                <Toolbar>
                    <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
                        S3 File Viewer
                    </Typography>
                    <Chip
                        label={bucket || 'No bucket'}
                        color="secondary"
                        variant="outlined"
                        sx={{ color: 'white', borderColor: 'white' }}
                    />
                </Toolbar>
            </AppBar>

            <Container maxWidth="md" sx={{ mt: 4 }}>
                <HeaderBox>
                    <Typography variant="h4" component="h1">
                        Files ({files.length})
                    </Typography>
                    <IconButton
                        onClick={fetchFiles}
                        disabled={loading}
                        color="primary"
                    >
                        <RefreshIcon />
                    </IconButton>
                </HeaderBox>

                {error && (
                    <Alert severity="error" sx={{ mb: 2 }}>
                        {error}
                    </Alert>
                )}

                <StyledCard>
                    <CardContent>
                        {files.length === 0 ? (
                            <Typography variant="body1" color="text.secondary" textAlign="center">
                                {loading ? 'Loading files...' : 'No files found in bucket'}
                            </Typography>
                        ) : (
                            <List>
                                {files.map((file, index) => (
                                    <StyledListItem key={index}>
                                        <ListItemIcon>
                                            <InsertDriveFileIcon color="primary" />
                                        </ListItemIcon>
                                        <ListItemText
                                            primary={file.key}
                                            secondary={
                                                <Box sx={{ display: 'flex', gap: 2, mt: 1 }}>
                                                    <Chip
                                                        label={formatFileSize(file.size)}
                                                        size="small"
                                                        variant="outlined"
                                                    />
                                                    <Chip
                                                        label={formatDate(file.lastModified)}
                                                        size="small"
                                                        variant="outlined"
                                                    />
                                                </Box>
                                            }
                                        />
                                    </StyledListItem>
                                ))}
                            </List>
                        )}
                    </CardContent>
                </StyledCard>
            </Container>
        </ThemeProvider>
    );
}

export default App;

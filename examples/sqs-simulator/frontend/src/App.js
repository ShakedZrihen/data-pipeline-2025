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
  Alert,
  Button,
  TextField,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Fab,
  Badge
} from '@mui/material';
import { styled } from '@mui/material/styles';
import MessageIcon from '@mui/icons-material/Message';
import RefreshIcon from '@mui/icons-material/Refresh';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import SendIcon from '@mui/icons-material/Send';

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

const MessageBody = styled(Typography)(({ theme }) => ({
  fontFamily: 'monospace',
  backgroundColor: theme.palette.grey[100],
  padding: theme.spacing(1),
  borderRadius: theme.spacing(1),
  marginTop: theme.spacing(1),
  wordBreak: 'break-word',
  whiteSpace: 'pre-wrap'
}));

function formatDate(dateString) {
  return new Date(dateString).toLocaleString();
}

function App() {
  const [messages, setMessages] = useState([]);
  const [queueName, setQueueName] = useState('');
  const [queueStats, setQueueStats] = useState({ visible: 0, notVisible: 0 });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [dialogOpen, setDialogOpen] = useState(false);
  const [newMessage, setNewMessage] = useState('');
  const [sending, setSending] = useState(false);

  const fetchMessages = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await fetch('http://localhost:8081/messages');
      const data = await response.json();
      
      if (response.ok) {
        setMessages(data.messages || []);
        setQueueName(data.queueName || '');
        setQueueStats({
          visible: parseInt(data.approximateNumberOfMessages || '0'),
          notVisible: parseInt(data.approximateNumberOfMessagesNotVisible || '0')
        });
      } else {
        setError(data.error || 'Failed to fetch messages');
      }
    } catch (err) {
      setError('Failed to connect to server');
    }
    setLoading(false);
  };

  const sendMessage = async () => {
    if (!newMessage.trim()) return;
    
    setSending(true);
    try {
      const response = await fetch('http://localhost:8081/send-message', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ message: newMessage })
      });
      
      if (response.ok) {
        setNewMessage('');
        setDialogOpen(false);
        fetchMessages(); // Refresh messages
      } else {
        const data = await response.json();
        setError(data.error || 'Failed to send message');
      }
    } catch (err) {
      setError('Failed to send message');
    }
    setSending(false);
  };

  const deleteMessage = async (receiptHandle) => {
    try {
      const response = await fetch('http://localhost:8081/delete-message', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ receiptHandle })
      });
      
      if (response.ok) {
        fetchMessages(); // Refresh messages
      } else {
        const data = await response.json();
        setError(data.error || 'Failed to delete message');
      }
    } catch (err) {
      setError('Failed to delete message');
    }
  };

  useEffect(() => {
    fetchMessages();
    const interval = setInterval(fetchMessages, 5000); // Poll every 5 seconds
    return () => clearInterval(interval);
  }, []);

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            SQS Message Viewer
          </Typography>
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Chip 
              label={`Visible: ${queueStats.visible}`} 
              color="secondary" 
              variant="outlined" 
              sx={{ color: 'white', borderColor: 'white' }}
            />
            <Chip 
              label={`In Flight: ${queueStats.notVisible}`} 
              color="secondary" 
              variant="outlined" 
              sx={{ color: 'white', borderColor: 'white' }}
            />
            <Chip 
              label={queueName || 'No queue'} 
              color="primary" 
              variant="filled" 
            />
          </Box>
        </Toolbar>
      </AppBar>
      
      <Container maxWidth="md" sx={{ mt: 4, pb: 10 }}>
        <HeaderBox>
          <Typography variant="h4" component="h1">
            Messages ({messages.length})
          </Typography>
          <IconButton 
            onClick={fetchMessages} 
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
            {messages.length === 0 ? (
              <Typography variant="body1" color="text.secondary" textAlign="center">
                {loading ? 'Loading messages...' : 'No messages in queue'}
              </Typography>
            ) : (
              <List>
                {messages.map((message, index) => (
                  <StyledListItem key={index}>
                    <ListItemIcon>
                      <MessageIcon color="primary" />
                    </ListItemIcon>
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                          <Typography variant="h6">
                            Message ID: {message.messageId}
                          </Typography>
                          <IconButton 
                            onClick={() => deleteMessage(message.receiptHandle)}
                            color="error"
                            size="small"
                          >
                            <DeleteIcon />
                          </IconButton>
                        </Box>
                      }
                      secondary={
                        <Box>
                          <MessageBody variant="body2">
                            {message.body}
                          </MessageBody>
                          <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                            <Chip 
                              label={`MD5: ${message.md5OfBody.substring(0, 8)}...`} 
                              size="small" 
                              variant="outlined"
                            />
                            <Chip 
                              label={`Handle: ${message.receiptHandle.substring(0, 8)}...`} 
                              size="small" 
                              variant="outlined"
                            />
                          </Box>
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

      <Fab 
        color="primary" 
        aria-label="add message"
        sx={{ position: 'fixed', bottom: 16, right: 16 }}
        onClick={() => setDialogOpen(true)}
      >
        <AddIcon />
      </Fab>

      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Send New Message</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            multiline
            rows={4}
            fullWidth
            label="Message Body"
            value={newMessage}
            onChange={(e) => setNewMessage(e.target.value)}
            variant="outlined"
            sx={{ mt: 1 }}
            placeholder="Enter your message content..."
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>
            Cancel
          </Button>
          <Button 
            onClick={sendMessage} 
            variant="contained" 
            disabled={sending || !newMessage.trim()}
            startIcon={<SendIcon />}
          >
            {sending ? 'Sending...' : 'Send Message'}
          </Button>
        </DialogActions>
      </Dialog>
    </ThemeProvider>
  );
}

export default App;
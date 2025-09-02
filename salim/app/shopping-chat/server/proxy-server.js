const express = require('express');
const cors = require('cors');
const { spawn } = require('child_process');
const path = require('path');

const app = express();
const PORT = 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Store MCP server process
let mcpProcess = null;

// Start MCP server
function startMCPServer() {
  const mcpServerPath = path.join(__dirname, '../../server.js');
  mcpProcess = spawn('node', [mcpServerPath], {
    stdio: ['pipe', 'pipe', 'inherit']
  });

  mcpProcess.on('error', (error) => {
    console.error('Failed to start MCP server:', error);
  });

  mcpProcess.on('close', (code) => {
    console.log(`MCP server exited with code ${code}`);
    mcpProcess = null;
  });

  return mcpProcess;
}

// Call MCP server tool
async function callMCPTool(toolName, args) {
  return new Promise((resolve, reject) => {
    if (!mcpProcess) {
      mcpProcess = startMCPServer();
    }

    const request = JSON.stringify({
      jsonrpc: '2.0',
      id: Date.now(),
      method: 'tools/call',
      params: {
        name: toolName,
        arguments: args
      }
    });

    let responseData = '';
    let timeoutId;

    // Set up timeout
    timeoutId = setTimeout(() => {
      reject(new Error('MCP request timeout'));
    }, 30000); // 30 second timeout

    // Handle response
    const handleData = (data) => {
      responseData += data.toString();
      try {
        const response = JSON.parse(responseData);
        if (response.id) {
          clearTimeout(timeoutId);
          mcpProcess.stdout.removeListener('data', handleData);
          
          if (response.error) {
            reject(new Error(response.error.message));
          } else {
            resolve(response.result);
          }
        }
      } catch (e) {
        // Response might be incomplete, keep listening
      }
    };

    mcpProcess.stdout.on('data', handleData);
    mcpProcess.stdin.write(request + '\n');
  });
}

// API Routes
app.post('/mcp', async (req, res) => {
  try {
    const { method, params } = req.body;
    
    if (method !== 'tools/call') {
      return res.status(400).json({ error: 'Invalid method' });
    }

    const { name, arguments: args } = params;
    const result = await callMCPTool(name, args);
    
    res.json({ result });
  } catch (error) {
    console.error('MCP call error:', error);
    res.status(500).json({ 
      error: 'Internal server error',
      message: error.message 
    });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    mcpServerRunning: mcpProcess !== null && !mcpProcess.killed 
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`Proxy server running on port ${PORT}`);
  console.log(`Starting MCP server...`);
  startMCPServer();
});

// Cleanup on exit
process.on('SIGINT', () => {
  if (mcpProcess) {
    mcpProcess.kill('SIGINT');
  }
  process.exit();
});

process.on('SIGTERM', () => {
  if (mcpProcess) {
    mcpProcess.kill('SIGTERM');
  }
  process.exit();
});
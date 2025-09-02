import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import dotenv from "dotenv";
import { randomUUID } from "crypto";

dotenv.config();

const sessions = new Map();

const MCP_SERVER_URL = process.env.MCP_SERVER_URL || "http://api:8000";

async function getMCPTools() {
  try {
    const fetch = (await import("node-fetch")).default;
    const response = await fetch(`${MCP_SERVER_URL}/api/mcp/tools`);

    if (!response.ok) {
      throw new Error(`MCP tools fetch failed: ${response.status}`);
    }

    const data = await response.json();
    console.log("🔧 Fetched MCP tools:", data.tools?.length || 0);

    // Transform MCP tools to Anthropic format
    return (
      data.tools?.map((tool) => ({
        name: tool.name,
        description: tool.description,
        input_schema: tool.inputSchema,
      })) || []
    );
  } catch (error) {
    console.error("❌ Failed to fetch MCP tools:", error);
    // Fallback to empty array if MCP server is not available
    return [];
  }
}

async function executeMCPTool(toolName, args) {
  try {
    const fetch = (await import("node-fetch")).default;
    const response = await fetch(
      `${MCP_SERVER_URL}/api/mcp/tools/${encodeURIComponent(toolName)}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ arguments: args }),
      }
    );

    if (!response.ok) {
      const errorData = await response
        .json()
        .catch(() => ({ error: "Unknown error" }));
      throw new Error(
        `MCP tool execution failed: ${errorData.error || response.statusText}`
      );
    }

    const result = await response.json();

    // Parse the result content if it's JSON
    if (result.content && result.content[0] && result.content[0].text) {
      try {
        return JSON.parse(result.content[0].text);
      } catch {
        // If not JSON, return the raw text
        return result.content[0].text;
      }
    }

    return result;
  } catch (error) {
    console.error(`❌ MCP tool execution error for ${toolName}:`, error);
    throw error;
  }
}

async function executeShoppingTool(toolName, args) {
  console.log("🚀 executeShoppingTool called with:", toolName, args);

  // First try to execute via MCP
  try {
    console.log(`🔧 Executing ${toolName} via MCP...`);
    const result = await executeMCPTool(toolName, args);
    console.log(`✅ MCP tool ${toolName} executed successfully`);
    return result;
  } catch (mcpError) {
    console.warn(
      `⚠️ MCP tool execution failed for ${toolName}, falling back to direct implementation:`,
      mcpError.message
    );
  }
}

async function trySearch(fetch, apiUrl, searchTerm) {
  try {
    const encodedTerm = encodeURIComponent(searchTerm);
    const searchUrl = `${apiUrl}/products?q=${encodedTerm}&limit=10`;

    const response = await fetch(searchUrl);
    if (!response.ok) {
      console.log(`Search failed for "${searchTerm}": ${response.status}`);
      return null;
    }

    const results = await response.json();
    return results && results.length > 0 ? results : null;
  } catch (error) {
    console.log(`Search error for "${searchTerm}":`, error.message);
    return null;
  }
}

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const app = express();
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    timestamp: new Date().toISOString(),
    connected_to: "Salim API",
    api_url: process.env.SALIM_API_URL || "http://localhost:8000",
  });
});

// Main chat endpoint
app.post("/chat", async (req, res) => {
  try {
    const { message, sessionId } = req.body;

    if (!message || message.trim() === "") {
      return res.status(400).json({ error: "Message is required" });
    }

    // Get or create session
    let currentSessionId = sessionId || randomUUID();
    let sessionData = sessions.get(currentSessionId) || { messages: [] };

    console.log(
      `💬 Processing message for session ${currentSessionId}:`,
      message
    );

    // Add user message to session
    sessionData.messages.push({ role: "user", content: message });

    // Prepare system message
    const systemMessage = {
      role: "system",
      content: `אתה עוזר קניות חכם לשוק הישראלי. אתה עובד עם מאגר נתונים של Salim שמכיל מחירים בזמן אמת מרמי לוי, יוחננוף וקרפור.

כללי תפעול:
- תמיד תענה בעברית בצורה ידידותית ומועילה
- השתמש בכלים שלך לחיפוש מוצרים והשוואת מחירים
- תן המלצות קונקרטיות איפה כדאי לקנות
- הסבר על חיסכון פוטנציאלי
- אם יש מבצעים, ציין אותם
- תמיד ציין מחירים בשקלים (₪)

הכלים שלך מחוברים למאגר Salim עם נתונים אמיתיים מ:
- רמי לוי (בדרך כלל הזול ביותר)  
- יוחננוף (רשת פרמיום)
- קרפור (רשת בינלאומית)

דוגמאות לשאלות שאתה יכול לענות עליהן:
- "איפה הכי זול לקנות חלב?"
- "כמה עולה לחם בכל החנויות?"
- "איפה כדאי לי לקנות את הסל שלי?"
- "מה המחיר של ביצים ברמי לוי?"`,
    };

    const toolExecutionLogs = [];
    let totalToolsUsed = 0;
    const startTime = Date.now();

    try {
      // Get tools from MCP server
      console.log("🔧 Fetching tools from MCP server...");
      const mcpTools = await getMCPTools();

      // Call Claude with MCP tools
      console.log("🤖 Calling Claude with MCP tools...");

      const response = await anthropic.messages.create({
        model: "claude-3-5-sonnet-20241022",
        max_tokens: 2000,
        system: systemMessage.content,
        messages: sessionData.messages,
        tools: mcpTools,
      });

      let finalResponse = "";

      // Handle tool use
      if (response.content.some((block) => block.type === "tool_use")) {
        console.log("🔧 Claude wants to use tools");

        const toolUses = response.content.filter(
          (block) => block.type === "tool_use"
        );
        toolExecutionLogs.push({
          type: "claude_decision",
          tools: toolUses.map((tu) => ({ name: tu.name, input: tu.input })),
        });

        const toolResults = [];

        for (const toolUse of toolUses) {
          totalToolsUsed++;
          console.log(`🔧 Executing tool: ${toolUse.name}`);

          toolExecutionLogs.push({
            type: "tool_execution_start",
            toolName: toolUse.name,
            input: toolUse.input,
          });

          const toolStartTime = Date.now();

          try {
            const result = await executeShoppingTool(
              toolUse.name,
              toolUse.input
            );
            const executionTime = Date.now() - toolStartTime;

            const resultText =
              typeof result === "object"
                ? JSON.stringify(result, null, 2)
                : String(result);

            toolResults.push({
              tool_use_id: toolUse.id,
              type: "tool_result",
              content: resultText,
            });

            toolExecutionLogs.push({
              type: "tool_execution_success",
              toolName: toolUse.name,
              executionTime,
              resultType: typeof result,
              resultLength: resultText.length,
              resultPreview:
                resultText.slice(0, 100) +
                (resultText.length > 100 ? "..." : ""),
            });
          } catch (error) {
            console.error(
              `❌ Tool execution error for ${toolUse.name}:`,
              error
            );

            toolResults.push({
              tool_use_id: toolUse.id,
              type: "tool_result",
              content: `Error: ${error.message}`,
              is_error: true,
            });

            toolExecutionLogs.push({
              type: "tool_execution_error",
              toolName: toolUse.name,
              error: error.message,
            });
          }
        }

        // Get final response from Claude with tool results
        console.log("🧠 Getting final response from Claude...");
        toolExecutionLogs.push({ type: "claude_final_request" });

        const messages = [...sessionData.messages];
        messages.push({
          role: "assistant",
          content: response.content,
        });
        messages.push({
          role: "user",
          content: toolResults,
        });

        const finalResponseResult = await anthropic.messages.create({
          model: "claude-3-5-sonnet-20241022",
          max_tokens: 2000,
          system: systemMessage.content,
          messages: messages,
        });

        finalResponse =
          finalResponseResult.content[0]?.text ||
          "מצטער, לא הצלחתי לעבד את הבקשה.";

        toolExecutionLogs.push({
          type: "claude_final_response",
          responseLength: finalResponse.length,
          responsePreview:
            finalResponse.slice(0, 100) +
            (finalResponse.length > 100 ? "..." : ""),
        });
      } else {
        // Direct response without tools
        finalResponse =
          response.content[0]?.text || "מצטער, לא הצלחתי לעבד את הבקשה.";

        toolExecutionLogs.push({
          type: "claude_direct_response",
          responseLength: finalResponse.length,
        });
      }

      // Add assistant response to session
      sessionData.messages.push({ role: "assistant", content: finalResponse });
      sessions.set(currentSessionId, sessionData);

      const executionTime = Date.now() - startTime;

      res.json({
        reply: finalResponse,
        sessionId: currentSessionId,
        toolExecutionLogs,
        debug: {
          totalToolsUsed,
          executionTime,
          logCount: toolExecutionLogs.length,
        },
      });
    } catch (error) {
      console.error("❌ Claude API error:", error);
      res.status(500).json({
        error: "שגיאה בעיבוד הבקשה",
        details: error.message,
      });
    }
  } catch (error) {
    console.error("❌ General error:", error);
    res.status(500).json({
      error: "שגיאה כללית בשרת",
      details: error.message,
    });
  }
});

const PORT = process.env.PORT || 3001;

app.listen(PORT, async () => {
  console.log(`🚀 Salim Shopping Chat Proxy Server running on port ${PORT}`);
  const SALIM_API_URL = process.env.SALIM_API_URL || "http://localhost:8000";
  console.log(`🔗 Connected to Salim API at ${SALIM_API_URL}`);
  console.log(`🔧 MCP Server URL: ${MCP_SERVER_URL}`);

  // Test MCP connection
  try {
    const tools = await getMCPTools();
    console.log(`✅ MCP connection successful! Loaded ${tools.length} tools`);
    tools.forEach((tool) =>
      console.log(`   - ${tool.name}: ${tool.description}`)
    );
  } catch (error) {
    console.warn(`⚠️ MCP connection failed: ${error.message}`);
    console.warn(`   Will fallback to direct API calls`);
  }

  console.log(`📱 Ready to process Hebrew shopping queries!`);
});

export default app;

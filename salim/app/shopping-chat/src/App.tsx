import { useState, useEffect, useRef } from 'react';
import { ChatMessage } from './components/ChatMessage';
import { ChatInput } from './components/ChatInput';
import type { Message } from './types';
import './App.css';

function App() {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: '1',
      content: 'שלום! 🛒 אני עוזר הקניות החכם שלך!\n\nאני יכול לעזור לך:\n• למצוא את המחירים הזולים ביותר למוצרים\n• להשוות מחירי סל קניות בין חנויות\n• למצוא את החנות הכי משתלמת באזור שלך\n\nפשוט שאל אותי משהו כמו:\n"איפה הכי זול חלב בכפר סבא?" או "איפה כדאי לקנות את הסל שלי ברעננה?"',
      role: 'assistant',
      timestamp: new Date(),
    }
  ]);
  const [isProcessing, setIsProcessing] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSendMessage = async (content: string) => {
    const userMessage: Message = {
      id: Date.now().toString(),
      content,
      role: 'user',
      timestamp: new Date(),
    };

    const processingMessage: Message = {
      id: (Date.now() + 1).toString(),
      content: '',
      role: 'assistant',
      timestamp: new Date(),
      isProcessing: true,
    };

    setMessages(prev => [...prev, userMessage, processingMessage]);
    setIsProcessing(true);

    try {
      console.log('🎯 Frontend: Sending message to Claude with MCP tools');
      
      // Call the chat endpoint which uses Claude with MCP tools
      const response = await fetch('http://localhost:3001/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: content,
          sessionId: sessionId
        }),
      });

      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }

      const data = await response.json();
      console.log('🤖 Frontend: Received response from Claude:', data);
      
      // Log detailed tool execution information
      if (data.toolExecutionLogs && data.toolExecutionLogs.length > 0) {
        console.log('🔧 TOOL EXECUTION LOGS:');
        console.log('=====================================');
        
        data.toolExecutionLogs.forEach((log, index) => {
          switch (log.type) {
            case 'claude_decision':
              console.log(`${index + 1}. 🤖 Claude decided to use ${log.tools.length} tools:`);
              log.tools.forEach(tool => {
                console.log(`   📝 Tool: ${tool.name}`);
                console.log('   📥 Input:', tool.input);
              });
              break;
              
            case 'tool_execution_start':
              console.log(`${index + 1}. 🔧 Executing ${log.toolName}...`);
              console.log('   📥 Input:', log.input);
              break;
              
            case 'tool_execution_success':
              console.log(`${index + 1}. ✅ ${log.toolName} completed (${log.executionTime}ms)`);
              console.log(`   📊 Result type: ${log.resultType}, length: ${log.resultLength}`);
              console.log('   📄 Preview:', log.resultPreview);
              break;
              
            case 'tool_execution_error':
              console.log(`${index + 1}. ❌ ${log.toolName} failed:`);
              console.log('   🚨 Error:', log.error);
              break;
              
            case 'claude_final_request':
              console.log(`${index + 1}. 🧠 Claude processing tool results...`);
              break;
              
            case 'claude_final_response':
              console.log(`${index + 1}. 📝 Claude final response (${log.responseLength} chars):`);
              console.log('   📄 Preview:', log.responsePreview);
              break;
              
            case 'claude_direct_response':
              console.log(`${index + 1}. 💬 Claude responded without tools (${log.responseLength} chars)`);
              break;
          }
        });
        
        console.log('=====================================');
        console.log('📊 EXECUTION SUMMARY:');
        console.log(`   🔧 Tools used: ${data.debug.totalToolsUsed}`);
        console.log(`   ⏱️  Total execution time: ${data.debug.executionTime}ms`);
        console.log(`   📝 Log entries: ${data.debug.logCount}`);
        console.log('=====================================');
      } else {
        console.log('💬 Claude responded directly without using any tools');
      }
      
      // Update session ID if provided
      if (data.sessionId) {
        setSessionId(data.sessionId);
      }
      
      // Remove processing message and add real response
      setMessages(prev => {
        const withoutProcessing = prev.slice(0, -1);
        return [...withoutProcessing, {
          id: (Date.now() + 2).toString(),
          content: data.reply,
          role: 'assistant',
          timestamp: new Date(),
        }];
      });

    } catch (error) {
      console.error('❌ Frontend: Error processing message:', error);
      setMessages(prev => {
        const withoutProcessing = prev.slice(0, -1);
        return [...withoutProcessing, {
          id: (Date.now() + 2).toString(),
          content: error instanceof Error ? error.message : 'מצטער, אירעה שגיאה. אנא נסה שוב בעוד מעט.',
          role: 'assistant',
          timestamp: new Date(),
        }];
      });
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>🛒 עוזר הקניות החכם</h1>
        <p>מחירים בזמן אמת מהסופרמרקטים בישראל</p>
      </header>
      
      <main className="chat-container">
        <div className="messages">
          {messages.map((message) => (
            <ChatMessage key={message.id} message={message} />
          ))}
          <div ref={messagesEndRef} />
        </div>
      </main>

      <ChatInput 
        onSendMessage={handleSendMessage} 
        disabled={isProcessing} 
      />
    </div>
  );
}

export default App;

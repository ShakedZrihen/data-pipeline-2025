import React, { useState, useRef, useEffect } from 'react';
import './ChatInput.css';

interface ChatInputProps {
  onSendMessage: (message: string) => void;
  disabled?: boolean;
}

export const ChatInput: React.FC<ChatInputProps> = ({ onSendMessage, disabled = false }) => {
  const [message, setMessage] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const adjustTextareaHeight = () => {
    const textarea = textareaRef.current;
    if (textarea) {
      textarea.style.height = 'auto';
      textarea.style.height = `${Math.min(textarea.scrollHeight, 200)}px`;
    }
  };

  useEffect(() => {
    adjustTextareaHeight();
  }, [message]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (message.trim() && !disabled) {
      onSendMessage(message.trim());
      setMessage('');
      // Reset textarea height
      if (textareaRef.current) {
        textareaRef.current.style.height = '44px';
      }
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  return (
    <div className="chat-input-container">
      <form onSubmit={handleSubmit} className="chat-input-form">
        <textarea
          ref={textareaRef}
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          onKeyPress={handleKeyPress}
          placeholder="שאל על מחירי מוצרים... (לדוגמה: 'איפה הכי זול חלב בכפר סבא?')"
          className="chat-input"
          disabled={disabled}
          dir="rtl"
          rows={1}
        />
        <button 
          type="submit" 
          disabled={!message.trim() || disabled}
          className="send-button"
          aria-label="שלח הודעה"
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" fill="currentColor"/>
          </svg>
        </button>
      </form>
      <div className="example-queries">
        <span>דוגמאות לשאלות:</span>
        <button onClick={() => setMessage('איפה הכי זול פופקורן ברעננה?')}>
          איפה הכי זול פופקורן ברעננה?
        </button>
        <button onClick={() => setMessage('הסל שלי: חלב, לחם, ביצים - איפה כדאי לקנות בכפר סבא?')}>
          השוואת סל קניות בכפר סבא
        </button>
      </div>
    </div>
  );
};
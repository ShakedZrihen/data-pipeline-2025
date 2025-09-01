import React from 'react';
import type { Message } from '../types';
import './ChatMessage.css';

interface ChatMessageProps {
  message: Message;
}

export const ChatMessage: React.FC<ChatMessageProps> = ({ message }) => {
  const getAvatarText = () => {
    if (message.role === 'user') {
      return 'א';
    }
    return '🛒';
  };

  return (
    <div className={`chat-message ${message.role}`}>
      <div className="message-avatar">
        {getAvatarText()}
      </div>
      <div className="message-content">
        {message.isProcessing ? (
          <div className="processing">
            <div className="typing-indicator">
              <span></span>
              <span></span>
              <span></span>
            </div>
            <span>מחפש מחירים...</span>
          </div>
        ) : (
          <div className="message-text">
            {message.content.split('\n').map((line, index) => (
              <React.Fragment key={index}>
                {line}
                {index < message.content.split('\n').length - 1 && <br />}
              </React.Fragment>
            ))}
          </div>
        )}
        <div className="message-time">
          {message.timestamp.toLocaleTimeString('he-IL', { 
            hour: '2-digit', 
            minute: '2-digit' 
          })}
        </div>
      </div>
    </div>
  );
};
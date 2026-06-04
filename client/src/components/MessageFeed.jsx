import React, { useEffect, useRef } from 'react';
import { MESSAGE_TYPES } from '../utils/constants';
import { useChatContext } from '../context/ChatContext';
import './MessageFeed.css';

const MessageFeed = ({ messages }) => {
  const { userId } = useChatContext();
  const messagesEndRef = useRef(null);
  const containerRef = useRef(null);

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const formatTime = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', {
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const renderMessage = (message, index) => {
    const isOwnMessage = message.user_id === userId;

    switch (message.type) {
      case MESSAGE_TYPES.CHAT:
        return (
          <div
            key={index}
            className={`message ${isOwnMessage ? 'own-message' : ''}`}
          >
            <div className="message-header">
              <span className="message-username">{message.username}</span>
              <span className="message-time">{formatTime(message.timestamp)}</span>
            </div>
            <div className="message-content">{message.content}</div>
          </div>
        );

      case MESSAGE_TYPES.PRIVATE:
        return (
          <div
            key={index}
            className={`message private-message ${isOwnMessage ? 'own-message' : ''}`}
          >
            <div className="message-header">
              <span className="message-username">
                {isOwnMessage ? `Для ${message.to_user_id}` : `От ${message.username}`}
              </span>
              <span className="message-time">{formatTime(message.timestamp)}</span>
            </div>
            <div className="message-content">{message.content}</div>
          </div>
        );

      case MESSAGE_TYPES.ERROR:
        return (
          <div key={index} className="error-message">
            <span>Ошибка: {message.error}</span>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="message-feed" ref={containerRef}>
      {messages.length === 0 ? (
        <div className="empty-state">
          <p>Сообщений пока нет. Начните общение!</p>
        </div>
      ) : (
        messages.map((message, index) => renderMessage(message, index))
      )}
      <div ref={messagesEndRef} />
    </div>
  );
};

export default MessageFeed;

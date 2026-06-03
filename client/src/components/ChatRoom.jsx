import React from 'react';
import { useChatContext } from '../context/ChatContext';
import { useChat } from '../hooks/useChat';
import RoomSelector from './RoomSelector';
import MessageFeed from './MessageFeed';
import MessageInput from './MessageInput';
import './ChatRoom.css';

const ChatRoom = () => {
  const { currentRoom, username, theme, toggleTheme } = useChatContext();
  const { messages, isConnected, isReconnecting, sendMessage } = useChat(currentRoom);

  const handleSendMessage = (content) => {
    sendMessage(content);
  };

  return (
    <div className="chat-room">
      <div className="chat-sidebar">
        <div className="user-info">
          <div className="user-avatar">{username.charAt(0).toUpperCase()}</div>
          <div className="user-details">
            <div className="user-name">{username}</div>
            <div className={`user-status ${isConnected ? 'online' : 'offline'}`}>
              {isReconnecting ? 'Переподключение...' : isConnected ? 'В сети' : 'Не в сети'}
            </div>
          </div>
        </div>
        <RoomSelector />
      </div>

      <div className="chat-main">
        <div className="chat-header">
          <h2># {currentRoom}</h2>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <button className="theme-toggle" onClick={toggleTheme}>
              {theme === 'light' ? 'Темная' : 'Светлая'}
            </button>
            <div className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
              {isConnected ? '● Подключено' : '○ Отключено'}
            </div>
          </div>
        </div>

        <MessageFeed messages={messages} />
        <MessageInput onSendMessage={handleSendMessage} disabled={!isConnected} />
      </div>
    </div>
  );
};

export default ChatRoom;

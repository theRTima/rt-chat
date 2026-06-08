import React from 'react';
import { useChatContext } from '../context/ChatContext';
import { useChat } from '../hooks/useChat';
import { MESSAGE_TYPES } from '../utils/constants';
import RoomSelector from './RoomSelector';
import MessageFeed from './MessageFeed';
import MessageInput from './MessageInput';
import './ChatRoom.css';

const ChatRoom = () => {
  const { currentRoom, username, theme, toggleTheme, activeDmUser, setActiveDmUser } = useChatContext();
  const {
    messages, isConnected, isReconnecting, participantCount,
    sendMessage, dmContacts, dmMessages, lookupUser,
    notification, clearNotification,
  } = useChat(currentRoom);

  const handleSendMessage = (content) => {
    if (activeDmUser) {
      sendMessage(content, MESSAGE_TYPES.PRIVATE, activeDmUser);
    } else {
      sendMessage(content);
    }
  };

  const activeDmMessages = activeDmUser ? (dmMessages[activeDmUser] || []) : [];

  const getHeaderTitle = () => {
    if (activeDmUser) {
      const contact = dmContacts.find((c) => c.userId === activeDmUser);
      return contact ? contact.username : activeDmUser;
    }
    return `# ${currentRoom}`;
  };

  const getParticipantInfo = () => {
    if (activeDmUser) return null;
    if (participantCount > 0) {
      return `${participantCount} участников`;
    }
    return null;
  };

  const handleBackToChannels = () => {
    setActiveDmUser(null);
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
        <RoomSelector
          dmContacts={dmContacts}
          dmMessages={dmMessages}
          onLookupUser={lookupUser}
        />
      </div>

      <div className="chat-main">
        <div className="chat-header">
          <div className="chat-header-left">
            {activeDmUser && (
              <button className="back-button" onClick={handleBackToChannels} title="Назад к каналам">
                &larr;
              </button>
            )}
            <h2>{getHeaderTitle()}</h2>
            {getParticipantInfo() && (
              <span className="participant-count">{getParticipantInfo()}</span>
            )}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <button className="theme-toggle" onClick={toggleTheme}>
              {theme === 'light' ? 'Темная' : 'Светлая'}
            </button>
            <div className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
              {isConnected ? '● Подключено' : '○ Отключено'}
            </div>
          </div>
        </div>

        <MessageFeed messages={activeDmUser ? activeDmMessages : messages} />
        <MessageInput onSendMessage={handleSendMessage} disabled={!isConnected} />
      </div>

      {notification && (
        <div className="toast-notification" onClick={clearNotification}>
          {notification}
        </div>
      )}
    </div>
  );
};

export default ChatRoom;

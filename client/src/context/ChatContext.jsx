import React, { createContext, useContext, useState, useEffect } from 'react';

const ChatContext = createContext();

export const ChatProvider = ({ children }) => {
  const [userId, setUserId] = useState(() => {
    return localStorage.getItem('userId') || `user_${Date.now()}`;
  });

  const [username, setUsername] = useState(() => {
    return localStorage.getItem('username') || `User_${userId.slice(-4)}`;
  });

  const [currentRoom, setCurrentRoom] = useState('general');

  useEffect(() => {
    localStorage.setItem('userId', userId);
    localStorage.setItem('username', username);
  }, [userId, username]);

  const updateUser = (newUserId, newUsername) => {
    setUserId(newUserId);
    setUsername(newUsername);
  };

  return (
    <ChatContext.Provider
      value={{
        userId,
        username,
        currentRoom,
        setCurrentRoom,
        updateUser,
      }}
    >
      {children}
    </ChatContext.Provider>
  );
};

export const useChatContext = () => {
  const context = useContext(ChatContext);
  if (!context) {
    throw new Error('useChatContext must be used within ChatProvider');
  }
  return context;
};

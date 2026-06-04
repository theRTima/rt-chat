import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';

const ChatContext = createContext();

export const ChatProvider = ({ children }) => {
  const [userId, setUserId] = useState(() => {
    return localStorage.getItem('userId') || `user_${Date.now()}`;
  });

  const [username, setUsername] = useState(() => {
    return localStorage.getItem('username') || `User_${userId.slice(-4)}`;
  });

  const [currentRoom, setCurrentRoom] = useState('general');
  const [activeDmUser, setActiveDmUser] = useState(null);

  const [theme, setTheme] = useState(() => {
    return localStorage.getItem('theme') || 'light';
  });

  useEffect(() => {
    localStorage.setItem('userId', userId);
    localStorage.setItem('username', username);
  }, [userId, username]);

  useEffect(() => {
    localStorage.setItem('theme', theme);
    document.documentElement.setAttribute('data-theme', theme);
  }, [theme]);

  const updateUser = (newUserId, newUsername) => {
    setUserId(newUserId);
    setUsername(newUsername);
  };

  const toggleTheme = useCallback(() => {
    setTheme((prev) => (prev === 'light' ? 'dark' : 'light'));
  }, []);

  return (
    <ChatContext.Provider
      value={{
        userId,
        username,
        currentRoom,
        setCurrentRoom,
        updateUser,
        theme,
        toggleTheme,
        activeDmUser,
        setActiveDmUser,
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

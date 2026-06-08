import React, { useEffect } from 'react';
import { ChatProvider } from './context/ChatContext';
import ChatRoom from './components/ChatRoom';
import './App.css';

function App() {
  useEffect(() => {
    if ('Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission();
    }
  }, []);

  return (
    <ChatProvider>
      <div className="app">
        <ChatRoom />
      </div>
    </ChatProvider>
  );
}

export default App;

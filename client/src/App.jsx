import React, { useEffect, useRef } from 'react';
import { ChatProvider } from './context/ChatContext';
import ChatRoom from './components/ChatRoom';
import './App.css';

function App() {
  const requestedRef = useRef(false);

  useEffect(() => {
    const handler = () => {
      if (requestedRef.current) return;
      if (!('Notification' in window)) {
        console.log('Browser notifications not supported');
        return;
      }
      if (Notification.permission !== 'default') {
        console.log('Notification permission already:', Notification.permission);
        return;
      }
      requestedRef.current = true;
      Notification.requestPermission().then((permission) => {
        console.log('Notification permission:', permission);
      });
      document.removeEventListener('click', handler);
    };
    document.addEventListener('click', handler);
    return () => document.removeEventListener('click', handler);
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

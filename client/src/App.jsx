import React from 'react';
import { ChatProvider } from './context/ChatContext';
import ChatRoom from './components/ChatRoom';
import './App.css';

function App() {
  return (
    <ChatProvider>
      <div className="app">
        <ChatRoom />
      </div>
    </ChatProvider>
  );
}

export default App;

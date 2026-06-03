export const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8080/ws';

export const MESSAGE_TYPES = {
  JOIN_ROOM: 'join_room',
  LEAVE_ROOM: 'leave_room',
  CHAT: 'chat',
  PRIVATE: 'private',
  ERROR: 'error',
  USER_JOINED: 'user_joined',
  USER_LEFT: 'user_left',
};

export const RECONNECT_DELAY = 3000; // 3 seconds
export const MAX_RECONNECT_ATTEMPTS = 5;

const loc = window.location;
const wsProtocol = loc.protocol === 'https:' ? 'wss:' : 'ws:';

export const WS_URL = import.meta.env.VITE_WS_URL || `${wsProtocol}//${loc.host}/ws`;

export const MESSAGE_TYPES = {
  JOIN_ROOM: 'join_room',
  LEAVE_ROOM: 'leave_room',
  CHAT: 'chat',
  PRIVATE: 'private',
  ERROR: 'error',
  USER_JOINED: 'user_joined',
  USER_LEFT: 'user_left',
  USER_LOOKUP: 'user_lookup',
  USER_FOUND: 'user_found',
  USER_NOT_FOUND: 'user_not_found',
};

export const RECONNECT_DELAY = 3000;
export const MAX_RECONNECT_ATTEMPTS = 5;

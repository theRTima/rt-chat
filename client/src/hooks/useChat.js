import { useState, useEffect, useRef, useCallback } from 'react';
import { useChatContext } from '../context/ChatContext';
import { WS_URL, MESSAGE_TYPES, RECONNECT_DELAY, MAX_RECONNECT_ATTEMPTS } from '../utils/constants';

const tryBrowserNotification = (title, body) => {
  console.log('tryBrowserNotification called:', { title, body, permission: Notification?.permission });
  if (!('Notification' in window)) {
    console.log('Browser notifications not supported');
    return;
  }

  if (Notification.permission === 'granted') {
    try {
      console.log('Creating browser notification:', title);
      const n = new Notification(title, { body, icon: '/favicon.svg', tag: 'rt-chat' });
      setTimeout(() => n.close(), 5000);
    } catch (error) {
      console.error('Browser notification failed:', error);
    }
  } else if (Notification.permission !== 'denied') {
    console.log('Requesting notification permission');
    Notification.requestPermission().then((permission) => {
      console.log('Permission result:', permission);
      if (permission === 'granted') {
        try {
          const n = new Notification(title, { body, icon: '/favicon.svg', tag: 'rt-chat' });
          setTimeout(() => n.close(), 5000);
        } catch (error) {
          console.error('Browser notification failed after permission grant:', error);
        }
      }
    });
  } else {
    console.log('Notification permission denied');
  }
};

export const useChat = (roomId) => {
  const { userId, username, activeDmUser } = useChatContext();
  const [messages, setMessages] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isReconnecting, setIsReconnecting] = useState(false);
  const [participantCount, setParticipantCount] = useState(0);

  // Notification state
  const [notification, setNotification] = useState(null);
  const notifTimerRef = useRef(null);

  // DM state
  const [dmContacts, setDmContacts] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem('dmContacts') || '[]');
    } catch {
      return [];
    }
  });
  const [dmMessages, setDmMessages] = useState({});
  const wsRef = useRef(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimeoutRef = useRef(null);
  const currentRoomRef = useRef(null);
  const roomGenRef = useRef(0);
  const lookupResolveRef = useRef(null);
  const lastDmLoadRef = useRef(0);

  // Persist contacts
  useEffect(() => {
    localStorage.setItem('dmContacts', JSON.stringify(dmContacts));
  }, [dmContacts]);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return;
    }

    try {
      const url = `${WS_URL}?user_id=${encodeURIComponent(userId)}&username=${encodeURIComponent(username)}`;
      const ws = new WebSocket(url);

      ws.onopen = () => {
        console.log('WebSocket connected');
        setIsConnected(true);
        setIsReconnecting(false);
        reconnectAttemptsRef.current = 0;
      };

      ws.onmessage = (event) => {
        try {
          const parts = event.data.split('\n');
          const gen = roomGenRef.current;
          for (const part of parts) {
            if (!part) continue;
            const message = JSON.parse(part);

            if (message.type === MESSAGE_TYPES.USER_JOINED || message.type === MESSAGE_TYPES.USER_LEFT) {
              if (typeof message.participant_count === 'number') {
                setParticipantCount(message.participant_count);
              }
              continue;
            }

            if (message.type === MESSAGE_TYPES.PRIVATE) {
              const otherId = message.user_id === userId ? message.to_user_id : message.user_id;
              const otherUsername = message.user_id === userId ? message.to_user_id : message.username;

              setDmContacts((prev) => {
                if (prev.some((c) => c.userId === otherId)) return prev;
                return [...prev, { userId: otherId, username: otherUsername || otherId }];
              });

              setDmMessages((prev) => {
                const userMsgs = prev[otherId] || [];
                return { ...prev, [otherId]: [...userMsgs, message] };
              });

              if (message.user_id !== userId && Date.now() - lastDmLoadRef.current > 500) {
                tryBrowserNotification(`DM от ${message.username}`, 'Новое сообщение');
                setNotification(`DM от ${message.username}`);
              }
              continue;
            }

            if (message.type === MESSAGE_TYPES.USER_FOUND) {
              if (lookupResolveRef.current) {
                lookupResolveRef.current({ found: true, userId: message.user_id, username: message.username });
                lookupResolveRef.current = null;
              }
              continue;
            }

            if (message.type === MESSAGE_TYPES.USER_NOT_FOUND) {
              if (lookupResolveRef.current) {
                lookupResolveRef.current({ found: false });
                lookupResolveRef.current = null;
              }
              continue;
            }

            if (message.type === MESSAGE_TYPES.ERROR) {
              console.warn('Server error:', message.error || message.content);
              continue;
            }

            if (message.type === MESSAGE_TYPES.CHAT && message.user_id !== userId) {
              // Only show notification if message is from a different channel than currently viewed
              // or if user is currently in DM view
              const currentRoom = currentRoomRef.current || roomId;
              const shouldNotify = activeDmUser || message.room_id !== currentRoom;
              console.log('Channel notification check:', {
                messageRoom: message.room_id,
                currentRoom,
                activeDmUser,
                shouldNotify,
                currentRoomRef: currentRoomRef.current,
                roomIdParam: roomId
              });
              if (shouldNotify) {
                console.log('Showing notification for channel message');
                tryBrowserNotification(`# ${message.room_id} — ${message.username}`, 'Новое сообщение');
                setNotification(`# ${message.room_id} — ${message.username}`);
              }
            }

            setMessages((prev) => {
              if (roomGenRef.current !== gen) return prev;
              return [...prev, message];
            });
          }
        } catch (error) {
          console.error('Failed to parse message:', error);
        }
      };

      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
      };

      ws.onclose = () => {
        console.log('WebSocket disconnected');
        setIsConnected(false);
        currentRoomRef.current = null;

        if (wsRef.current === ws) {
          wsRef.current = null;
        }

        if (reconnectAttemptsRef.current < MAX_RECONNECT_ATTEMPTS) {
          setIsReconnecting(true);
          reconnectAttemptsRef.current += 1;
          reconnectTimeoutRef.current = setTimeout(() => {
            console.log(`Reconnecting... Attempt ${reconnectAttemptsRef.current}`);
            connect();
          }, RECONNECT_DELAY);
        } else {
          console.error('Max reconnection attempts reached');
          setIsReconnecting(false);
        }
      };

      wsRef.current = ws;
    } catch (error) {
      console.error('Failed to create WebSocket:', error);
    }
  }, [userId, username]);

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
    }

    if (wsRef.current) {
      if (currentRoomRef.current) {
        const leaveMessage = {
          type: MESSAGE_TYPES.LEAVE_ROOM,
          room_id: currentRoomRef.current,
        };
        wsRef.current.send(JSON.stringify(leaveMessage));
      }

      wsRef.current.close();
      wsRef.current = null;
    }

    setIsConnected(false);
    setIsReconnecting(false);
  }, []);

  const sendMessage = useCallback((content, type = MESSAGE_TYPES.CHAT, toUserId = null) => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      console.error('WebSocket is not connected');
      return false;
    }

    const message = {
      type,
      content,
    };

    if (type === MESSAGE_TYPES.CHAT) {
      message.room_id = roomId;
    } else if (type === MESSAGE_TYPES.PRIVATE) {
      message.to_user_id = toUserId;
    }

    try {
      wsRef.current.send(JSON.stringify(message));
      return true;
    } catch (error) {
      console.error('Failed to send message:', error);
      return false;
    }
  }, [roomId]);

  const lookupUser = useCallback((lookupUsername) => {
    return new Promise((resolve) => {
      if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
        resolve({ found: false });
        return;
      }

      lookupResolveRef.current = resolve;

      const message = {
        type: MESSAGE_TYPES.USER_LOOKUP,
        content: lookupUsername,
      };
      wsRef.current.send(JSON.stringify(message));

      setTimeout(() => {
        if (lookupResolveRef.current === resolve) {
          lookupResolveRef.current = null;
          resolve({ found: false, timeout: true });
        }
      }, 5000);
    });
  }, []);

  const joinRoom = useCallback((newRoomId) => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      return;
    }

    if (currentRoomRef.current && currentRoomRef.current !== newRoomId) {
      const leaveMessage = {
        type: MESSAGE_TYPES.LEAVE_ROOM,
        room_id: currentRoomRef.current,
      };
      wsRef.current.send(JSON.stringify(leaveMessage));
    }

    const joinMessage = {
      type: MESSAGE_TYPES.JOIN_ROOM,
      room_id: newRoomId,
    };
    wsRef.current.send(JSON.stringify(joinMessage));
    currentRoomRef.current = newRoomId;
    console.log('Joined room:', newRoomId, 'currentRoomRef updated to:', currentRoomRef.current);
  }, []);

  useEffect(() => {
    roomGenRef.current += 1;
    setMessages([]);
    setParticipantCount(0);
  }, [roomId]);

  useEffect(() => {
    if (isConnected && roomId) {
      joinRoom(roomId);
    }
  }, [roomId, isConnected, joinRoom]);

  useEffect(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN && activeDmUser) {
      setDmMessages((prev) => ({ ...prev, [activeDmUser]: [] }));
      lastDmLoadRef.current = Date.now();
      wsRef.current.send(JSON.stringify({
        type: MESSAGE_TYPES.LOAD_DM_HISTORY,
        to_user_id: activeDmUser,
      }));
    }
  }, [activeDmUser, isConnected]);

  const clearNotification = useCallback(() => {
    setNotification(null);
    if (notifTimerRef.current) clearTimeout(notifTimerRef.current);
  }, []);

  useEffect(() => {
    if (notification) {
      if (notifTimerRef.current) clearTimeout(notifTimerRef.current);
      notifTimerRef.current = setTimeout(() => setNotification(null), 4000);
    }
  }, [notification]);

  useEffect(() => {
    connect();

    return () => {
      disconnect();
    };
  }, [connect, disconnect]);

  return {
    messages,
    isConnected,
    isReconnecting,
    participantCount,
    sendMessage,
    joinRoom,
    disconnect,
    dmContacts,
    setDmContacts,
    dmMessages,
    lookupUser,
    notification,
    clearNotification,
  };
};

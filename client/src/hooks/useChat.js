import { useState, useEffect, useRef, useCallback } from 'react';
import { useChatContext } from '../context/ChatContext';
import { WS_URL, MESSAGE_TYPES, RECONNECT_DELAY, MAX_RECONNECT_ATTEMPTS } from '../utils/constants';

export const useChat = (roomId) => {
  const { userId, username } = useChatContext();
  const [messages, setMessages] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isReconnecting, setIsReconnecting] = useState(false);

  const wsRef = useRef(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimeoutRef = useRef(null);
  const currentRoomRef = useRef(null);

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
          const message = JSON.parse(event.data);
          setMessages((prev) => [...prev, message]);
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
        wsRef.current = null;

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

    setMessages([]);
  }, []);

  useEffect(() => {
    connect();

    return () => {
      disconnect();
    };
  }, [connect, disconnect]);

  useEffect(() => {
    if (isConnected && roomId) {
      joinRoom(roomId);
    }
  }, [roomId, isConnected, joinRoom]);

  return {
    messages,
    isConnected,
    isReconnecting,
    sendMessage,
    joinRoom,
    disconnect,
  };
};

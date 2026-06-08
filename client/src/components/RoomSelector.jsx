import React, { useState, useEffect, useRef } from 'react';
import { useChatContext } from '../context/ChatContext';
import './RoomSelector.css';

const DEFAULT_CHANNELS = [
  { id: 'general', name: 'Общий' },
  { id: 'random', name: 'Случайный' },
];

const mergeChannels = (serverRooms, local) => {
  const seen = new Set();
  const merged = [];
  for (const ch of DEFAULT_CHANNELS) {
    merged.push(ch);
    seen.add(ch.id);
  }
  for (const ch of local) {
    if (!seen.has(ch.id)) {
      merged.push(ch);
      seen.add(ch.id);
    }
  }
  for (const ch of serverRooms) {
    if (!seen.has(ch.room_id)) {
      merged.push({ id: ch.room_id, name: ch.name || ch.room_id });
      seen.add(ch.room_id);
    }
  }
  return merged;
};

const RoomSelector = ({ dmContacts, dmMessages, onLookupUser }) => {
  const { currentRoom, setCurrentRoom, activeDmUser, setActiveDmUser } = useChatContext();
  const [showAddPerson, setShowAddPerson] = useState(false);
  const [lookupName, setLookupName] = useState('');
  const [lookupError, setLookupError] = useState('');
  const [lookupLoading, setLookupLoading] = useState(false);
  const [channels, setChannels] = useState(() => {
    try {
      const saved = JSON.parse(localStorage.getItem('channels') || 'null');
      if (saved && Array.isArray(saved) && saved.length > 0) return saved;
    } catch {}
    return DEFAULT_CHANNELS;
  });
  const [showCreateChannel, setShowCreateChannel] = useState(false);
  const [newChannelName, setNewChannelName] = useState('');
  const mergedRef = useRef(false);

  const persistChannels = (updated) => {
    localStorage.setItem('channels', JSON.stringify(updated));
  };

  useEffect(() => {
    if (mergedRef.current) return;
    mergedRef.current = true;

    fetch('/rooms')
      .then((res) => res.json())
      .then((serverRooms) => {
        if (!Array.isArray(serverRooms)) return;
        setChannels((prev) => {
          const merged = mergeChannels(serverRooms, prev);
          persistChannels(merged);
          return merged;
        });
      })
      .catch(() => {});
  }, []);

  const handleCreateChannel = () => {
    const name = newChannelName.trim();
    if (!name) return;

    const id = name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
    if (!id) return;

    if (channels.some((c) => c.id === id)) return;

    const updated = [...channels, { id, name }];
    setChannels(updated);
    persistChannels(updated);
    setShowCreateChannel(false);
    setNewChannelName('');
    handleSelectRoom(id);
  };

  const handleAddPerson = async () => {
    const name = lookupName.trim();
    if (!name) return;

    setLookupLoading(true);
    setLookupError('');

    const result = await onLookupUser(name);
    setLookupLoading(false);

    if (result.found) {
      setShowAddPerson(false);
      setLookupName('');
      setActiveDmUser(result.userId);
    } else {
      setLookupError(`Пользователь "${name}" не найден или не в сети`);
    }
  };

  const handleSelectRoom = (roomId) => {
    setActiveDmUser(null);
    setCurrentRoom(roomId);
  };

  const handleSelectDm = (contactUserId) => {
    setActiveDmUser(contactUserId);
  };

  const getLastDmMessage = (contactUserId) => {
    const msgs = dmMessages[contactUserId];
    if (!msgs || msgs.length === 0) return null;
    return msgs[msgs.length - 1];
  };

  return (
    <div className="room-selector">
      <div className="sidebar-section">
        <div className="sidebar-section-header">
          <h3 className="sidebar-section-title">Каналы</h3>
          <button
            className="sidebar-add-button"
            onClick={() => { setShowCreateChannel(true); setShowAddPerson(false); }}
            title="Создать канал"
          >
            +
          </button>
        </div>

        {showCreateChannel && (
          <div className="add-person-form">
            <input
              type="text"
              className="add-person-input"
              placeholder="Название канала..."
              value={newChannelName}
              onChange={(e) => setNewChannelName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleCreateChannel();
                if (e.key === 'Escape') { setShowCreateChannel(false); setNewChannelName(''); }
              }}
              autoFocus
            />
            <div className="add-person-actions">
              <button
                className="add-person-confirm"
                onClick={handleCreateChannel}
                disabled={!newChannelName.trim()}
              >
                Создать
              </button>
              <button
                className="add-person-cancel"
                onClick={() => { setShowCreateChannel(false); setNewChannelName(''); }}
              >
                Отмена
              </button>
            </div>
          </div>
        )}

        <div className="sidebar-list">
          {channels.map((room) => (
            <button
              key={room.id}
              className={`sidebar-button ${currentRoom === room.id && !activeDmUser ? 'active' : ''}`}
              onClick={() => handleSelectRoom(room.id)}
            >
              # {room.name}
            </button>
          ))}
        </div>
      </div>

      <div className="sidebar-section">
        <div className="sidebar-section-header">
          <h3 className="sidebar-section-title">Личные сообщения</h3>
          <button
            className="sidebar-add-button"
            onClick={() => { setShowAddPerson(true); setLookupError(''); }}
            title="Добавить пользователя"
          >
            +
          </button>
        </div>

        {showAddPerson && (
          <div className="add-person-form">
            <input
              type="text"
              className="add-person-input"
              placeholder="Введите имя пользователя..."
              value={lookupName}
              onChange={(e) => setLookupName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleAddPerson();
                if (e.key === 'Escape') { setShowAddPerson(false); setLookupName(''); setLookupError(''); }
              }}
              autoFocus
              disabled={lookupLoading}
            />
            {lookupError && <div className="add-person-error">{lookupError}</div>}
            <div className="add-person-actions">
              <button
                className="add-person-confirm"
                onClick={handleAddPerson}
                disabled={!lookupName.trim() || lookupLoading}
              >
                {lookupLoading ? 'Поиск...' : 'Добавить'}
              </button>
              <button
                className="add-person-cancel"
                onClick={() => { setShowAddPerson(false); setLookupName(''); setLookupError(''); }}
              >
                Отмена
              </button>
            </div>
          </div>
        )}

        <div className="sidebar-list">
          {dmContacts.length === 0 && !showAddPerson && (
            <div className="sidebar-empty">Нет контактов</div>
          )}
          {dmContacts.map((contact) => {
            const lastMsg = getLastDmMessage(contact.userId);
            return (
              <button
                key={contact.userId}
                className={`sidebar-button dm-button ${activeDmUser === contact.userId ? 'active' : ''}`}
                onClick={() => handleSelectDm(contact.userId)}
              >
                <span className="dm-button-name">{contact.username}</span>
                {lastMsg && (
                  <span className="dm-button-preview">
                    {lastMsg.content.length > 30 ? lastMsg.content.slice(0, 30) + '...' : lastMsg.content}
                  </span>
                )}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
};

export default RoomSelector;

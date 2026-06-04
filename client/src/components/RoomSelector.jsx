import React, { useState } from 'react';
import { useChatContext } from '../context/ChatContext';
import './RoomSelector.css';

const CHANNELS = [
  { id: 'general', name: 'Общий' },
  { id: 'random', name: 'Случайный' },
];

const RoomSelector = ({ dmContacts, dmMessages, onLookupUser }) => {
  const { currentRoom, setCurrentRoom, activeDmUser, setActiveDmUser } = useChatContext();
  const [showAddPerson, setShowAddPerson] = useState(false);
  const [lookupName, setLookupName] = useState('');
  const [lookupError, setLookupError] = useState('');
  const [lookupLoading, setLookupLoading] = useState(false);

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
        <h3 className="sidebar-section-title">Каналы</h3>
        <div className="sidebar-list">
          {CHANNELS.map((room) => (
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

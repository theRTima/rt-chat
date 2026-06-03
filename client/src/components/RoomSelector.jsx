import React from 'react';
import { useChatContext } from '../context/ChatContext';
import './RoomSelector.css';

const AVAILABLE_ROOMS = [
  { id: 'general', name: 'Общий' },
  { id: 'random', name: 'Случайный' },
  { id: 'tech', name: 'Технический' },
  { id: 'dev-team', name: 'Разработка' },
];

const RoomSelector = () => {
  const { currentRoom, setCurrentRoom } = useChatContext();

  return (
    <div className="room-selector">
      <h3>Комнаты</h3>
      <div className="room-list">
        {AVAILABLE_ROOMS.map((room) => (
          <button
            key={room.id}
            className={`room-button ${currentRoom === room.id ? 'active' : ''}`}
            onClick={() => setCurrentRoom(room.id)}
          >
            # {room.name}
          </button>
        ))}
      </div>
    </div>
  );
};

export default RoomSelector;

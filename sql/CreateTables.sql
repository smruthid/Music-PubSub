CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(50) UNIQUE NOT NULL,
  email VARCHAR(100) UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE subscriptions (
  id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users(id) ON DELETE CASCADE,
  genre VARCHAR(256),
  artist VARCHAR(256),
  city VARCHAR(256) NOT NULL,
  state VARCHAR(256) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),

  CONSTRAINT valid_subscription_type CHECK (
    (genre IS NOT NULL AND artist IS NULL)
    OR (artist IS NOT NULL AND genre IS NULL)
    OR (genre IS NULL AND artist IS NULL)
  ),

  CONSTRAINT valid_date_range CHECK (end_date >= start_date)
);

-- For city/state only subs
CREATE UNIQUE INDEX unique_city_state_sub
  ON subscriptions (user_id, city, state)
  WHERE genre IS NULL AND artist IS NULL;

-- For genre subs
CREATE UNIQUE INDEX unique_genre_sub
  ON subscriptions (user_id, genre, city, state)
  WHERE artist IS NULL AND genre IS NOT NULL;

-- For artist subs
CREATE UNIQUE INDEX unique_artist_sub
  ON subscriptions (user_id, artist, city, state)
  WHERE genre IS NULL AND artist IS NOT NULL;

-- Events (published by venues/artists)
CREATE TABLE events (
  id SERIAL PRIMARY KEY,
  title VARCHAR(256) NOT NULL,
  artist VARCHAR(256) NOT NULL,
  genre VARCHAR(256) NOT NULL,
  city VARCHAR(256) NOT NULL,
  state VARCHAR(256) NOT NULL,
  venue VARCHAR(256),
  event_date_time TIMESTAMP NOT NULL,
  priority VARCHAR(20) DEFAULT 'normal', -- 'normal' or 'urgent'
  published_at TIMESTAMP DEFAULT NOW()
);

-- Notification log (track what was sent to whom)
CREATE TABLE notifications (
  id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users(id) ON DELETE CASCADE,
  event_id INT REFERENCES events(id) ON DELETE CASCADE,
  sent_at TIMESTAMP DEFAULT NOW()
);

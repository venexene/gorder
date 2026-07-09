CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'user',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO users (username, password_hash, role)
VALUES ('admin', '$2y$10$.EbkHOLZG6UQP2i917mu.eJEswidNxY5DSxGVl1vI21TIkG3UZep2', 'admin');
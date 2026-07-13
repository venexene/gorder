CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'user',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO users (username, password_hash, role) VALUES
('admin', '$2a$10$E5SWv..hzx3hbgSGG6bijeoggUuyiAtVprKCpl04QFBvksdlwKSMm', 'admin'),
('alice', '$2a$10$tsYLHkt4WE6TZqCsE.EzCOd/gWRwSSaZsueb9jS0NKLw8j0ls9gWK', 'user'),
('bob',   '$2a$10$t.EeP5hXa/QSaOaa5r4y7.SUXJsTgbp7sL7Y/LSz.Ihqo//wpeJfi', 'user');

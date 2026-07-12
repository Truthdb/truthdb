INSERT INTO archive (id, name) SELECT id, name FROM users WHERE active = 1;

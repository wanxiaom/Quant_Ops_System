USE quant_ops;

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(64) PRIMARY KEY,
    username VARCHAR(64) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(128) NOT NULL DEFAULT '',
    role VARCHAR(32) NOT NULL DEFAULT 'researcher',
    status VARCHAR(16) NOT NULL DEFAULT 'active',
    last_login_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_users_role_status(role, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO users
    (user_id, username, password_hash, name, email, role, status)
VALUES
    ('u_admin', 'your_username', SHA2('admin123', 256), '系统管理员', 'your_username@quant.local', 'your_username', 'active');

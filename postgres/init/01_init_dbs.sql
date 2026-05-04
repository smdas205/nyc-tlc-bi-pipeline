CREATE DATABASE warehouse;
CREATE USER powerbireader WITH PASSWORD 'powerbiuser';
GRANT CONNECT ON DATABASE warehouse TO powerbireader;
GRANT USAGE ON SCHEMA public TO powerbireader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO powerbireader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO powerbireader;

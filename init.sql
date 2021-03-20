CREATE USER tarpit WITH PASSWORD 't4rP!t';
CREATE DATABASE ssh_tarpit
    WITH
    OWNER = tarpit
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

GRANT ALL PRIVILEGES ON DATABASE "ssh_tarpit" to tarpit;

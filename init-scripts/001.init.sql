-- init.sql
-- This script will be executed to initialize the database

-- Drop the existing schema if it exists
DO $$
BEGIN
    EXECUTE 'DROP SCHEMA IF EXISTS public CASCADE';
    EXECUTE 'CREATE SCHEMA public';
END $$;

-- Restore the database from the plain SQL dump file
\i /var/log/postgresql/backup.sql

-- Grant privileges
GRANT ALL ON SCHEMA public TO public;
GRANT ALL ON SCHEMA public TO postgres;

-- Set the schema to the default
SET search_path TO public;

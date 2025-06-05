-- Create the outbox database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'outbox')
BEGIN
    CREATE DATABASE outbox;
END
GO

-- Switch to the outbox database
USE outbox;
GO

-- Create the outbox table
CREATE TABLE outbox (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    created_at DATETIMEOFFSET(3) NOT NULL,
    scheduled_at DATETIMEOFFSET(3) NOT NULL,
    metadata VARBINARY(MAX),
    payload VARBINARY(MAX) NOT NULL,
    times_attempted INT NOT NULL
);

CREATE INDEX idx_outbox_created_at ON outbox (created_at);
CREATE INDEX idx_outbox_scheduled_at ON outbox (scheduled_at);
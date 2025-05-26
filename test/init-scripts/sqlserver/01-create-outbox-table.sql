-- Create the outbox database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'outbox')
BEGIN
    CREATE DATABASE outbox;
END
GO

-- Switch to the outbox database
USE outbox;
GO

-- Create the Outbox table
CREATE TABLE Outbox (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    created_at DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
    context VARBINARY(MAX) NOT NULL,
    payload VARBINARY(MAX) NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at); 
// Package outbox implements the transactional outbox pattern, ensuring reliable message publishing
// by first persisting messages within a database transaction before they are sent to a message broker.
//
// The core of the pattern involves two main operations:
//
//  1. Writing: Messages are stored durably in an "outbox" table as part of the application's
//     local database transaction. This ensures that the message is captured atomically with
//     the business operation that produces it.
//
//  2. Reading & Publishing: A background process reads messages from the outbox
//     table and publishes them to the message broker. Once successfully published, messages
//     are removed from the outbox table.
//
// This package provides the following components to integrate this pattern:
//   - A `Writer` to facilitate the atomic storage of messages into the outbox table
//     alongside your application's domain changes within a single transaction.
//   - A `Reader` background process to poll the outbox table for unpublished messages,
//     attempt to publish them to a message broker, and remove them upon success.
//
// The library is designed to be agnostic to specific database or message broker technologies,
// allowing integration with various systems. For detailed setup, features, and examples,
// please refer to the project README.
package outbox

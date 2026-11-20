// Package integration tests kafkax against a Kafka cluster. By default the
// cluster is an in-memory one started inside the test; set
// KAKAROT_TEST_KAFKA to seed brokers (host:port, comma separated) to run the
// same tests against a real cluster. It is a module of its own so that the
// fake cluster and the admin client, which only the tests need, are not
// dependencies of projects that use kafkax.
package integration

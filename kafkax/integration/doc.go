// Package integration tests kafkax against an in-memory Kafka cluster
// started inside the test. It is a module of its own so that the fake
// cluster, which only the tests need, is not a dependency of projects that
// use kafkax.
package integration

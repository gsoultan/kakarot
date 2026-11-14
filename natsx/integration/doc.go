// Package integration tests natsx against a real NATS server started inside
// the test. It is a module of its own so that the server, which only the
// tests need, is not a dependency of projects that use natsx.
package integration

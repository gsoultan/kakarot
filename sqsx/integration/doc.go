// Package integration tests sqsx against a real SQS endpoint. The tests are
// skipped unless KAKAROT_TEST_SQS is set: to an endpoint URL such as
// http://localhost:4566 for LocalStack, or to "aws" for Amazon SQS with the
// credentials and region of the default AWS configuration (environment,
// ~/.aws, or an instance role). Every test creates its own queues and
// deletes them afterwards. It is a module of its own so that the AWS
// configuration loader, which only the tests need, is not a dependency of
// projects that use sqsx.
package integration

# Contributing
1. `make install && make test` must pass before and after your change.
2. New behaviour needs a test; claims in docs must match what tests/benchmarks show.
3. Keep the `Broker` and `StreamProcessor` interfaces stable: they are the extension points for Kafka/Spark adapters.

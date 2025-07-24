# Pebble

Pebble is a distributed, highly available, and fault-tolerant consumer coordination library for processing records from Amazon Kinesis Data Streams using Enhanced Fan-Out (EFO). It leverages etcd for distributed coordination, leader election, and persistent checkpointing, enabling scalable and resilient stream processing across multiple consumer nodes.

## Features

- **Distributed Shard Assignment:** Automatically balances Kinesis shards across multiple consumer workers, ensuring even load distribution and efficient failover.
- **Leader Election:** Uses etcd to elect a leader node responsible for managing shard assignments and system state.
- **Fault Tolerance:** Seamlessly reassigns shards from failed or inactive workers to healthy ones, minimizing downtime and data loss.
- **Checkpointing:** Persists processing progress (sequence numbers) in etcd, enabling consumers to resume from the last known position after a failure or restart.
- **Scalability:** Supports dynamic scaling of consumer nodes, with automatic rebalancing as nodes join or leave.
- **Pluggable Processing:** Allows users to define custom record processing logic for Kinesis records.

## Use Cases
- Building robust, horizontally scalable Kinesis consumer applications that require high availability and minimal operational overhead.
- Scenarios where multiple independent services or instances need to cooperatively process a Kinesis stream with strong coordination and failover.
- Applications needing reliable checkpointing and recovery in the face of node failures or restarts. 
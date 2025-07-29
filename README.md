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

## Usage

### Install

```
go get github.com/buddhike/pebble
```

### Standalone Consumer

This is the simplest way to use Pebble. Standalone consumer processes a given stream in standalone mode. Suitable when stream can be processed with the capacity available in a single host. Availability of the host can be managed using HA facilities available in compute (e.g. EC2 auto scaling groups, ECS services or Kubernetes services). Create a new standalone consumer by calling `MustNewStandaloneConsumer` function with a consumer name, stream name, EFO consumer ARN and the callback function to process each record.

```go
processFn := func(record types.Record) {
    fmt.Printf("Processing record: %s\n", *record.PartitionKey)
}

c := consumer.MustNewStandaloneConsumer("my-consumer", streamName, streamConsumerArn, processFn)

// Start the consumer
err := c.Start()
if err != nil {
    panic(err)
}

// Listen to SIGTERM and SIGINT and initiate graceful shutdown
notifyChan := make(chan os.Signal, 1)
signal.Notify(notifyChan, syscall.SIGTERM, syscall.SIGINT)
go func() {
    <-notifyChan
    c.Stop()
}()

// Wait for the consumer to finish
<-c.Done()
```

### Standard Consumer
Standard consumers are distributed, highly available, and fault tolrent. From API point of view, they are created with `NewConsumer` function by passing one additional argument called `popURLs`. Pebble Operator (a.k.a pop) is a component that assists consumers to run with their HA features.

Before deploying consumers, create an instance of pop using the following command.

```
pop create-instance --consumer-name <unique name for consumer> --stream-name <stream name>
```

This create the necessary infrastructure to run consumers and output the pop url that can be used with `NewConsumer` function.

```go
processFn := func(record types.Record) {
    fmt.Printf("Processing record: %s\n", *record.PartitionKey)
}

c := consumer.MustNewConsumer("my-consumer", streamName, streamConsumerArn, popURLs, processFn)

// Start the consumer
err := c.Start()
if err != nil {
    panic(err)
}

// Listen to SIGTERM and SIGINT and initiate graceful shutdown
notifyChan := make(chan os.Signal, 1)
signal.Notify(notifyChan, syscall.SIGTERM, syscall.SIGINT)
go func() {
    <-notifyChan
    c.Stop()
}()

// Wait for the consumer to finish
<-c.Done()
```

## Roadmap to v1.0.0
- Language bindings for Python, Javascript, Typescript, and C#
- Producer API 

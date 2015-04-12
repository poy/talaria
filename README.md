Talaria
=======

Think Kafka without Zookeeper (and in Go).

### Data Pipeline Architecture
Producer -> DataCombiner -> MessageLengthWriter -> FileWriter --> FileListeners (Not Implemented) -> Consumer (Not Implemented)
                                                              \-> ReplicatedFile (Not Implemented)

##### `Producer`
The producer is the entry point to the data pipeline.  There are different producer types, each having unique ways to submit data:

> #### Websockets (Not Implemented)
>> Websockets is the best fit for the REST API.  It only uses the write functionality (and therefore the server never writes anything to the websocket).

> #### TCP (Not Implemented)
>> This is the most optimal method, but is less RESTful.  The server simply reads from the TCP stream.

> #### Non-Network (Not Implemented)
>> This is the best method if Talaria is embedded and not networked.

##### `DataCombiner`
The `DataCombiner` takes data from multiple producers, that want to write to the same place, and combines it.  It is a fan-in.

##### `MessageLengthWriter`
The `MessageLengthWriter` appends the data length to each message.  This is necessary for the `Consumer` to properly read the data.

##### `FileWriter`
The `FileWriter` breaks up the data into blocks.  Each file does not exceed the set length, and records on closing how many writes it had.

##### `FileListeners`
The `FileListeners` writes to each of the listening `Consumers`.

##### `ReplicatedFile`
The `ReplicatedFile` replicates the files across the cluster. This protects the data against failures by copying to different locations.

##### `Consumer`
The `Consumer` reads from the data pipeline and returns the data to the client.  The consumer starts from a specific index, and reads data as it is available.  If the `Consumer` reaches the end of the file, it waits for additional data.  There are different `Consumer` types, see the `Producer` types.

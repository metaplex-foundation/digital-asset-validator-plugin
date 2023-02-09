## Digital Asset Validator Plugin

This repo houses a validator plugin that is a key part of the Metaplex's Digital Asset RPC API. It is responsible for getting
information out of the Solana validator and sending it to a message bus in a binary format. While this component was
built to serve the API's it was designed to allow any message bus tech to be used. That being sai, it can be used for many use cases.

## WARNING

```
Solana 1.10.41 or greater is required. Your calling components must support V2 GeyserPlugin types
```

It is built on the following principles.

- Do a little work in the validator process as possible.
- Allow any message bus tech to work.
- Opinionated and efficient Wire format as a standard.
- Async first

### Components

1. Plerkle -> Geyser Plugin that sends raw information to a message bus using Messenger
2. Messenger -> A message bus agnostic Messaging Library that sends Transaction, Account, Block and Slot updates in the Plerkle Serialization format.
3. Plerkle Serialization -> FlatBuffers based serialization code and schemas. This is the wire-format of Plerkle.

## Developing

If you are building the Metaplex RPC API infrastructure please follow the instructions in [Metaplex RPC API infrastructure](https://github.com/metaplex-foundation/digital-asset-rpc-infrastructure).

If you are using this plugin for your bespoke use case then the build steps are below.

### Building Locally

**NOTE -> M1 macs may have issues. Linux is best.**

`cargo build` for debug or
`cargo build --release` for a release build.

You will now have a libplerkle.so file in the target folder. This is the binary that you will pass into the validator using the following option.

```bash
--geyser-plugin-config plugin-config.json
```

The plugin config for plerkle must have this format, but you can put whatever keys you want

```json
EXAMPLE PLEASE DONT CONSIDER THIS THE PERFECT CONFIG
{
  "libpath": "/.../libplerkle.so",
  "enable_metrics": false,
  "env": "local",
  "handle_startup": true, // set to false if you dont want initial account flush
  "accounts_selector": {
    "owners": [
      "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
    ]
  },
  "transaction_selector": {
    "mentions": [
      "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY"
    ]
  }
}
```

This config file points to where your plugin library file is, and what programs it is listening to.
This is the standard Solana geyser plugin config file that the validator reads.

There are some other bits of configuration needed. Environment Variables.
The process running the validator must have access to environment variables. Those variables are as follows

```bash
RUST_LOG=warn
PLUGIN_MESSENGER_CONFIG='{ messenger_type="Redis", connection_config={ redis_connection_str="redis://redis" } }'
```

The PLUGIN_MESSENGER_CONFIG determins which compiled messenger to select and a specific configuration for the messenger.


#### Additional Configuration Examples

***Producer Configuration***

- "pipeline_size_bytes" - Maximum command size, roughly equates to the payload size. This setting locally buffers bytes in a queue to be flushed when the buffere grows past the desired amount. Default is 512mb(max redis command size) / 100, maximum is 512mb(max redis command size) / 100. You should test your optimal size to avoid high send latency and avoid RTT.
- "local_buffer_max_window" - Maximum time to wait for the buffer to fill be for flushing. For lower traffic you dont want to be waiting around so set a max window and it will send at a minumum of every X milliseconds . Default 1000
- "confirmation_level" - Can be one of "Processed", "Confirmed", "Rooted". Defaults to Processed this is the level we wait for before sending. "Processed" is essentially when we first see it which can on rare cases be reverted. "Confirmed" has extremley low likley hood of being reverted but takes longer (~1k ms in our testing) to show up. "Rooted" is impossible to revert but takes the longest.
- "num_workers" - This is the number of workers who will pickup notifications from the plugin and send them to the messenger. Default is 5


```
Lower Scale Low network latency 

PLUGIN_MESSENGER_CONFIG='{pipeline_size_bytes=1000000,local_buffer_max_window=10, messenger_type="Redis", connection_config={ redis_connection_str="redis://redis" } }'

High Scale Higher latency

PLUGIN_MESSENGER_CONFIG='{pipeline_size_bytes=50000000,local_buffer_max_window=500, messenger_type="Redis", connection_config={ redis_connection_str="redis://redis" } }'


```

***Consumer Configuration***

- "retries" - Amount of times to deliver the message. If delivered this many times and not acked, then its deleted
- "batch_size" - Max Amout of messages to grab within the wait timeout window.
- "message_wait_timeout" - Amount of time the consumer will keep the stream open and wait for messages 
- "idle_timeout" - Amount of time a consumer can have the message before it goes back on the queue
- "consumer_id" - VERY important. This is used to scaler horizontally so messages arent duplicated over instances.Make sure this is different per instance

```

PLUGIN_MESSENGER_CONFIG='{batch_size=1000,message_wait_timeout=5,retries=5, consumer_id="random_string",messenger_type="Redis", connection_config={ redis_connection_str="redis://redis" } }'

```


*** Hardcoded Configuration ***
We are still tuning some fo the default values for max stream size but this is what we have currently
```
msg.set_buffer_size(ACCOUNT_STREAM,100_000_000).await;
msg.set_buffer_size(SLOT_STREAM, 100_000).await;
msg.set_buffer_size(TRANSACTION_STREAM, 10_000_000).await;
msg.set_buffer_size(BLOCK_STREAM, 100_000).await;

```

NOTE: in 1.4.0 we are not sending to slot status.


### Metrics
The plugin exposes the following statsd metrics
- count plugin.startup -> times the plugin started
- time message_send_queue_time ->  time spent on messenger internal buffer
- time message_send_latency -> rtt time to messenger bus
- count account_seen_event , tags: owner , is_startup -> number of account events filtered and seen
- time startup.timer -> startup flush timer
- count transaction_seen_event tags slot-idx -> number of filtered txns seen

### Building With Docker

This repo contains a docker File that allows you to run an test the plerkle plugin using a test validator.
To test it you can build the container with`docker compose build` and run it with `docker compose up`.

You will want to change the programs you are listening to in `./docker/runs.sh`. Once you spin up the validator send your transactions to the docker host as you would a normal RPC.

Any program .so files you add to the /so/ file upon running the docker compose system will be added to the local validator.

You need to name the so file what you want the public key to be:

```bash
metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s.so
```

This is because of this line in the `docker-compose.yml` file.

```yaml
- ./programs:/so/:ro
```

You can comment this out if you dont want it.

### Using docker output for solana and geyser artifacts

You can run `./docker/build.sh` to create a build of the geyser plugin and solana, then output the artifacts
to your local filesystem so you can run the validator.

Note that differences in image distro could cause incompatible GLibc versions.

### Crates

NOTE WE DO NOT PUBLISH THE PLUGIN ANY MORE:

plerkle_messenger-https://crates.io/crates/plerkle_messenger
plerkle_serialization-https://crates.io/crates/plerkle_serialization


## Digital Asset Validator Plugin
This repo houses a validator plugin that is a key part of the Metaplex's Digital Asset RPC API. It is responsible for getting 
information out of the Solana validator and sending it to a message bus in a binary format. While this component was 
built to serve the API's it was designed to allow any message bus tech to be used. That being sai, it can be used for many use cases.

## WARNING
```
Solana 1.10.41 or greater is required. Your calling components must support V2 GeyserPlugin types
```

It is built on the following principles.
* Do a little work in the validator process as possible.
* Allow any message bus tech to work.
* Opinionated and efficient Wire format as a standard.
* Async first

### Components
1. Plerkle -> Geyser Plugin that sends raw information to a message bus using Messenger
2. Messenger -> A message bus agnostic Messaging Library that sends Transaction, Account, Block and Slot updates in the Plerkle Serialization format.
3. Plerkle Serialization -> FlatBuffers based serialization code and schemas. This is the wire-format of Plerkle.


## Developing
If you are building the Metaplex RPC API infrastructure please follow the instructions in [Metaplex RPC API infrastructure](https://github.com/metaplex-foundation/digital-asset-rpc-infrastructure).

If you are using this plugin for your bespoke use case then the build steps are below.

### Building Locally
**NOTE -> M1 macs may have issues. Linux is best.**

``cargo build`` for debug or
``cargo build --release`` for a release build. 

You will now have a libplerkle.so file in the target folder. This is the binary that you will pass into the validator using the following option.

```bash
--geyser-plugin-config plugin-config.json
```

The plugin config for plerkle must have this format

```json
{
    "libpath": "/.../libplerkle.so",
    "enable_metrics": false,
    "env": "local",
    "handle_startup": true, // set to false if you dont want initial account flush
    "accounts_selector" : {
        "owners" : [
            "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
            "GRoLLMza82AiYN7W9S9KCCtCyyPRAQP2ifBy4v4D5RMD",
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
            "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY"
        ]
    },
    "transaction_selector" : {
        "mentions" : [
            "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
            "GRoLLMza82AiYN7W9S9KCCtCyyPRAQP2ifBy4v4D5RMD",
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
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
PLUGIN_CONFIG_RELOAD_TTL=300  
PLUGIN_MESSENGER_CONFIG='{ messenger_type="Redis", connection_config={ redis_connection_str="redis://redis" } }'
```
The PLUGIN_CONFIG_TTL_RELOAD tells the plugin how long to keep the geyser plugin file cached in seconds. This allows hot reloading of what programs you are listening to without restarting the validator.
The PLUGIN_MESSENGER_CONFIG determins which compiled messenger to select and a specific configuration for the messenger.
 
### Building With Docker
This repo contains a docker File that allows you to run an test the plerkle plugin using a test validator.
To test it you can build the container with```docker compose build .``` and run it with ```docker compose up```. 

You will want to change the programs you are listening to in `./docker/runs.sh`. Once you spin up the validator send your transactions to the docker host as you would a normal RPC.

Any program .so files you add to the /so/ file upon running the docker compose system will be added to the local validator.

You need to name the so file what you want the public key to be:
```bash
metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s.so
```
This is because of this line in the ``docker-compose.yml`` file. 
```yaml
      - ./programs:/so/:ro
```

You can comment this out if you dont want it.

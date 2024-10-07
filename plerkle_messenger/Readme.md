# Messenger

A message bus agnostic Messaging Library that sends Transaction, Account, Block and Slot updates in the Plerkle Serialization format.

## Note on 1.0.0

The plerkle serialization API changes at 1.0.0 which is a breaking change. 
This method removes confusion around the Recv data lifetime being tied back to the messenger interface. Now the data is owned.

# Env example

The Messenger can operate in two modes: a single Redis instance or multiple Redis instances.

Just to clarify, the multiple Redis instances setup doesn't create a clustered connection. It's designed to work with separate, independent instances.

You can configure the Redis client type via environment variables.

Example environment configuration for a single Redis instance:

```
export PLUGIN_MESSENGER_CONFIG='{
  messenger_type="Redis",
  redis_connection_str="redis://:pass@redis.app:6379"
}'
```

Example environment configuration for multiple Redis instances:

```
export PLUGIN_MESSENGER_CONFIG='{
  messenger_type="RedisPool",
  redis_connection_str=[
    "redis://:pass@redis1.app:6379",
    "redis://:pass@redis2.app:6379"
  ]
}'
```

To switch between modes, you'll need to update both the `messenger_type` and `redis_connection_str` values.
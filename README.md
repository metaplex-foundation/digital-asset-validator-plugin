### This repo was originally built inside https://github.com/jarry-xiao/candyland as a join effort between Solana X Metaplex
This repo is in transition, and we are factoring out components from CandyLand here.

## Digital Asset Validator Plugin
This repo houses a validator plugin that is a key part of the Metaplex's Digital Asset RPC API. It is responsible for getting 
information out of the Solana validator and sending it to a message bus in a binary format. While this component was 
built to serve the API's it was designed to allow any message bus tech to be used. That being sai, it can be used for many use cases.

It is built on the following principles.
* Do a little work in the validator process as possible.
* Allow any message bus tech to work.
* Opinionated and efficient Wire format as a standard.
* Async first

### Components
1. Plerkle -> Geyser Plugin that sends raw information to a message bus using Messenger
2. Messenger -> A message bus agnostic Messaging Library that sends Transaction, Account, Block and Slot updates in the Plerkle Serialization format.
2. Plerkle Serialization -> FlatBuffers based serialization code and schemas. This is the wire-format of Plerkle.



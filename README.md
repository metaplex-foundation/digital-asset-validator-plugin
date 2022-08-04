# This repo was originally built inside https://github.com/jarry-xiao/candyland as a join effort between Solana X Metaplex
This repo is in transition and we are factoring out components from candly land here.


## Digital Asset RPC Infrastructure

### Components
1. Plerkle -> Geyser Plugin that sends raw information to a message bus
2. Ingester-> Reads from the message bus and parses transaction, account and other raw information
3. Api -> Serves Assets , Merkle Proofs and Some Rudimentary Sales Statistics

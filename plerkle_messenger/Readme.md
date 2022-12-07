# Messenger

A message bus agnostic Messaging Library that sends Transaction, Account, Block and Slot updates in the Plerkle Serialization format.

## Note on 1.0.0

The plerkle serialization API changes at 1.0.0 which is a breaking change. 
This method removes confusion around the Recv data lifetime being tied back to the messenger interface. Now the data is owned.

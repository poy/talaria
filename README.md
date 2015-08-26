Talaria
=======

Talaria is a message queue that uses files (Yes files... Get over it). You can only write to the end of a file, but you can read from anywhere (as long as the segment hasn't been deleted). This implies that while each write still has an ack, the read doesn't. So ideally it's fast... Also, if your reader croaks, the one that picks up the load can pick up where the otherone left off or a little before or whatever.

###Files?
Yep... File IO isn't as bad as you might think... In fact it's definately better than the network IO. Also, if you feel like writing it to a persistent disk, then you can make deployments a little simpler. You can also use a SSD and help throughput.

###Distributed?
Also yep... Each file has a leader and replicas (or once that is implemented at least...). So your client will connect to all the brokers and then decide who to write to based on what file you write to and read from. When a leader goes down, then the first replica picks up the role and the clients will talk to it. So a client only talks to the leader of a file. A strong use of Talaria is to write to many files and distribute the load across them. 

###Use Case?
Data! The idea behind Talaria is to write and read lots of data.

###Contribute?
Do it! Pull requests encouraged!

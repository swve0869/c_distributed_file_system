# c_distributed_file_system
A simple distributed files system for linux systems  
Implementation consists of a client process that runs a single command. put, ls, or get.
The client is aware of the ip addresses and ports of the file servers through a config file.

When putting a file to the DFS the file is hashed and a chunking schema is selected.
For example a file may be split into n chunks (where n is the number of servers) and then copies of the chunks will be sent to the servers.
(chunk1,chunk2 to server1) (chunk2,chunk3 to server2) (chunk3,chunk4 to server3) etc ...
This storage style is to provide redundancy. If server 2 goes does chunk2 still exists on server1 and chunk3 still exists on server3

When a get command is given the client requests the servers for their files and checks if all the needed chunks are present for the file to be reconstructed. 

An ls command will only show files as available if the needed chunks exist on the system. 

If a file with the same name as an already existing file is put then the dfs will respect the most recent write rule and will ignore the previous version of the file.

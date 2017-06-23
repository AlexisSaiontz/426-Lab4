# Assignment 4: Partitioning the Graph Store #
#Alex Saiontz and Stelios Rousoglou#

----

## Introduction ##

In this lab, we'll add the partition to the graph store. Nodes will be stored on different partitions based on a simple partitioning function (for this lab, modulo is sufficient). For example, nodes 0, 3, 6... will be on partition 1; nodes 1, 4, 7... will be on partition 2; and nodes 2, 5, 8... will be on partition 3.

When an accessor or mutator command comes in, you'll have to first lock all the partitions involved; modify them via RPC; and then unlock them.

## Changelist ##

## API Changes ##

The service retains the API described in Lab1, except that you don't need to support the shortest path and remove node APIs. You do have to support add_node, add_edge, get_node, get_edge, remove_edge, and get_neighbors.

In the command line, accept an additional parameter specifying the partition number, and a list of all the partitions in order:

```sh
$ ./cs426_graph_server <graph_server_port> -p <partnum> -l <partlist> 
```

Where each entry in partlist is of the form IP:rpc_server_port. For example, if there are three partitions in the system, for machine with IP 10.0.0.1, who servers as the first partion, the command is as follows:
```sh
$ ./cs426_graph_server 8000 -p 1 -l 10.0.0.1:1111 10.0.0.2:2222 10.0.0.3:1111  
```

## Testing Methodology ##
Submit the code to the same repository as previous labs. Commit your changes, label your commit lab4 with git tag lab4 and perform a git push && git push --tags.

Provide a make file. Your code should run with the following command:

make && ./cs426_graph_server <graph_server_port> -p <partnum> -l <partlist> 

This time we will test your code on our server instead of make you run the server on your own VMs. To test the system, we'll create three partitions on different servers. And we will then run three instances of a tester simultaneously, each one against a different partition. Partitions will only get requests meant for their own partitions. Multi-partition updates (add edge, remove edge) will be sent to the lower-numbered partition. The tester is a script of curl commands.

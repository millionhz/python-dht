# Distributed Hash Table

## Objectives
- Learn Distributed Hash Tables
- Implement networking using sockets
- Learn synchronous and asynchronous

## Explanation

- `DHT.py` implements a node in a distributed hash table capable of storing, retrieving and transferring key-value pairs. 
- In this specific implementation the keys are filenames and values are the respective file content. 
- The hash table looks up and retrieves files iteratively, which is a faster retrieval method compared to recursive. 
- The hash table also has the capability to seamlessly backup and restore files on node failures.
- File backup is currently not fully functional. Requires implementing separate backup subfolder.
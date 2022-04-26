# cs6210Project4
MapReduce Infrastructure

## Project Instructions

[Project Description](description.md)

[Code walk through](structure.md)

### How to setup the project  
Same as project 3 instructions

# Implementation notes

This implementation uses a synchronous gRPC server/client relationship.  Workers operate in a state machine and use synchronous calls from the master to change the state.  The worker primarily is implemented within the SyncWorker class.  The SyncWorker operates as the gRPC server.  The master monitors the state of the worker via the Ping gRPC, which the worker writes its current state machine state in the response field of the Ack.  Once the master detects the state change on the SyncWorker, it updates continues to the next step of the state machine.

The available worker states during the mapping phase are:
    - STATUS_CODE_IDLE
    - STATUS_CODE_MAP_WORKING
    - STATUS_CODE_FAILED
    - STATUS_CODE_COMPLETE
    - STATUS_CODE_MISSING
    - STATUS_CODE_WRITING_MAP
    - STATUS_CODE_MAP_WRITE_COMPLETE

Each intermediate writes to n_output_files intermediate files.  When the master finds a node is in the IDLE or COMPLETE state, it attempts to find a new shard to give to the worker.  If no shards are available, it waits on all workers to be completed and then instructs all workers to write their mappings to an intermediate file.  The keys are written to one of the n_output_files based on the hash of their keys mod n_output_files.


The available worker states during the reduction phase are:
    - STATUS_CODE_IDLE
    - STATUS_CODE_REDUCE_WORKING
    - STATUS_CODE_FAILED
    - STATUS_CODE_COMPLETE
    - STATUS_CODE_MISSING
    - STATUS_CODE_WRITING_REDUCE
    - STATUS_CODE_REDUCE_WRITE_COMPLETE

Once the shards have all been mapped, the master then begins assigning reduce tasks to each worker.  Each reduce task takes the i-th intermediate file from each worker and reduces to a single file.

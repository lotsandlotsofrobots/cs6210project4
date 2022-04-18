#pragma once

class WorkerStatus;

#define REDUCE_STATE_UNASSIGNED 1
#define REDUCE_STATE_ASSIGNED   2
#define REDUCE_STATE_COMPLETE   3

typedef struct ReduceTask
{
    // break the intermediate shard files up into "columns"
    // and run a reduce on each of them
    // ie for 8 workers producing 8 files each,
    // column 2 means the second file from each worker
    // this ensures the hashed values are together
    int reduceID;
    int state;

    WorkerStatus * assignedWorker;
    std::vector<int>  workersThatAttemptedThis;
} ReduceTask;

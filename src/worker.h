#pragma once

#include <thread>

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "file_shard.h"

#include "masterworker.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include "SyncWorker.h"

// grpc namespace imports
using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;

using masterworker::MapperReducer;


/******************************************************************************
**
**  Worker CLASS DECLARATION
**
******************************************************************************/

// user first byte for overall worker state
#define STATUS_CODE_IDLE                        0x00000000
#define STATUS_CODE_WORKING                     0x00000001
#define STATUS_CODE_COMPLETE                    0x00000002
#define STATUS_CODE_FAILED                      0x00000004
#define STATUS_CODE_MISSING                     0x00000008  // only the master sets this, a worker doesn't know it's missing
#define STATUS_CODE_WRITING_MAP                 0x00000010
#define STATUS_CODE_MAP_WRITE_COMPLETE          0x00000020
#define STATUS_CODE_MAP_DUMP_RESULTS            0x00000040
#define STATUS_CODE_MAP_DUMP_RESULTS_COMPLETE   0x00000080

// next three bytes for information
#define STATUS_CODE_INVALID_ARGS       0x00000100
#define STATUS_CODE_FILE_ERROR         0x00000200
#define STATUS_CODE_SHARD_MATH_ERROR   0x00000400



/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		std::string ipAndPort;

};


extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

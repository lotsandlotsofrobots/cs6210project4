#pragma once

#include <thread>

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "file_shard.h"

#include "MapperShardCallData.h"
#include "PingCallData.h"
#include "SetWorkerInfoCallData.h"
#include "DiscardShardResultsCallData.h"
#include "WriteShardToIntermediateFileCallData.h"

#include "masterworker.grpc.pb.h"
#include <grpcpp/grpcpp.h>

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


/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		void SetupBaseMapperImpl(std::string outputFile);

		void SetWorkerID(int i)                { mapper->impl_->SetWorkerID(i);         reducer->impl_->SetWorkerID(i);  workerID = i; }
		int  GetWorkerID()									   { return workerID; }
    void SetOutputDirectory(std::string s) { mapper->impl_->SetOutputDirectory(s);  reducer->impl_->SetOutputDirectory(s); }
    void SetNumberOfWorkers(int i)         { mapper->impl_->SetNumberOfWorkers(i);  reducer->impl_->SetNumberOfWorkers(i); }
    void SetNumberOfFiles(int i)           { mapper->impl_->SetNumberOfFiles(i);    reducer->impl_->SetNumberOfFiles(i); }
		void SetReduceSubset(int i)            {                                        reducer->impl_->SetReduceSubset(i); }
		void SetupMapper()                     { mapper->impl_->Setup(); }
		void SetupReducer()                    { reducer->impl_->Setup(); }

		void Map(std::string s);
		void WriteShardToIntermediateFile()    { mapper->impl_->WriteShardToIntermediateFile(); }
		void DiscardShardResults()             { mapper->impl_->DiscardShardResults(); }

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		std::string ipAndPort;
		int workerID;

		std::unique_ptr<grpc::ServerCompletionQueue>  completionQueue;
		masterworker::MapperReducer::AsyncService     asyncService;
		std::unique_ptr<grpc::Server>                 server;

		ServerBuilder builder;

		std::shared_ptr<BaseMapper> mapper;
		std::shared_ptr<BaseReducer> reducer;
};


extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

#pragma once

#include "file_shard.h"

#include "masterworker.grpc.pb.h"
#include <grpcpp/grpcpp.h>

class Worker;

// no, we shouldn't use this in a header, but we'll live.
using masterworker::MapperReducer;
using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;

enum CallStatus { CREATE, PROCESS, FINISH };

class CallDataBase
{
public:
	  CallDataBase() {}
	  virtual void Proceed() {}

		void Map(std::string s);
    void Reduce();

		int GetWorkerID();

protected:
		MapperReducer::AsyncService* service_;
		ServerCompletionQueue* cq_;
		ServerContext ctx_;

		CallStatus status_;  // The current serving state.
		Worker * worker;
};

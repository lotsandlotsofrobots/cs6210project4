#pragma once

#include "masterworker.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <thread>

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "file_shard.h"
#include "worker.h"

using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using masterworker::MapperReducer;
using masterworker::EmptyMsg;
using masterworker::Ack;
using masterworker::WorkerInfo;
using masterworker::ShardInfo;

class SyncWorker final : public MapperReducer::Service {

public:
    SyncWorker(std::string ip_addr_port,
               std::shared_ptr<BaseMapper> mapper,
               BaseMapperInternal* mapperImpl,
               std::shared_ptr<BaseReducer> reducer,
               BaseReducerInternal* reducerImpl
             );

    virtual ~SyncWorker() {}

// RPCS:
    Status Ping( ServerContext* context, const EmptyMsg* request, Ack* reply ) override;
    Status SetWorkerInfo( ServerContext * context, const WorkerInfo* request, Ack * reply) override;
    Status MapShard( ServerContext * context, const ShardInfo* request, Ack * reply) override;
    Status WriteShardToIntermediateFile( ServerContext * context, const EmptyMsg* request, Ack * reply) override;
    Status DiscardShardResults( ServerContext * context, const EmptyMsg* request, Ack * reply) override;

// Auxilary:
    void EnqueShard(FileShard fileShard);
    void SetupMapper() { mapperImpl->Setup(); }
    //void Map(std::string s);
    void DoShardMapping();
    BaseMapperInternal* GetMapperImpl() { return mapperImpl; }

    void run();

    void SetStatusCode(int i)              { statusCode = i; }
		int GetStatusCode()                    { return statusCode; }

    void SetWorkerID(int i)                { workerID = i; mapperImpl->SetWorkerID(i);         reducerImpl->SetWorkerID(i); }
    int  GetWorkerID()                     { return workerID; }
    void SetOutputDirectory(std::string s) { mapperImpl->SetOutputDirectory(s);  reducerImpl->SetOutputDirectory(s); }
    void SetNumberOfWorkers(int i)         { mapperImpl->SetNumberOfWorkers(i);  reducerImpl->SetNumberOfWorkers(i); }
    void SetNumberOfFiles(int i)           { mapperImpl->SetNumberOfFiles(i);    reducerImpl->SetNumberOfFiles(i); }

protected:
    std::thread t;
    FileShard fileShardArg;
    int statusCode;
    int workerID;

    std::string ipAndPort;
    BaseMapperInternal* mapperImpl;
    BaseReducerInternal* reducerImpl;
    std::shared_ptr<BaseMapper> mapper;
    std::shared_ptr<BaseReducer> reducer;
};

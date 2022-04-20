#pragma once

#include "masterworker.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <string>
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
using masterworker::ReduceSubset;

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
    Status Reduce(ServerContext * context, const ReduceSubset* request, Ack * reply);

    Status WriteReduceFile(ServerContext * context, const EmptyMsg* request, Ack * reply);
    Status DiscardReduceResults(ServerContext * context, const EmptyMsg* request, Ack * reply);

    Status Finish(ServerContext * context, const EmptyMsg* request, Ack * reply);

// Auxilary:
    //void SetupMapper() { mapperImpl->Setup(); }
    //void SetupMapper() { mapperImpl->Setup(); }

    void EnqueShard(FileShard fileShard);
    void DoShardMapping();
    void DoReducing();

    BaseMapperInternal* GetMapperImpl() { return mapperImpl; }
    BaseReducerInternal* GetReducerImpl() { return reducerImpl; }



    void run();

    void SetStatusCode(int i)              { statusCode = i; }
		int GetStatusCode()                    { return statusCode; }

    void SetWorkerID(int i)                { mapperImpl->SetWorkerID(i);         reducerImpl->SetWorkerID(i);        workerID = i; }
    int  GetWorkerID()                     { return workerID; }

    int GetReduceSubset()                  { return reduceSubset; }

    void SetOutputDirectory(std::string s) { mapperImpl->SetOutputDirectory(s);  reducerImpl->SetOutputDirectory(s); outputDirectory = s; }
    void SetNumberOfWorkers(int i)         { mapperImpl->SetNumberOfWorkers(i);  reducerImpl->SetNumberOfWorkers(i); numberOfWorkers = i;}
    void SetNumberOfFiles(int i)           { mapperImpl->SetNumberOfFiles(i);    reducerImpl->SetNumberOfFiles(i); }

    void SetdesiredShardSize(int i) { desiredShardSize = 1;  reducerImpl->SetdesiredShardSize(i); }
    void SetnumberOfShardsTotal(int i) { numberOfShardsTotal = 1;  reducerImpl->SetnumberOfShardsTotal(i); }
    void SetInputFiles(std::string s) { inputFiles = s; reducerImpl->SetInputFiles(s); }

    int GetdesiredShardSize() { return desiredShardSize; }
    int GetnumberOfShardsTotal() { return numberOfShardsTotal; }




protected:
    std::thread t;
    FileShard fileShardArg;
    int reduceSubset;
    int statusCode;
    int workerID;
    int numberOfWorkers;
    std::string outputDirectory;

    int desiredShardSize;
    int numberOfShardsTotal;
    std::string inputFiles;

    std::string ipAndPort;
    BaseMapperInternal* mapperImpl;
    BaseReducerInternal* reducerImpl;
    std::shared_ptr<BaseMapper> mapper;
    std::shared_ptr<BaseReducer> reducer;
};

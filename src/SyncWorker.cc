#include "SyncWorker.h"

void MonitorAndDoWork(SyncWorker * worker)
{
    std::cout << "Worker " << std::to_string(worker->GetWorkerID()) << " - Monitoring and doing work!\n";

    while(true)
    {
        switch(worker->GetStatusCode())
        {
            // these are almost the only cases where we do something,
            // the rest are just waiting on outside intervention
            case STATUS_CODE_WORKING:
                worker->DoShardMapping();
                break;
            case STATUS_CODE_WRITING_MAP:
                worker->GetMapperImpl()->WriteShardToIntermediateFile();
                worker->SetStatusCode(STATUS_CODE_MAP_WRITE_COMPLETE);
                break;
            case STATUS_CODE_MAP_DUMP_RESULTS:
                worker->GetMapperImpl()->DiscardShardResults();
                worker->SetStatusCode(STATUS_CODE_MAP_DUMP_RESULTS_COMPLETE);
                break;
            case STATUS_CODE_IDLE:
            case STATUS_CODE_COMPLETE:
            case STATUS_CODE_FAILED:
            case STATUS_CODE_MISSING:
            case STATUS_CODE_MAP_WRITE_COMPLETE:
                break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

}



SyncWorker::SyncWorker(std::string ip_addr_port,
                       std::shared_ptr<BaseMapper> mapper,
                       BaseMapperInternal* mapperImpl,
                       std::shared_ptr<BaseReducer> reducer,
                       BaseReducerInternal* reducerImpl)
{
    this->ipAndPort = ip_addr_port;
    this->mapper = mapper;
    this->reducer = reducer;
    this->mapperImpl = mapperImpl;
    this->reducerImpl = reducerImpl;
    statusCode = STATUS_CODE_IDLE;
}


void SyncWorker::run()
{

}


void SyncWorker::DoShardMapping()
{
    std::cout << "ShardID : " << std::to_string(fileShardArg.shardID) << "\n\n";

    std::ifstream shard(fileShardArg.fileName);

    shard.seekg(0, shard.end);
    int fileLength = shard.tellg();

    if (fileShardArg.offset + fileShardArg.shardSize > fileLength)
    {
        SetStatusCode(STATUS_CODE_FAILED | STATUS_CODE_INVALID_ARGS);
        return;
    }

    shard.seekg(fileShardArg.offset);
    int position = shard.tellg();

    std::string record;
    int bytes = position;
    int lines = 0;

    while(getline(shard, record) && (bytes - fileShardArg.offset) < fileShardArg.shardSize)
    {
        bytes += record.length() + 1; // remember to add the newline character to the length, even though we don't get it
        lines++;

        mapper->map(record);
    }

    bytes -= 1; // scoot back one so we're AT the newline, not over it

    if (bytes != fileShardArg.offset + fileShardArg.shardSize)
    {
        std::cout << "Something went wrong here..\n";
        std::cout << "ShardID : " << std::to_string(fileShardArg.shardID) << "\n";
        std::cout << "WorkerID: " << std::to_string(workerID) << "\n";
        std::cout << "Filename: " << fileShardArg.fileName << "\n";
        std::cout << "Offset:   " << std::to_string(fileShardArg.offset) << "\n";
        std::cout << "Size:     " << std::to_string(fileShardArg.shardSize) << "\n";
        std::cout << "Position: " << std::to_string(bytes) << "\n";

        SetStatusCode(STATUS_CODE_FAILED | STATUS_CODE_SHARD_MATH_ERROR);

        // todo figure out what to do with this
    }
    else
    {
        std::cout << "Done with file shard " << std::to_string(fileShardArg.shardID) << "\n";
    }

    shard.close();

    SetStatusCode(STATUS_CODE_COMPLETE);
}



Status SyncWorker::Ping( ServerContext* context, const EmptyMsg* request, Ack* reply )
{
    reply->set_response(GetStatusCode());
    return Status::OK;
}


Status SyncWorker::SetWorkerInfo( ServerContext * context, const WorkerInfo* request, Ack * reply)
{
    SetWorkerID(request->workerid());
    SetOutputDirectory(request->outputdirectory());
    SetNumberOfWorkers(request->numberofworkers());
    SetNumberOfFiles(request->numberoffiles());
    SetupMapper();

    t = std::thread(MonitorAndDoWork, this);

    reply->set_response(1);

    return Status::OK;
}

Status SyncWorker::MapShard( ServerContext * context, const ShardInfo* request, Ack * reply)
{
    fileShardArg.fileName = request->filename();
    fileShardArg.offset = request->offset();
    fileShardArg.shardSize = request->shardsize();
    fileShardArg.shardID = request->shardid();

    //EnqueShard(fileShard);
    SetStatusCode( STATUS_CODE_WORKING );

    reply->set_response(1);

    return Status::OK;
}



Status SyncWorker::WriteShardToIntermediateFile( ServerContext * context, const EmptyMsg* request, Ack * reply)
{
    SetStatusCode( STATUS_CODE_WRITING_MAP );

    // let thread pick up the flag and write everything

    reply->set_response(1);
    return Status::OK;
}




Status SyncWorker::DiscardShardResults( ServerContext * context, const EmptyMsg* request, Ack * reply)
{
    SetStatusCode(STATUS_CODE_MAP_DUMP_RESULTS);

    reply->set_response(1);
    return Status::OK;
}




//

#include "MapperShardCallData.h"
#include "worker.h"

#include <string>

void MapDataShard(MapperShardCallData * callData)
{
		auto mapper = get_mapper_from_task_factory("cs6210");

		FileShard fileShardArg = callData->GetFileShard();

		std::cout << "ShardID : " << std::to_string(fileShardArg.shardID) << "\n\n";

		std::ifstream shard(fileShardArg.fileName);

		shard.seekg(0, shard.end);
		int fileLength = shard.tellg();


		if (fileShardArg.offset + fileShardArg.shardSize > fileLength)
		{
				callData->SetStatusCode(STATUS_CODE_FAILED | STATUS_CODE_INVALID_ARGS);
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


			  callData->Map(record);
		}

		bytes -= 1; // scoot back one so we're AT the newline, not over it

		if (bytes != fileShardArg.offset + fileShardArg.shardSize)
		{
				std::cout << "Something went wrong here..\n";
				std::cout << "ShardID : " << std::to_string(fileShardArg.shardID) << "\n";
				std::cout << "WorkerID: " << std::to_string(callData->GetWorkerID()) << "\n";
				std::cout << "Filename: " << fileShardArg.fileName << "\n";
				std::cout << "Offset:   " << std::to_string(fileShardArg.offset) << "\n";
				std::cout << "Size:     " << std::to_string(fileShardArg.shardSize) << "\n";
				std::cout << "Position: " << std::to_string(bytes) << "\n";

				callData->SetStatusCode(STATUS_CODE_FAILED | STATUS_CODE_SHARD_MATH_ERROR);

		    // todo figure out what to do with this
		}
		else
		{
		}

		shard.close();

		callData->SetStatusCode(STATUS_CODE_COMPLETE);
}



/******************************************************************************
**
**  MapperShardCallData implementation
**
******************************************************************************/



MapperShardCallData::MapperShardCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * w)
		: responder_(&ctx_)
{
		 service_ = service;
		 cq_ = cq;
		 status_  = CREATE;
		 worker = w;
		 statusCode = grpc::StatusCode::OK;

		 // Invoke the serving logic right away.
		Proceed();
}

void MapperShardCallData::StartWorkerMapThread()
{
		t = std::thread(MapDataShard, this);
}

FileShard MapperShardCallData::GetFileShard()
{
		FileShard fileShard;
		fileShard.fileName = request_.filename();
		fileShard.offset = request_.offset();
		fileShard.shardSize = request_.shardsize();
		fileShard.shardID = request_.shardid();

		return fileShard;
}

void MapperShardCallData::Proceed()
{
		if (status_ == CREATE)
		{
				status_ = PROCESS;

				// As part of the initial CREATE state, we *request* that the system
				// start processing SayHello requests. In this request, "this" acts are
				// the tag uniquely identifying the request (so that different CallData
				// instances can serve different requests concurrently), in this case
				// the memory address of this CallData instance.

				service_->RequestMapShard(&ctx_, &request_, &responder_, cq_, cq_, this);
		}
		else if (status_ == PROCESS)
		{
				// Spawn a new CallData instance to serve new clients while we process
				// the one for this CallData. The instance will deallocate itself as
				// part of its FINISH state.
				worker->SetStatusCode( STATUS_CODE_WORKING );
				new MapperShardCallData(service_, cq_, worker);
				StartWorkerMapThread();

				status_ = FINISH;

				responder_.Finish(reply_, grpc::Status::OK, this);
		}
		else
		{
				GPR_ASSERT(status_ == FINISH);
				// Once in the FINISH state, deallocate ourselves (CallData).

				t.join();
				delete this;
		}
}

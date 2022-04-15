#include "MapperShardCallData.h"
#include "worker.h"

#include <string>

void MapDataShard(MapperShardCallData * callData)
{
		auto mapper = get_mapper_from_task_factory("cs6210");

		FileShard fileShardArg = callData->GetFileShard();

		// First open the file(s)
		std::cout << "ShardID : " << std::to_string(fileShardArg.shardID) << "\n\n";

		std::ifstream shard(fileShardArg.fileName);

		shard.seekg(0, shard.end);
		int fileLength = shard.tellg();

		std::cout << "FileLength: " << std::to_string(fileLength) << "\n";

		if (fileShardArg.offset + fileShardArg.shardSize > fileLength)
		{
				std::cout << "offset + size > length!\n";
			  // TODO handle error here.
		}

		shard.seekg(fileShardArg.offset);
		int position = shard.tellg();
		std::cout << "Position after seek: " << std::to_string(position) << "\n";

		std::string record;
		int bytes = position;
		int lines = 0;

		while(getline(shard, record) && (bytes - fileShardArg.offset) < fileShardArg.shardSize)
		//while(getline(shard, record) && (position - fileShardArg.offset) < fileShardArg.shardSize)
		{
			  bytes += record.length() + 1; // remember to add the newline character to the length, even though we don't get it
				lines++;

				std::cout << "  Current: " << bytes << "\n";

			  callData->Map(record);
				//position = shard.tellg();
		}

		bytes -= 1; // scoot back one so we're AT the newline, not over it

		std::cout << "Read " << std::to_string(lines) << " lines.\n";
		//position = shard.tellg();

		//if (position != fileShardArg.offset + fileShardArg.shardSize)
		if (bytes != fileShardArg.offset + fileShardArg.shardSize)
		{
				std::cout << "Something went wrong here..\n";
				std::cout << "ShardID : " << std::to_string(fileShardArg.shardID) << "\n";
				std::cout << "WorkerID: " << std::to_string(callData->GetWorkerID()) << "\n";
				std::cout << "Filename: " << fileShardArg.fileName << "\n";
				std::cout << "Offset:   " << std::to_string(fileShardArg.offset) << "\n";
				std::cout << "Size:     " << std::to_string(fileShardArg.shardSize) << "\n";
				std::cout << "Position: " << std::to_string(bytes) << "\n";

		    // todo figure out what to do with this
		}
		else
		{
			  std::cout << "Done mapping!\n";
		}

		shard.close();

		callData->Finish();
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

				std::cout << "Requesting mapshard\n";
				service_->RequestMapShard(&ctx_, &request_, &responder_, cq_, cq_, this);
		}
		else if (status_ == PROCESS)
		{
				// Spawn a new CallData instance to serve new clients while we process
				// the one for this CallData. The instance will deallocate itself as
				// part of its FINISH state.
				std::cout << "Got it, now creating a new mapshardcalldata and startworkmapthread\n\n";
				new MapperShardCallData(service_, cq_, worker);
				StartWorkerMapThread();
		}
		else
		{
				GPR_ASSERT(status_ == FINISH);
				// Once in the FINISH state, deallocate ourselves (CallData).

				t.join();

				std::cout << "t was joined!\n";

				delete this;
		}
}

void MapperShardCallData::Finish()
{
		status_ = FINISH;
		
		responder_.Finish(reply_, grpc::Status::OK, this);
}

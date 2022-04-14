#pragma once

#include <thread>

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "file_shard.h"

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

		void Map(std::string s);
		void Done();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		std::string ipAndPort;

		std::unique_ptr<grpc::ServerCompletionQueue>  completionQueue;
		masterworker::MapperReducer::AsyncService     asyncService;
		std::unique_ptr<grpc::Server>                 server;

		ServerBuilder builder;

		std::shared_ptr<BaseMapper> mapper;
};






/******************************************************************************
**
**  CALL DATA DECLARATIONS
**
******************************************************************************/



enum CallStatus { CREATE, PROCESS, FINISH };

// Idea for multiple CallData handling here:  https://stackoverflow.com/questions/41732884/grpc-multiple-services-in-cpp-async-server

// Forward Declarations
class MapperCallData;
class MapperShardCallData;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);
void MapDataShard(MapperShardCallData * callData);


class MapperCallData
{
public:
	  MapperCallData() {}
	  virtual void Proceed() {}

		void Map(std::string s);

protected:
		MapperReducer::AsyncService* service_;
		ServerCompletionQueue* cq_;
		ServerContext ctx_;

		CallStatus status_;  // The current serving state.
		Worker * worker;
};





class MapperShardCallData : public MapperCallData {
public:
		MapperShardCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * worker);
		void StartWorkerMapThread();
		FileShard GetFileShard();
		virtual void Proceed();
    void Finish();

	private:
			masterworker::ShardInfo request_;
			masterworker::ShardStartingMsg reply_;

			std::thread t;

	    // The means to get back to the client.
	    grpc::ServerAsyncResponseWriter<masterworker::ShardStartingMsg> responder_;
};









class MapperPingCallData : public MapperCallData {
public:

		MapperPingCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * worker);

		void Proceed();
		void Finish();

	private:
			masterworker::PingMsg request_;
			masterworker::PingAck reply_;

	    // The means to get back to the client.
	    grpc::ServerAsyncResponseWriter<masterworker::PingAck> responder_;

};










void MapDataShard(MapperShardCallData * callData)
{
		auto mapper = get_mapper_from_task_factory("cs6210");

		FileShard fileShardArg = callData->GetFileShard();

		// First open the file(s)

		std::ifstream shard(fileShardArg.fileName);

		shard.seekg(0, shard.end);
		int fileLength = shard.tellg();

		if (fileShardArg.offset + fileShardArg.shardSize > fileLength)
		{
				std::cout << "offset + size > length!\n";
			  // TODO handle error here.
		}

		shard.seekg(fileShardArg.offset);
		int position = shard.tellg();

		std::string record;
		while(getline(shard, record) && (position - fileShardArg.offset) < fileShardArg.shardSize)
		{
			  callData->Map(record);
		}

		position = shard.tellg();

		if (position != fileShardArg.offset + fileShardArg.shardSize)
		{
				std::cout << "Something went wrong here..\n";
		    // todo figure out what to do with this
		}
		else
		{
			  std::cout << "Done mapping!\n";
		}

		shard.close();

		callData->Finish();
}












/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
		ipAndPort = ip_addr_port;
		mapper = get_mapper_from_task_factory("cs6210");
}



/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/

bool Worker::run() {

		//builder.AddListeningPort("0.0.0.0:50051", InsecureServerCredentials());
		builder.AddListeningPort(ipAndPort, InsecureServerCredentials());
		builder.RegisterService(&asyncService);

		completionQueue = builder.AddCompletionQueue();
		server = builder.BuildAndStart();

		new MapperShardCallData(&asyncService, completionQueue.get(), this);
		new MapperPingCallData(&asyncService, completionQueue.get(), this);

		void *tag;
		bool ok;

		while (true)
		{
		    // Block waiting to read the next event from the completion queue. The
		    // event is uniquely identified by its tag, which in this case is the
		    // memory address of a CallData instance.
		    completionQueue->Next(&tag, &ok);
		    GPR_ASSERT(ok);
		    static_cast<MapperCallData*>(tag)->Proceed();
	  }

/*
	std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	auto mapper = get_mapper_from_task_factory("cs6210");
	mapper->map("I m just a 'dummy', a \"dummy line\"");
*/


	auto reducer = get_reducer_from_task_factory("cs6210");
	reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	return true;
}


void Worker::Map(std::string s)
{
	  mapper->map(s);
}

void Worker::Done()
{
		mapper->impl_->Done();
}

void Worker::SetupBaseMapperImpl(std::string outputDir)
{

}







/******************************************************************************
**
** MapperCallData implementation
**
******************************************************************************/

void MapperCallData::Map(std::string s)
{
	  worker->Map(s);
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
				std::cout << "Got it, now creating a new mapshardcalldata and startworkmapthread\n";
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



















MapperPingCallData::MapperPingCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * w)
		: responder_(&ctx_)
{
		service_ = service;
		cq_ = cq;
		status_ = CREATE;
		worker = w;


		std::cout << "Requesting ping.\n";
		 // Invoke the serving logic right away.
		Proceed();
}


void MapperPingCallData::Proceed()
{
		if (status_ == CREATE)
		{
				status_ = PROCESS;

				// As part of the initial CREATE state, we *request* that the system
				// start processing SayHello requests. In this request, "this" acts are
				// the tag uniquely identifying the request (so that different CallData
				// instances can serve different requests concurrently), in this case
				// the memory address of this CallData instance.
				service_->RequestPing(&ctx_, &request_, &responder_, cq_, cq_, this);
		}
		else if (status_ == PROCESS)
		{
				// Spawn a new CallData instance to serve new clients while we process
				// the one for this CallData. The instance will deallocate itself as
				// part of its FINISH state.
				new MapperPingCallData(service_, cq_, worker);

				std::cout << "Responding to ping!\n";

				status_ = FINISH;
				reply_.set_ack(99);
				responder_.Finish(reply_,  grpc::Status::OK, this);
		}
		else
		{
				GPR_ASSERT(status_ == FINISH);
				// Once in the FINISH state, deallocate ourselves (CallData).
				delete this;
		}

}

void MapperPingCallData::Finish()
{
	std::cout << "Worker done with this shard.\n";
	status_ = FINISH;
	responder_.Finish(reply_, grpc::Status::OK, this);
}

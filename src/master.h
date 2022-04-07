#pragma once

#include "mapreduce_spec.h"
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



/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

		void SendShardRPCToWorker(std::string ipAndPort, FileShard &shard);
		void SendPingRPCToWorker(std::string ipAndPort);

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		const MapReduceSpec * mr_spec;
		const std::vector<FileShard> * fileShards;

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
		this->mr_spec = &mr_spec;
		this->fileShards = &file_shards;
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

	// for each worker in worker list from file
	//   assign work based on number of bytes per shard
	//

	std::cout << "In master::run()!\n";

	for (int i = 0; i < mr_spec->ipAddressAndPorts.size(); i++)
	{
			std::string ipAddressAndPort = mr_spec->ipAddressAndPorts[i];
			FileShard shard = (*fileShards)[i];
		  SendShardRPCToWorker(ipAddressAndPort, shard);
	}

	for (int i = 0; i < mr_spec->ipAddressAndPorts.size(); i++)
	{
			std::string ipAddressAndPort = mr_spec->ipAddressAndPorts[i];
			//FileShard shard = (*fileShards)[i];
			SendPingRPCToWorker(ipAddressAndPort);
	}

	return true;
}


void Master::SendShardRPCToWorker(std::string ipAndPort, FileShard &shard)
{
		std::cout << "Sending shard to worker @ " << ipAndPort << "!\n";

		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );
		grpc::CompletionQueue          completionQueue;
		grpc::ClientContext 					 context;

		masterworker::ShardInfo        request;
		masterworker::ShardStartingMsg response;

		std::unique_ptr<masterworker::MapperReducer::Stub>  mapStub;
		std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::ShardStartingMsg>>  responseReader;

		grpc::Status                   status;

		channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );

		// write the name to the request
		request.set_filename(shard.fileName);
		request.set_offset(shard.offset);
		request.set_shardsize(shard.shardSize);
		request.set_outputdirectory(mr_spec->outputDir);
		request.set_numberoffiles(mr_spec->numberOfOutputFiles);

		// create a stub using the channel
		mapStub = masterworker::MapperReducer::NewStub(channel);

		// create an asynchronous response reader using the stub, request, and completion queue
		//   - this will watch for the response in the background
		//   - then read it into a BidReply
		//   - then let us know when the BidReply is available for reading via the completionQueue
		responseReader =
				std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::ShardStartingMsg>>
						(mapStub->AsyncMapShard(&context, request, &completionQueue));

		std::cout << " - Calling finish\n";

		responseReader->Finish(&response, &status, (void*)(long long int) 1);

		void* got_tag;
		bool ok = false;
		completionQueue.Next(&got_tag, &ok);

		if (ok && got_tag == (void*)1)
		{
			  std::cout << " - Got okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";// ? "okay!" : "not okay!") << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
		}

}


void Master::SendPingRPCToWorker(std::string ipAndPort)
{
		std::cout << "Sending ping to worker @ " << ipAndPort << "!\n";

		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );
		grpc::CompletionQueue          completionQueue;
		grpc::ClientContext 					 context;

		masterworker::PingMsg          request;
		masterworker::PingAck          response;

		std::unique_ptr<masterworker::MapperReducer::Stub>  mapStub;
		std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::PingAck>>  responseReader;

		grpc::Status                   status;

		channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );

		mapStub = masterworker::MapperReducer::NewStub(channel);

		// create an asynchronous response reader using the stub, request, and completion queue
		//   - this will watch for the response in the background
		//   - then read it into a BidReply
		//   - then let us know when the BidReply is available for reading via the completionQueue
		responseReader =
				std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::PingAck>>
						(mapStub->AsyncPing(&context, request, &completionQueue));

		std::cout << " - Calling finish\n";

		responseReader->Finish(&response, &status, (void*)(long long int) 1);

		void* got_tag;
		bool ok = false;
		completionQueue.Next(&got_tag, &ok);

		if (ok && got_tag == (void*)1)
		{
			  std::cout << " - Got okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";// ? "okay!" : "not okay!") << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
		}
}

#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

#include "masterworker.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <time.h>
#include <stdlib.h>
#include <sys/time.h>
#include <chrono>

#include "worker.h"

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
/*
	enum WorkerState
	{
		  IDLE = 0,
			BUSY = 1,
			DONE = 2,
			ERROR = 3,
			MISSING = 4
	};
*/
	struct WorkerStatus
	{
			int                 workerID;
			std::string         ipAndPort;
			//WorkerState         state;
			int                 state;
			FileShard *         assignedShard;
			unsigned long long  lastPingTime;
			unsigned long long  assignedTime;

			std::shared_ptr<grpc::Channel> channel;
			std::unique_ptr<MapperReducer::Stub> stub;
	};


	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

		//void SendShardRPCToWorker(std::string ipAndPort, FileShard &shard);
		void SendPingRPCToWorker(Master::WorkerStatus * ws);
		void SendWorkerInfoToWorker(int i, WorkerStatus *ws);
		void SendShardRPCToWorker(WorkerStatus * ws, FileShard * shard);
		void TryToAssignWorkToWorker(WorkerStatus * ws);

		void SendWriteIntermediateToFile(WorkerStatus * ws);
		void SendDropIntermediateToFile(WorkerStatus * ws);

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		std::unique_ptr<MapperReducer::Stub> stub_;
		std::shared_ptr<grpc::Channel> channel;

		const MapReduceSpec * mr_spec;
		std::vector<FileShard> fileShards;
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
{
		this->mr_spec = &mr_spec;


		for(int i = 0; i < file_shards.size(); i++)
		{
			  fileShards.push_back(file_shards[i]);
		}
}


void Master::TryToAssignWorkToWorker(WorkerStatus * ws)
{
				FileShard * shard = NULL;

				// attempt to find a shard that this worker has not already tried
				for(int i = 0; i < fileShards.size(); i++)
				{
						FileShard * temp = &(fileShards[i]);

						if(temp->state != FILE_SHARD_STATE_UNASSIGNED)
						{
							  continue;
						}

            bool foundit = false;

						for (int it = 0; it < temp->workersThatAttemptedThis.size(); it++)
						{
							  if (temp->workersThatAttemptedThis[it] == ws->workerID)
								{
									  foundit = true;
										shard = temp;
										break;
								}
						}

						if ( foundit == false )
						{
								SendShardRPCToWorker(ws, temp);
								return;
						}
				}

}



/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

	srand(time(NULL));

	// for each worker in worker list from file
	//   assign work based on number of bytes per shard
	//

	std::cout << "In master::run()!\n";

	// enter the main processing loop

	/*std::vector<WorkerStatus*>  idleWorkers;
	std::vector<WorkerStatus*>  busyWorkers;
	std::vector<WorkerStatus*>  deadWorkers;*/
	std::vector<WorkerStatus*> workers;

	//std::vector<FileShard>		 assignedShards;
	std::vector<FileShard*>    completedShards;  // just store a pointer to the shard, no point to duplicating memory

	for (int i = 0; i < mr_spec->ipAddressAndPorts.size(); i++)
	{
		  WorkerStatus * ws = new WorkerStatus;
			ws->workerID = i;
			ws->ipAndPort = mr_spec->ipAddressAndPorts[i];
			ws->state = STATUS_CODE_IDLE;
			ws->assignedShard = NULL;
			ws->lastPingTime = 0;
			ws->assignedTime = 0;
 		  ws->channel = grpc::CreateChannel( ws->ipAndPort, grpc::InsecureChannelCredentials() );
 		  ws->stub = (MapperReducer::NewStub(ws->channel));

			SendPingRPCToWorker(ws);

			workers.push_back(ws);

			if (ws->state == STATUS_CODE_MISSING)
			{
				  //deadWorkers.push_back(ws);
			}
			else
			{
  				//idleWorkers.push_back(ws);
					SendWorkerInfoToWorker(i, ws);
			}
	}


	// main processing loop
	// while completedShards queue is not same size as shardsQueue:
	//     while workers in dead workers:
	//         try to ping
	//         if succeeded AND idle give work
	//	  		 if succeeded AND busy, wait on work complete
	//         if failed, assign their assignedShard over to someone else(?)
	//             if it comes back late, we will just ignore whichever result comes back last
	//
	//     (remember, workerstatus gets put back in idle queue when done!)
	//
	//     while workers in idle workers:
	//         move their assignedShard to completedShards
	//		     pass them a new shard(s) to work on
	//
	//     once all shards are mapped, send MapDone msg?
	//     why though, just have it emit to files as it goes???

	int numberOfShards = fileShards.size();
	while (completedShards.size() != numberOfShards)
	{
			for (int i = 0; i < workers.size(); i++)
			{
					WorkerStatus * ws = workers[i];

				  switch(ws->state)
					{
						  case STATUS_CODE_IDLE:
							{
									std::cout << "Worker " << std::to_string(i) << " is IDLE\n";
									TryToAssignWorkToWorker(ws);
									break;
							}
							case STATUS_CODE_WORKING:
							{
									std::cout << "Worker " << std::to_string(i) << " is WORKING\n";
									SendPingRPCToWorker(ws);
									break;
							}
							case STATUS_CODE_FAILED:
							{
								  // complicated case...  what do we do with this?
									// reassign the shard, but NOT to this worker (?)
									std::cout << "Worker " << std::to_string(i) << " is FAILED\n";

									ws->assignedShard->workersThatAttemptedThis.push_back(ws->workerID);
									ws->state = STATUS_CODE_IDLE;
									ws->assignedShard->state = FILE_SHARD_STATE_UNASSIGNED;
									ws->assignedShard = NULL;

									break;
							}
							case STATUS_CODE_COMPLETE:
							{
									std::cout << "Worker " << std::to_string(i) << " is COMPLETE\n";

									FileShard * shard = ws->assignedShard;

									if (std::find(completedShards.begin(),
																 completedShards.end(),
																 shard) == completedShards.end())
									{
										  completedShards.push_back(shard);
											shard->state = FILE_SHARD_STATE_COMPLETE;
											ws->state = STATUS_CODE_WRITING_MAP;

											SendWriteIntermediateToFile(ws);
									}
									else
									{
										  // if this shard was completed by someone else first, discard it
											SendDropIntermediateToFile(ws);
									}

									break;
							}
							case STATUS_CODE_MISSING:
							{
									std::cout << "Worker " << std::to_string(i) << " is MISSING\n";

									break;
								  // just here for completion, not doing anything with this
							}
							case STATUS_CODE_WRITING_MAP:
							{
								  std::cout << "Waiting for worker " << std::to_string(i) << " to finish writing map.\n";
									SendPingRPCToWorker(ws);
									break;
							}
							case STATUS_CODE_MAP_WRITE_COMPLETE:
							{
									std::cout << "Done writing worker map for worker " << std::to_string(i) << "\n";
									ws->state = STATUS_CODE_IDLE;
									break;
							}
					}
			}

		/*	std::cout << "Your code is hanging here because you took all the workers out of idle but didn't put them anywhere.\n";
			std::cout << "Your RPC is being done synchronously.  You need to use pings to monitor for when a worker is done.\n";
			std::cout << "You need to add a timeout to your synchronous calls as well, so if they timeout the whole app doesn't hang.\n";
			*/

			//std::this_thread::sleep_for(std::chrono::milliseconds(500)); // let everyone get started
			std::this_thread::sleep_for(std::chrono::milliseconds(500)); // let everyone get started

			// so now we have to go through and try to ping everyone.
			// we should have zero idle workers because they should all be in the busy queue after that last loop
/*
			// kind of convoluted loop but it works...
			int numberOfBusyWorkers = busyWorkers.size();
			int numBusyWorkersVisited = 0;
			int busyWorkerIndex = 0;

			while (numBusyWorkersVisited < numberOfBusyWorkers)
			{
					switch(busyWorkers)
			}
*/
	}


	std::cout << "MASTER SENT EVERY SHARD TO WORKERS!!\n";



	/*for (int i = 0; i < mr_spec->ipAddressAndPorts.size(); i++)
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
	}*/

	return true;
}


void Master::SendShardRPCToWorker(WorkerStatus * ws, FileShard * shard)
{
		std::cout << "Sending shard to worker @ " << ws->ipAndPort << "!\n";

		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel( ws->ipAndPort, grpc::InsecureChannelCredentials() );
		grpc::CompletionQueue          completionQueue;
		grpc::ClientContext 					 context;

		masterworker::ShardInfo        request;
		masterworker::Ack              response;

		std::unique_ptr<masterworker::MapperReducer::Stub>  mapStub;
		std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>  responseReader;

		grpc::Status                   status;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		channel = grpc::CreateChannel( ws->ipAndPort, grpc::InsecureChannelCredentials() );

		// write the name to the request
		request.set_filename(shard->fileName);
		request.set_offset(shard->offset);
		request.set_shardsize(shard->shardSize);
		std::cout << "Sending shardID " << std::to_string(shard->shardID) << "\n";
		request.set_shardid(shard->shardID);

		// create a stub using the channel
		mapStub = masterworker::MapperReducer::NewStub(channel);

		// create an asynchronous response reader using the stub, request, and completion queue
		//   - this will watch for the response in the background
		//   - then read it into a BidReply
		//   - then let us know when the BidReply is available for reading via the completionQueue
		responseReader =
				std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>
						(mapStub->AsyncMapShard(&context, request, &completionQueue));

		std::cout << " - Calling finish\n";

		long long int randTag = rand();
		void * tag = (void*)randTag;

		//responseReader->Finish(&response, &status, (void*)(long long int) 1);
		responseReader->Finish(&response, &status, tag);

		void* got_tag;
		bool ok = false;
		completionQueue.Next(&got_tag, &ok);

		if (ok && got_tag == tag)
		{
			  std::cout << " - Got okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";// ? "okay!" : "not okay!") << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
				ws->state = STATUS_CODE_WORKING;
				shard->state = FILE_SHARD_STATE_ASSIGNED;
				ws->assignedShard = shard;
		}
		else
		{
			  ws->state = STATUS_CODE_MISSING;
		}

}


void Master::SendPingRPCToWorker(Master::WorkerStatus * ws)
{

	masterworker::EmptyMsg request;
	masterworker::Ack        reply;

	grpc::ClientContext context;

	grpc::Status status = ws->stub->Ping(&context, request, &reply);

	if (status.ok())
	{
			return;
	}
	else
	{
			std::cout << " - SendWorkerInfoToWorker not okay!\n";
			std::cout << " - status is " << (status.error_code()) << "\n";
			std::cout << " - status msg is " << (status.error_message()) << "\n";
	}

	/*
		if (ws == NULL)
		{
	      std::cout << "ERROR:  Could not send ping to worker because ws was null!";
		}

		std::cout << "Sending ping to worker @ " << ws->ipAndPort << "!\n";

		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel( ws->ipAndPort, grpc::InsecureChannelCredentials() );
		grpc::CompletionQueue          completionQueue;
		grpc::ClientContext 					 context;

		masterworker::EmptyMsg      request;
		masterworker::Ack           response;

		std::unique_ptr<masterworker::MapperReducer::Stub>  mapStub;
		std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>  responseReader;

		grpc::Status                   status;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		channel = grpc::CreateChannel( ws->ipAndPort, grpc::InsecureChannelCredentials() );

		mapStub = masterworker::MapperReducer::NewStub(channel);

		// create an asynchronous response reader using the stub, request, and completion queue
		//   - this will watch for the response in the background
		//   - then read it into a BidReply
		//   - then let us know when the BidReply is available for reading via the completionQueue
		responseReader =
				std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>
						(mapStub->AsyncPing(&context, request, &completionQueue));

		std::cout << " - Calling finish\n";

		long long int randTag = rand();
		void * tag = (void*)randTag;

		responseReader->Finish(&response, &status, (void*)tag);

		void* got_tag = 0;
		bool ok = false;
*/

/*
		gpr_timespec deadline;
		deadline.clock_type = GPR_TIMESPAN;
		deadline.tv_sec = 1;
		deadline.tv_nsec = 0;

		grpc::CompletionQueue::NextStatus pingStatus = completionQueue.AsyncNext(&got_tag, &ok, deadline);
*/
/*
		grpc::CompletionQueue::NextStatus pingStatus = completionQueue.AsyncNext(&got_tag, &ok, std::chrono::system_clock::now() + std::chrono::milliseconds(1000));

		// regardless of response, record the time we pinged it.
		struct timeval thetime;
		gettimeofday(&thetime, NULL);

		ws->lastPingTime = thetime.tv_sec;

		if (pingStatus == grpc::CompletionQueue::GOT_EVENT && ok && got_tag == tag)
		{
			  std::cout << " - Got okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";// ? "okay!" : "not okay!") << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
				std::cout << " - response is " << response.response() << "\n";

				// do stuff here!
				// - set workerstatus statuscode here!
				// DO stuff in the main thread.

				ws->state = response.response();
		}
		else
		{
			  std::cout << "Worker did not respond to ping!\n";
				std::cout << "Moving worker to \"missing\" state until a response is reached.\n";

				std::cout << "Okay was: " << std::to_string(ok) << "\n";
				std::cout << "got_tag was: " << got_tag << " and we expected " << tag << "\n";

				ws->state = STATUS_CODE_MISSING;
		}
		*/
}



void Master::SendWorkerInfoToWorker(int i, WorkerStatus *ws)
{
		masterworker::WorkerInfo request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		request.set_workerid(i);
		request.set_outputdirectory(mr_spec->outputDir);
		request.set_numberofworkers(mr_spec->numberOfWorkers);
		request.set_numberoffiles(mr_spec->numberOfOutputFiles);

		grpc::Status status = ws->stub->SetWorkerInfo(&context, request, &reply);

		if (status.ok())
		{
		  	return;
		}
		else
		{
		  	std::cout << " - SendWorkerInfoToWorker not okay!\n";
		  	std::cout << " - status is " << (status.error_code()) << "\n";
		  	std::cout << " - status msg is " << (status.error_message()) << "\n";
		}

	/*
		std::string ipAndPort = ws->ipAndPort;
		std::cout << "Sending setup info to worker @ " << ipAndPort << "!\n";

		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );
		grpc::CompletionQueue          completionQueue;
		grpc::ClientContext 					 context;

		masterworker::WorkerInfo       request;
		masterworker::Ack              response;

		std::unique_ptr<masterworker::MapperReducer::Stub>  mapStub;
		std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>  responseReader;

		grpc::Status                   status;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );

		// write the name to the request
		request.set_workerid(i);
		request.set_outputdirectory(mr_spec->outputDir);
		request.set_numberofworkers(mr_spec->numberOfWorkers);
		request.set_numberoffiles(mr_spec->numberOfOutputFiles);

		// create a stub using the channel
		mapStub = masterworker::MapperReducer::NewStub(channel);

		// create an asynchronous response reader using the stub, request, and completion queue
		//   - this will watch for the response in the background
		//   - then read it into a BidReply
		//   - then let us know when the BidReply is available for reading via the completionQueue
		responseReader =
				std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>
						(mapStub->AsyncSetWorkerInfo(&context, request, &completionQueue));

		std::cout << " - Calling finish\n";

		long long int randTag = rand();
		void * tag = (void*)randTag;

		//responseReader->Finish(&response, &status, (void*)(long long int) 1);
		responseReader->Finish(&response, &status, tag);

		void* got_tag;
		bool ok = false;
		//completionQueue.Next(&got_tag, &ok);

		grpc::CompletionQueue::NextStatus asyncStatus = completionQueue.AsyncNext(&got_tag, &ok, std::chrono::system_clock::now() + std::chrono::milliseconds(1000));

		if (ok && got_tag == tag && asyncStatus == grpc::CompletionQueue::GOT_EVENT)
		{
			  std::cout << " - Got okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";// ? "okay!" : "not okay!") << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
		}
*/

}





void Master::SendWriteIntermediateToFile(WorkerStatus * ws)
{
	/*
		std::string ipAndPort = ws->ipAndPort;
		std::cout << "Sending write intermediate files to worker @ " << ipAndPort << "!\n";

		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );
		grpc::CompletionQueue          completionQueue;
		grpc::ClientContext 					 context;

		masterworker::EmptyMsg         request;
		masterworker::Ack              response;

		std::unique_ptr<masterworker::MapperReducer::Stub>  mapStub;
		std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>  responseReader;

		grpc::Status                   status;

		channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );

		// create a stub using the channel
		mapStub = masterworker::MapperReducer::NewStub(channel);

		// create an asynchronous response reader using the stub, request, and completion queue
		//   - this will watch for the response in the background
		//   - then read it into a BidReply
		//   - then let us know when the BidReply is available for reading via the completionQueue
		responseReader =
				std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>
						(mapStub->AsyncWriteShardToIntermediateFile(&context, request, &completionQueue));

		std::cout << " - Calling finish\n";

		long long int randTag = rand();
		void * tag = (void*)randTag;

		responseReader->Finish(&response, &status, tag);

		void* got_tag;
		bool ok = false;

		grpc::CompletionQueue::NextStatus asyncStatus = completionQueue.AsyncNext(&got_tag, &ok, std::chrono::system_clock::now() + std::chrono::milliseconds(1000));

		//completionQueue.Next(&got_tag, &ok);

		if (ok && got_tag == tag && asyncStatus == grpc::CompletionQueue::GOT_EVENT)
		{
				std::cout << " - Got okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";// ? "okay!" : "not okay!") << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
		}
	*/
}



void Master::SendDropIntermediateToFile(WorkerStatus * ws)
{
	/*
		std::string ipAndPort = ws->ipAndPort;
		std::cout << "Sending dump intermediate files to worker @ " << ipAndPort << "!\n";

		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );
		grpc::CompletionQueue          completionQueue;
		grpc::ClientContext 					 context;

		masterworker::EmptyMsg         request;
		masterworker::Ack              response;

		std::unique_ptr<masterworker::MapperReducer::Stub>  mapStub;
		std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>  responseReader;

		grpc::Status                   status;

		channel = grpc::CreateChannel( ipAndPort, grpc::InsecureChannelCredentials() );

		// create a stub using the channel
		mapStub = masterworker::MapperReducer::NewStub(channel);

		// create an asynchronous response reader using the stub, request, and completion queue
		//   - this will watch for the response in the background
		//   - then read it into a BidReply
		//   - then let us know when the BidReply is available for reading via the completionQueue
		responseReader =
				std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Ack>>
						(mapStub->AsyncDiscardShardResults(&context, request, &completionQueue));

		std::cout << " - Calling finish\n";

		long long int randTag = rand();
		void * tag = (void*)randTag;


		responseReader->Finish(&response, &status, tag);

		void* got_tag;
		bool ok = false;



//		completionQueue.Next(&got_tag, &ok);
		grpc::CompletionQueue::NextStatus asyncStatus = completionQueue.AsyncNext(&got_tag, &ok, std::chrono::system_clock::now() + std::chrono::milliseconds(1000));

		if (ok && got_tag == tag && asyncStatus == grpc::CompletionQueue::GOT_EVENT)
		{
				std::cout << " - Got okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";// ? "okay!" : "not okay!") << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
		}
	*/
}

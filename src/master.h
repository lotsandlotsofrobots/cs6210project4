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

#define WORKER_FLAGS_NORMAL           0
#define WORKER_FLAGS_SLOW             1
#define WORKER_FLAGS_SLOW_REASSIGNED  2

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */

class Master {

	struct WorkerStatus
	{
			int                 workerID;
			std::string         ipAndPort;
			int                 state;
			FileShard *         assignedShard;
			unsigned long long  lastPingTime;
			unsigned long long  assignedTime;
			int                 slowWorkerFlags;

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

	std::vector<WorkerStatus*> workers;
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

									if (ws->slowWorkerFlags == WORKER_FLAGS_SLOW)
									{
											std::cout << "Dealing with slow worker here.\n";

										  FileShard * shard = ws->assignedShard;
											shard->state = FILE_SHARD_STATE_UNASSIGNED;
											// don't NULL it's assignedShard though, just let someone else grab it up
											// BUT don't let it re-grab it
											ws->assignedShard->workersThatAttemptedThis.push_back(ws->workerID);
									}

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
									std::cout << "Worker is completed.\n";
									FileShard * shard = ws->assignedShard;
									std::cout << "Worker " << std::to_string(i) << ", (" << std::to_string(shard->shardID) << ") is COMPLETE\n";


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

									// reassign it's work
									FileShard * shard = ws->assignedShard;
									shard->state = FILE_SHARD_STATE_UNASSIGNED;

									// try to get it back
									SendPingRPCToWorker(ws);
									break;
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

			std::this_thread::sleep_for(std::chrono::milliseconds(10)); // let everyone get started

	}


	std::cout << "MASTER SENT EVERY SHARD TO WORKERS!!\n";

	return true;
}


void Master::SendShardRPCToWorker(WorkerStatus * ws, FileShard * shard)
{
	 std::cout << "Sending shard " << shard->shardID << "\n";


		masterworker::ShardInfo  request;
		masterworker::Ack        reply;

		// write the name to the request
		request.set_filename(shard->fileName);
		request.set_offset(shard->offset);
		request.set_shardsize(shard->shardSize);
		std::cout << "Sending shardID " << std::to_string(shard->shardID) << "\n";
		request.set_shardid(shard->shardID);

		grpc::ClientContext context;

		grpc::Status status = ws->stub->MapShard(&context, request, &reply);

		if (status.ok())
		{
				// reset this flag
				ws->slowWorkerFlags = WORKER_FLAGS_NORMAL;
				ws->state = reply.response();

			  struct timeval tv;
				gettimeofday(&tv, NULL);

				ws->state = STATUS_CODE_WORKING;
				ws->assignedTime = tv.tv_sec;
				ws->assignedShard = shard;

				shard->state = FILE_SHARD_STATE_ASSIGNED;

				return;
		}
		else
		{
				std::cout << " - SendShardRPCToWorker not okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();

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
				ws->state = reply.response();

				struct timeval tv;
				gettimeofday(&tv, NULL);

				int duration = tv.tv_sec - ws->assignedTime;

				if (duration > 1 && ws->state == STATUS_CODE_WORKING)
				{
					  std::cout << "Worker flaged as slow!";
						ws->slowWorkerFlags = WORKER_FLAGS_SLOW;
				}

				return;
		}
		else
		{
				std::cout << " - SendPingRPCToWorker not okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();

		}

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

}





void Master::SendWriteIntermediateToFile(WorkerStatus * ws)
{
		masterworker::EmptyMsg request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		grpc::Status status = ws->stub->WriteShardToIntermediateFile(&context, request, &reply);

		if (status.ok())
		{
				//ws->state = reply.response();
				return;
		}
		else
		{
				std::cout << " - SendWorkerInfoToWorker not okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();
		}

}



void Master::SendDropIntermediateToFile(WorkerStatus * ws)
{
		masterworker::EmptyMsg request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		grpc::Status status = ws->stub->DiscardShardResults(&context, request, &reply);

		if (status.ok())
		{
				return;
		}
		else
		{
				std::cout << " - SendWorkerInfoToWorker not okay!\n";
				std::cout << " - status is " << (status.error_code()) << "\n";
				std::cout << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();
		}

}

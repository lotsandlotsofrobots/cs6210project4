#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "reduce_task.h"

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
			int                 workerID = 0;
			std::string         ipAndPort = "";
			int                 state = 0;
			FileShard *         assignedShard = NULL;
			ReduceTask *        assignedReduce = NULL;
			unsigned long long  lastPingTime = 0;
			unsigned long long  assignedTime = 0;
			int                 slowWorkerFlags = 0;

			std::shared_ptr<grpc::Channel> channel = NULL;
			std::unique_ptr<MapperReducer::Stub> stub = NULL;
	};


	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

		//void SendShardRPCToWorker(std::string ipAndPort, FileShard &shard);
		void SendPingRPCToWorker(Master::WorkerStatus * ws);
		void SendWorkerInfoToWorker(int i, WorkerStatus *ws, int numberOfShardsTotal);
		void SendShardRPCToWorker(WorkerStatus * ws, FileShard * shard);
		void TryToAssignWorkToWorker(WorkerStatus * ws);

		void SendWriteIntermediateToFile(WorkerStatus * ws);
		void SendDropIntermediateToFile(WorkerStatus * ws);

		void TryToAssignReduceToWorker(WorkerStatus * ws);
		void SendReduceToWorker(WorkerStatus * ws, ReduceTask * rt);
		void SendWriteReduceToFile(WorkerStatus * ws);
		void SendDropReduceToFile(WorkerStatus * ws);

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		std::unique_ptr<MapperReducer::Stub> stub_;
		std::shared_ptr<grpc::Channel> channel;

		const MapReduceSpec * mr_spec;
		std::vector<FileShard> fileShards;
		std::vector<ReduceTask> reduceTasks;

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
bool Master::run()
{

		srand(time(NULL));

		// for each worker in worker list from file
		//   assign work based on number of bytes per shard
		//

		std::cerr << "In master::run()!\n";

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
						SendWorkerInfoToWorker(i, ws, fileShards.size());
				}
		}

		//std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		std::this_thread::sleep_for(std::chrono::milliseconds(10));


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
										//std::cerr << "Worker " << std::to_string(i) << " is IDLE\n";
										TryToAssignWorkToWorker(ws);
										break;
								}
								case STATUS_CODE_MAP_WORKING:
								{
										//std::cerr << "Worker " << std::to_string(i) << " is WORKING\n";
										SendPingRPCToWorker(ws);

										if (ws->slowWorkerFlags == WORKER_FLAGS_SLOW)
										{
												std::cerr << "Dealing with slow worker here.\n";

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
										//std::cerr << "Worker " << std::to_string(i) << " is FAILED\n";

										ws->assignedShard->workersThatAttemptedThis.push_back(ws->workerID);
										ws->state = STATUS_CODE_IDLE;
										ws->assignedShard->state = FILE_SHARD_STATE_UNASSIGNED;
										ws->assignedShard = NULL;

										break;
								}
								case STATUS_CODE_COMPLETE:
								{
										//std::cerr << "Worker is completed.\n";
										FileShard * shard = ws->assignedShard;
										std::cerr << "Worker " << std::to_string(i) << ", (" << std::to_string(shard->shardID) << ") is COMPLETE\n";


										if (std::find(completedShards.begin(),
																	 completedShards.end(),
																	 shard) == completedShards.end())
										{
											  //completedShards.push_back(shard);
												//shard->state = FILE_SHARD_STATE_COMPLETE;
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
										//std::cerr << "Worker " << std::to_string(i) << " is MISSING\n";

										// reassign it's work
										FileShard * shard = ws->assignedShard;
										shard->state = FILE_SHARD_STATE_UNASSIGNED;

										// try to get it back
										SendPingRPCToWorker(ws);
										break;
								}
								case STATUS_CODE_WRITING_MAP:
								{
									  //std::cerr << "Waiting for worker " << std::to_string(i) << " to finish writing map "  << " for shard " << std::to_string(ws->assignedShard->shardID) << ".\n";
										SendPingRPCToWorker(ws);
										break;
								}
								case STATUS_CODE_MAP_WRITE_COMPLETE:
								{
										//std::cerr << "Done writing worker map for worker " << std::to_string(i)  << " for shard " << std::to_string(ws->assignedShard->shardID) << "\n";

										ws->state = STATUS_CODE_IDLE;
										ws->assignedShard->state = FILE_SHARD_STATE_COMPLETE;

										completedShards.push_back(ws->assignedShard);

										ws->assignedShard = NULL;
										break;
								}
						}
				}

				//std::this_thread::sleep_for(std::chrono::milliseconds(1)); // let everyone get started

		}


		std::cerr << "MASTER SENT EVERY SHARD TO WORKERS!!\n";


		// time to map things!

		// get the list of files they need to work on

		// worker needs to build up key/value pairs like this:
		/*auto reducer = get_reducer_from_task_factory("cs6210");
		reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));*/

		std::vector<ReduceTask*> completedReduceTasks;

		for (int i = 0; i < mr_spec->numberOfOutputFiles; i++)
		{
			  ReduceTask r;

				r.assignedWorker = NULL;
				r.state = REDUCE_STATE_UNASSIGNED;
				r.reduceID = i;

				reduceTasks.push_back(r);
		}


		while (completedReduceTasks.size() != mr_spec->numberOfOutputFiles)
		{
				std::cerr << "Should be " << mr_spec->numberOfOutputFiles << ", actually is " << completedReduceTasks.size() << "\n";

				for (int i = 0; i < workers.size(); i++)
				{
						WorkerStatus * ws = workers[i];
						std::cerr << "Worker is " << ws->state << "\n";

						switch(ws->state)
						{
								case STATUS_CODE_IDLE:
								{
										std::cerr << "Worker " << std::to_string(i) << " is IDLE\n";
										TryToAssignReduceToWorker(ws);
										break;
								}
								case STATUS_CODE_REDUCE_WORKING:
								{
										std::cerr << "Worker " << std::to_string(i) << " is WORKING\n";
										SendPingRPCToWorker(ws);

										if (ws->slowWorkerFlags == WORKER_FLAGS_SLOW)
										{
												std::cerr << "Dealing with slow worker here.\n";

												ReduceTask * shard = ws->assignedReduce;
												shard->state = REDUCE_STATE_UNASSIGNED;
												// don't NULL it's assignedShard though, just let someone else grab it up
												// BUT don't let it re-grab it
												ws->assignedReduce->workersThatAttemptedThis.push_back(ws->workerID);
										}

										break;
								}
								case STATUS_CODE_FAILED:
								{
										// complicated case...  what do we do with this?
										// reassign the shard, but NOT to this worker (?)
										std::cerr << "Worker " << std::to_string(i) << " is FAILED\n";

										ws->assignedReduce->workersThatAttemptedThis.push_back(ws->workerID);
										ws->state = STATUS_CODE_IDLE;
										ws->assignedReduce->state = REDUCE_STATE_UNASSIGNED;
										ws->assignedReduce = NULL;

										break;
								}
								case STATUS_CODE_COMPLETE:
								{
										//std::cerr << "Worker is completed.\n";
										ReduceTask * rt = ws->assignedReduce;
										std::cerr << "Worker " << std::to_string(i) << ", (" << std::to_string(rt->reduceID) << ") is REDUCE COMPLETE\n";


										if (std::find(completedReduceTasks.begin(),
																	 completedReduceTasks.end(),
																	 rt) == completedReduceTasks.end())
										{
												completedReduceTasks.push_back(rt);
												rt->state = REDUCE_STATE_COMPLETE;
												ws->state = STATUS_CODE_WRITING_REDUCE;

												SendWriteReduceToFile(ws);
										}
										else
										{
												// if this shard was completed by someone else first, discard it
												SendDropReduceToFile(ws);
										}

										break;
								}
								case STATUS_CODE_MISSING:
								{
										std::cerr << "Worker " << std::to_string(i) << " is MISSING\n";

										// reassign it's work
										ReduceTask * rt = ws->assignedReduce;
										rt->state = REDUCE_STATE_UNASSIGNED;

										// try to get it back
										SendPingRPCToWorker(ws);
										break;
								}
								case STATUS_CODE_WRITING_REDUCE:
								{
										std::cerr << "Waiting for worker " << std::to_string(i) << " to finish writing map.\n";
										SendPingRPCToWorker(ws);
										break;
								}
								case STATUS_CODE_REDUCE_WRITE_COMPLETE:
								{
										std::cerr << "Done writing worker reduce for worker " << std::to_string(i) << "\n";
										ws->state = STATUS_CODE_IDLE;
										break;
								}
            }
				}

		    //std::this_thread::sleep_for(std::chrono::milliseconds(1)); // let everyone get started

		}




		WorkerStatus * ws = workers[0];
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		//std::this_thread::sleep_for(std::chrono::seconds(1));


		masterworker::EmptyMsg request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		grpc::Status status = ws->stub->Finish(&context, request, &reply);

		if (status.ok())
		{
				//ws->state = reply.response();
				return false;
		}
		else
		{
				std::cerr << " - Finish not okay!\n";
				std::cerr << " - status is " << (status.error_code()) << "\n";
				std::cerr << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();
		}



		return true;
}




void Master::TryToAssignReduceToWorker(WorkerStatus * ws)
{
			std::cerr << "Attempting to send reduce to worker.\n";

			ReduceTask * rt = NULL;

			// attempt to find a shard that this worker has not already tried
			for(int i = 0; i < reduceTasks.size(); i++)
			{
					ReduceTask * temp = &(reduceTasks[i]);

					if(temp->state != REDUCE_STATE_UNASSIGNED)
					{
						  continue;
					}

          bool foundit = false;

					for (int it = 0; it < temp->workersThatAttemptedThis.size(); it++)
					{
						  if (temp->workersThatAttemptedThis[it] == ws->workerID)
							{
									std::cerr << "Found a reduce to send.\n";
								  foundit = true;
									rt = temp;
									break;
							}
					}

					// ??????
					if ( foundit == false )
					{
							std::cerr << "Sending reduce to worker.\n";
							SendReduceToWorker(ws, temp);
							return;
					}
			}

}



void Master::SendReduceToWorker(WorkerStatus * ws, ReduceTask * rt)
{
		std::cerr << "Sending reduce " << rt->reduceID << "\n";


		masterworker::ReduceSubset  request;
		masterworker::Ack        reply;

		// write the name to the request
		request.set_subset(rt->reduceID);
		std::cerr << "Sending reduce subset " << std::to_string(rt->reduceID) << "\n";

		grpc::ClientContext context;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		grpc::Status status = ws->stub->Reduce(&context, request, &reply);

		if (status.ok())
		{
				struct timeval tv;
				gettimeofday(&tv, NULL);

				ws->state = STATUS_CODE_REDUCE_WORKING;
				ws->assignedTime = tv.tv_sec;
				ws->assignedReduce = rt;
				ws->slowWorkerFlags = WORKER_FLAGS_NORMAL;

				rt->state = REDUCE_STATE_ASSIGNED;

				return;
		}
		else
		{
				std::cerr << " - SendReduceToWorker not okay!\n";
				std::cerr << " - status is " << (status.error_code()) << "\n";
				std::cerr << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();
		}

}


void Master::SendWriteReduceToFile(WorkerStatus * ws)
{
		std::cerr << "Sending write reduce to file\n";

		masterworker::EmptyMsg request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		grpc::Status status = ws->stub->WriteReduceFile(&context, request, &reply);

		if (status.ok())
		{
				//ws->state = reply.response();
				return;
		}
		else
		{
				std::cerr << " - SendWriteReduceToFile not okay!\n";
				std::cerr << " - status is " << (status.error_code()) << "\n";
				std::cerr << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();
		}
}


void Master::SendDropReduceToFile(WorkerStatus * ws)
{
		//std::cerr << "Sending drop reduce to file\n";

		masterworker::EmptyMsg request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		grpc::Status status = ws->stub->DiscardReduceResults(&context, request, &reply);

		if (status.ok())
		{
				return;
		}
		else
		{
				std::cerr << " - SendDropReduceToFile not okay!\n";
				std::cerr << " - status is " << (status.error_code()) << "\n";
				std::cerr << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();
		}
}





void Master::SendShardRPCToWorker(WorkerStatus * ws, FileShard * shard)
{
	 	//std::cerr << "Sending shard " << shard->shardID << "\n";


		masterworker::ShardInfo  request;
		masterworker::Ack        reply;

		// write the name to the request
		request.set_filename(shard->fileName);
		request.set_offset(shard->offset);
		request.set_shardsize(shard->shardSize);
		//std::cerr << "Sending shardID " << std::to_string(shard->shardID) << "\n";
		request.set_shardid(shard->shardID);

		grpc::ClientContext context;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		grpc::Status status = ws->stub->MapShard(&context, request, &reply);

		if (status.ok())
		{
				// reset this flag
				ws->slowWorkerFlags = WORKER_FLAGS_NORMAL;
				ws->state = reply.response();

			  struct timeval tv;
				gettimeofday(&tv, NULL);

				ws->state = STATUS_CODE_MAP_WORKING;
				ws->assignedTime = tv.tv_sec;
				ws->assignedShard = shard;

				shard->state = FILE_SHARD_STATE_ASSIGNED;

				return;
		}
		else
		{
				std::cerr << " - SendShardRPCToWorker not okay!\n";
				std::cerr << " - status is " << (status.error_code()) << "\n";
				std::cerr << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();

		}

}


void Master::SendPingRPCToWorker(Master::WorkerStatus * ws)
{
		//std::cerr << "Sending ping to worker\n";

		masterworker::EmptyMsg request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		grpc::Status status = ws->stub->Ping(&context, request, &reply);

		if (status.ok())
		{
				ws->state = reply.response();

				struct timeval tv;
				gettimeofday(&tv, NULL);

				int duration = tv.tv_sec - ws->assignedTime;

				if (duration > 1 && (ws->state == STATUS_CODE_MAP_WORKING || ws->state == STATUS_CODE_REDUCE_WORKING))
				{
					  std::cerr << "Worker flaged as slow!";
						ws->slowWorkerFlags = WORKER_FLAGS_SLOW;
				}

				return;
		}
		else
		{
				std::cerr << " - SendPingRPCToWorker not okay!\n";
				std::cerr << " - status is " << (status.error_code()) << "\n";
				std::cerr << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();

		}

}



void Master::SendWorkerInfoToWorker(int i, WorkerStatus *ws, int numberOfShardsTotal)
{
		std::cerr << "sending worker info to worker\n";

		masterworker::WorkerInfo request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		request.set_workerid(i);
		request.set_outputdirectory(mr_spec->outputDir);
		request.set_numberofworkers(mr_spec->numberOfWorkers);
		request.set_numberoffiles(mr_spec->numberOfOutputFiles);

	  request.set_desiredshardsize(mr_spec->desiredShardSize);
	  request.set_numberofshardstotal(numberOfShardsTotal);

		std::stringstream inputFiles;
		for (int i = 0; i < mr_spec->inputFiles.size(); i++)
		{
			  inputFiles << mr_spec->inputFiles[i] << " ";
		}

		request.set_inputfiles(inputFiles.str());

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		grpc::Status status = ws->stub->SetWorkerInfo(&context, request, &reply);

		if (status.ok())
		{
		  	return;
		}
		else
		{
		  	std::cerr << " - SendWorkerInfoToWorker not okay!\n";
		  	std::cerr << " - status is " << (status.error_code()) << "\n";
		  	std::cerr << " - status msg is " << (status.error_message()) << "\n";
		}

}





void Master::SendWriteIntermediateToFile(WorkerStatus * ws)
{
		//std::cerr << "Sending write intermediate file\n";

		masterworker::EmptyMsg request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);

		grpc::Status status = ws->stub->WriteShardToIntermediateFile(&context, request, &reply);

		if (status.ok())
		{
				//ws->state = reply.response();
				return;
		}
		else
		{
				std::cerr << " - SendWorkerInfoToWorker not okay!\n";
				std::cerr << " - status is " << (status.error_code()) << "\n";
				std::cerr << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();
		}

}



void Master::SendDropIntermediateToFile(WorkerStatus * ws)
{
		//std::cerr << "Sending drop intermediate file\n";

		masterworker::EmptyMsg request;
		masterworker::Ack        reply;

		grpc::ClientContext context;

		std::chrono::time_point<std::chrono::system_clock> deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
		context.set_deadline(deadline);
				
		grpc::Status status = ws->stub->DiscardShardResults(&context, request, &reply);

		if (status.ok())
		{
				return;
		}
		else
		{
				std::cerr << " - SendWorkerInfoToWorker not okay!\n";
				std::cerr << " - status is " << (status.error_code()) << "\n";
				std::cerr << " - status msg is " << (status.error_message()) << "\n";
				ws->state = reply.response();
		}

}

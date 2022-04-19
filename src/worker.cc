#include "worker.h"

/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {

		ipAndPort = ip_addr_port;

		/*statusCode = 0;
		*/


}


/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/

bool Worker::run()
{
		std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory("cs6210");
		std::shared_ptr<BaseReducer> reducer = get_reducer_from_task_factory("cs6210");

		SyncWorker syncWorker(ipAndPort, mapper, mapper->impl_, reducer, reducer->impl_);

		ServerBuilder builder;

		builder.AddListeningPort(ipAndPort, grpc::InsecureServerCredentials());
		builder.RegisterService(&syncWorker);

		std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

		server->Wait();




/*


		builder.AddListeningPort(ipAndPort, InsecureServerCredentials());
		builder.RegisterService(&asyncService);

		completionQueue = builder.AddCompletionQueue();
		server = builder.BuildAndStart();

		new MapperShardCallData(&asyncService, completionQueue.get(), this);
		new PingCallData(&asyncService, completionQueue.get(), this);
    new SetWorkerInfoCallData(&asyncService, completionQueue.get(), this);
    new WriteShardToIntermediateFileCallData(&asyncService, completionQueue.get(), this);
    new DiscardShardResultsCallData(&asyncService, completionQueue.get(), this);

		void *tag;
		bool ok;

		while (true)
		{
		    // Block waiting to read the next event from the completion queue. The
		    // event is uniquely identified by its tag, which in this case is the
		    // memory address of a CallData instance.
		    completionQueue->Next(&tag, &ok);
		    GPR_ASSERT(ok);

				// todo - reducer calldata needs to be subclass
				// calldata
				//   - mappercalldata
				//   - reducercalldata

		    static_cast<CallDataBase*>(tag)->Proceed();
	  }
*/

/*
	std::cerr << "worker.run(), I 'm not ready yet" <<std::endl;
	auto mapper = get_mapper_from_task_factory("cs6210");
	mapper->map("I m just a 'dummy', a \"dummy line\"");
*/


/*
	auto reducer = get_reducer_from_task_factory("cs6210");
	reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
*/

	return true;
}

/*
void Worker::Map(std::string s)
{
	  mapper->map(s);
}


void Worker::SetupBaseMapperImpl(std::string outputDir)
{

}
*/
/*
void Worker::SetStatusCode(int i)
{
    worker->SetStatusCode(i);
}*/

#pragma once

#include <thread>

#include "CallDataBase.h"

class MapperShardCallData : public CallDataBase {
public:
		MapperShardCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * worker);
		void StartWorkerMapThread();
		FileShard GetFileShard();
		virtual void Proceed();
    //void Finish();

	private:
			masterworker::ShardInfo request_;
			masterworker::Ack reply_;

			std::thread t;

	    // The means to get back to the client.
	    grpc::ServerAsyncResponseWriter<masterworker::Ack> responder_;
};

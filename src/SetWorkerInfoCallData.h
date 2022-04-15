#pragma once

#include "CallDataBase.h"

class SetWorkerInfoCallData : public CallDataBase {
public:

		SetWorkerInfoCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * worker);

		void Proceed();
		void Finish();

	private:
			masterworker::WorkerInfo request_;
			masterworker::Ack reply_;

	    // The means to get back to the client.
	    grpc::ServerAsyncResponseWriter<masterworker::Ack> responder_;

};

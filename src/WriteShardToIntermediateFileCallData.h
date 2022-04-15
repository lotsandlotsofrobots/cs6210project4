#pragma once

#include "CallDataBase.h"

class WriteShardToIntermediateFileCallData : public CallDataBase {
public:

		WriteShardToIntermediateFileCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * worker);

		void Proceed();
		void Finish();

	private:
			masterworker::EmptyMsg request_;
			masterworker::Ack reply_;

	    // The means to get back to the client.
	    grpc::ServerAsyncResponseWriter<masterworker::Ack> responder_;

};

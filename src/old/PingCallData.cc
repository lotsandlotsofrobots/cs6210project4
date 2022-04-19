#include "PingCallData.h"
#include "worker.h"

PingCallData::PingCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * w)
		: responder_(&ctx_)
{
		service_ = service;
		cq_ = cq;
		status_ = CREATE;
		worker = w;


		std::cerr << "Requesting ping.\n";
		 // Invoke the serving logic right away.
		Proceed();
}


void PingCallData::Proceed()
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
				new PingCallData(service_, cq_, worker);

				std::cerr << "Responding to ping!\n";

				status_ = FINISH;
				reply_.set_response(worker->GetStatusCode());
				responder_.Finish(reply_,  grpc::Status::OK, this);
		}
		else
		{
				GPR_ASSERT(status_ == FINISH);
				// Once in the FINISH state, deallocate ourselves (CallData).
				delete this;
		}

}
/*
void PingCallData::Finish()
{
	std::cerr << "Worker done with this shard.\n";
	status_ = FINISH;
	responder_.Finish(reply_, grpc::Status::OK, this);
}
*/

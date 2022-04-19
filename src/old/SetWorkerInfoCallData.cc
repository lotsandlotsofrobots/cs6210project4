#include "SetWorkerInfoCallData.h"
#include "worker.h"

SetWorkerInfoCallData::SetWorkerInfoCallData(MapperReducer::AsyncService* service, ServerCompletionQueue* cq, Worker * w)
    : responder_(&ctx_)
{
    service_ = service;
    cq_ = cq;
    status_ = CREATE;
    worker = w;


    std::cerr << "Requesting set worker info.\n";
     // Invoke the serving logic right away.
    Proceed();
}


void SetWorkerInfoCallData::Proceed()
{
		if (status_ == CREATE)
		{
				status_ = PROCESS;

				// As part of the initial CREATE state, we *request* that the system
				// start processing SayHello requests. In this request, "this" acts are
				// the tag uniquely identifying the request (so that different CallData
				// instances can serve different requests concurrently), in this case
				// the memory address of this CallData instance.
				service_->RequestSetWorkerInfo(&ctx_, &request_, &responder_, cq_, cq_, this);
		}
		else if (status_ == PROCESS)
		{
				// Spawn a new CallData instance to serve new clients while we process
				// the one for this CallData. The instance will deallocate itself as
				// part of its FINISH state.
				new SetWorkerInfoCallData(service_, cq_, worker);

				std::cerr << "Responding to worker info!\n";

        worker->SetWorkerID(request_.workerid());
        worker->SetOutputDirectory(request_.outputdirectory());
        worker->SetNumberOfWorkers(request_.numberofworkers());
        worker->SetNumberOfFiles(request_.numberoffiles());

        worker->SetupMapper();

				status_ = FINISH;
				reply_.set_response(1);
				responder_.Finish(reply_,  grpc::Status::OK, this);
		}
		else
		{
				GPR_ASSERT(status_ == FINISH);
				// Once in the FINISH state, deallocate ourselves (CallData).
				delete this;
		}

}

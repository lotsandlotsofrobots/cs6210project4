#include "CallDataBase.h"
#include "worker.h"

/******************************************************************************
**
** MapperCallData implementation
**
******************************************************************************/

void CallDataBase::Map(std::string s)
{
	  worker->Map(s);
}

void CallDataBase::Reduce()
{

}

int CallDataBase::GetWorkerID()
{
		return worker->GetWorkerID();
}

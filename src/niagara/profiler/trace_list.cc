#include <trace_list.h>
#include <np_consts.h>
#include <np_funcs.h>
#include <niag_profiler.h>

extern Niag_Profiler profiler;

Trace_List::Trace_List(const Method_List* const _methodList, int _threadId) {
  numTraces = 0;
  numAlloc = 30;
  traceList = new List_Elem*[numAlloc];
  methodList = _methodList;
  threadId = _threadId;
  mostRecentTrace = -1;
}

Trace_List::~Trace_List() {
  for(int i = 0; i<numTraces; i++) {
    delete traceList[i]->trace;
    delete traceList[i];
  }
  delete []traceList;
}

void Trace_List::addAlloc(JVMPI_CallTrace* trace, int bytes, char* threadName) {
  // see if we can find a matching trace
  for(int i = 0; i<numTraces; i++) {
    if(traceEquals(trace, traceList[i]->trace) == NP_TRUE) {
      traceList[i]->memAllocd += bytes;
      traceList[i]->numAllocs++;
      mostRecentTrace = i;
      return;
    }
  }

  // have new trace
  checkSpace();
  traceList[numTraces] = new List_Elem();
  traceList[numTraces]->trace = copyTrace(trace); 
  traceList[numTraces]->memAllocd = bytes;
  traceList[numTraces]->freedMem = 0;
  traceList[numTraces]->traceNum = numTraces;
  traceList[numTraces]->numAllocs = 1;
  mostRecentTrace = numTraces;
  numTraces++;
}

void Trace_List::print(ostream& os, char* threadName) {
  cout << "printing traces " << numTraces << " thr:" << threadId << endl;
  sortTraces();
  for(int i = 0; i<numTraces && traceList[i]->memAllocd >= 5000; i++) {
    os << "   TRACE:" << traceList[i]->traceNum 
       << "  Allocd: " << traceList[i]->memAllocd << " (bytes)" 
       << " Live: " << traceList[i]->memAllocd - traceList[i]->freedMem << " (bytes)"
       << " Num Allocs: " << traceList[i]->numAllocs
       << endl;
      printTrace(traceList[i]->trace, os);
    }
}

int Trace_List::getTotalMem() {
  int totalMem = 0;
  for(int i = 0; i<numTraces; i++) {
    totalMem += traceList[i]->memAllocd;
  }
  return totalMem;
}

int Trace_List::getLiveMem() {
  int liveMem = 0;
  for(int i = 0; i<numTraces; i++) {
    liveMem += traceList[i]->memAllocd;
    liveMem -= traceList[i]->freedMem;
  }
  return liveMem;
}

void Trace_List::resetData() {
  for(int i = 0; i<numTraces; i++) {
    delete traceList[i]->trace;
    delete traceList[i];
    traceList[i] = NULL;
  }
  numTraces = 0;
}

void Trace_List::processFreedObjList() {
  profiler.getFreedObjListLatch();
  profiler.getAllocObjListLatch();

  int freeListSize = profiler.getFreedListSize();
  for(int i = 0; i<freeListSize; i++) {
    Obj_Info* freedObjInfo = NULL;
    jobjectID id = profiler.getFreedObjId(i);
    freedObjInfo = profiler.getObjInfo(id);
    if(freedObjInfo != NULL && freedObjInfo->threadNum == threadId) {
      if(traceList[freedObjInfo->traceNum] == NULL)
	cout << "HELP null trace num " << freedObjInfo->traceNum << 
	  "num traces " << numTraces << " thread num " << threadId << endl;
      traceList[freedObjInfo->traceNum]->freedMem +=
	freedObjInfo->objSize;
      profiler.removeFreedObj(i);
      profiler.removeAllocdObj(id);
    }
  }

  profiler.releaseAllocObjListLatch();
  profiler.releaseFreedObjListLatch();
}

int Trace_List::getMostRecentTrace() {
  if(mostRecentTrace >= numTraces) {
    cout << "NUMTRACES " << numTraces << " most recent " << mostRecentTrace << endl;
    barf("bad mostRecentTrace");
  }
    
  return mostRecentTrace;
}

// --------------------- PRIVATE FUNCTIONS ----------------------------

void Trace_List::printTrace(const JVMPI_CallTrace* const trace, ostream& os) {
  for(int i = 0; i<trace->num_frames; i++) {
    os << "      " << i+1 << ") ";
    const Method_Info* mInfo = methodList->getMethodInfo(trace->frames[i].method_id);
    if(mInfo == NULL) {
      os << "UNKNOWN M:" << trace->frames[i].method_id << " L:";
      os << trace->frames[i].lineno << endl;
    } else {
      os << mInfo->sourceFile << "::" << mInfo->name;
      os << " (line:" << trace->frames[i].lineno << ")" << endl;
    }
  }
}


int Trace_List::traceEquals(const JVMPI_CallTrace* const trace1,
			    const JVMPI_CallTrace* const trace2) {
  if(trace1->num_frames != trace2->num_frames)
    return NP_FALSE;

  for(int i = 0; i<trace1->num_frames; i++) {
    if(trace1->frames[i].lineno != trace2->frames[i].lineno)
      return NP_FALSE;
    if((long)trace1->frames[i].method_id != (long)trace2->frames[i].method_id)
      return NP_FALSE;
  }
  return NP_TRUE;
}

JVMPI_CallTrace* Trace_List::copyTrace(const JVMPI_CallTrace* const trace) {
  JVMPI_CallTrace* newTrace = new JVMPI_CallTrace();
  newTrace->frames = new JVMPI_CallFrame[trace->num_frames];
  newTrace->num_frames = trace->num_frames;
  for(int i = 0; i<trace->num_frames; i++) {
    newTrace->frames[i].lineno = trace->frames[i].lineno;
    newTrace->frames[i].method_id = trace->frames[i].method_id;
  }
  return newTrace;
}

void Trace_List::checkSpace() {
  if(numTraces < numAlloc)
    return;
  
  int oldSize = numAlloc;
  numAlloc *= 2;
  
  List_Elem** newTraceList = new List_Elem*[numAlloc];

  for(int i = 0; i<oldSize; i++) {
    newTraceList[i] = traceList[i];
  }
  delete []traceList;
  traceList = newTraceList;
}

void Trace_List::sortTraces() {
  insertionsort(traceList, 0, numTraces-1);
  //  quicksort(traceList, 0, numTraces-1);
}

void Trace_List::quicksort(List_Elem** array, int startIdx, int endIdx) {
  List_Elem* temp; // for swap
  
  // if elements to sort small - use insertion or bubble sort
  if((endIdx-startIdx) < 10) { 
    insertionsort(array, startIdx, endIdx);
    return;
  }
  
  // else partition around median
  int median = (endIdx+1-startIdx)/2 + startIdx;
  int partVal = array[median]->memAllocd; // partition value
  
  int l = startIdx;
  int r = endIdx;
  
  // partition
  while(l < r) { 
    while(array[l]->memAllocd >= partVal) {
      l++;
      if(l==r)
	break;
    }
    if(l<r) {
      while(array[r]->memAllocd <= partVal) {
	r--;
	if(l==r)
	  break;
      }
    }
    
    if(l >= r) 
      break;
    
    // swap
    temp = array[l];
    array[l] = array[r];
    array[r] = temp;
    l++;
    r--;
  }

  if(l==startIdx || l == endIdx) { // partition did not make any progress
    insertionsort(array, startIdx, endIdx);
  } else {
    if(l == r) {
      if(array[l]->memAllocd <= partVal)
	l++;
      else
	r--;
    } 
    quicksort(array, startIdx, r);
    quicksort(array, l, endIdx);
  }

}

void Trace_List::insertionsort(List_Elem** array, int startIdx, int endIdx) {
  if(startIdx >= endIdx) 
    return;

  for(int toIns = startIdx+1; toIns<=endIdx; toIns++) {
    List_Elem* toInsert = array[toIns];
    int emptySlot = toIns;
    for(; emptySlot>0; emptySlot--) {
      if(toInsert->memAllocd > array[emptySlot-1]->memAllocd) 
	array[emptySlot] = array[emptySlot-1];
      else 
	break;
    }
    array[emptySlot] = toInsert;
  }
}



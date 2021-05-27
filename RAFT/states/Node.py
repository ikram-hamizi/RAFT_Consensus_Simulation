from time import sleep

class Node():
    
    def __init__(self, ID):

        self.LOG = []
        self.ID = ID
        self.TermNumber = 0
        self.commitIndex = 1
        self.votedFor = None #currentLeader
        self.heartbeat = True
        self.timeout = None
        self.followers = None

    def _compareWithMyLOG(self, incomingLog):
        #compare the 2 unique identifiers of a log
        TermNumber_1  = incomingLog.split("#Term=")[1].split(",")[0]
        commitIndex_1 = incomingLog.split("#commitIndex=")[1].split(":")[0]
        #if self.TermNumber 
        #self.TermNumber, self.commitIndex    
        
    
    def print_node(self, print_fn = True, additional=""):
        
         
        info = f"ID = {self.ID} | TermNumber = {self.TermNumber} | followers = {len(self.followers)} | {additional}"
        
        if print_fn:
            print(info)
        else:
            return info
        

    """
    def AppendEntries(self, RPC='replicatelog'):
                        
        if RPC == 'replicatelog':
            responses = _replicate_logs(self.LOG, self.commitIndex, super.TermNumber)
        
        return responses
    
    
    def printLog(self, last=False):
    	if last:
    	    return super.LOG[commitIndex]
    	
    	return super.LOG[:self.commitIndex+1]
    
           
    def _write_to_LOG(self, command, entry):
        #1. write to own LOG
    	super.LOG[commit_index, super.TermNumber]
    	self.commitIndex += 1
    	
    	#2. send to FOLLWOERS to replicate
    	responses = __AppendEntries(RPC='replicatelog')
    	
    	#3. Find majority responses
    	ch = Counter(responses) #count number of True and False
    	
    	#4. decide to commit or not
    	if votes[True]<votes[False]:
    	    #if majority did not change their LOG,
    	    #then undo the LEADER LOG changes
    	    self.commit_index -= 1
    	
    	printLog(last=True)
        """

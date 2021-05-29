import LogEntry
class Follower():
    
    def __init__(self, ID):
    
        self.TermNumber = 0
        self.votedFor = None
        self.LOG = [LogEntry("",None,0,0)] #save LOG objects
        self.logLength = 0
        
        self.lastTerm = None
        self.state = 'Follower'
        
        self.commitLength = 0 #PDF
        self.currentLeader = None #PDF # x 
        
        #they map nodeID to int
        self.sentLength  = {} #PDF
        self.ackedLength = {} #PDF
        
    def on_recovery_from_crash(self):
        self.currentRole = 'Follower'
        self.votedFor = None
        self.currentLeader = None #PDF # x 
        self.sentLength  = [] #PDF
        self.ackedLength = [] #PDF
        
            
    def on_election_timeout(self, Followers):
        
        self.votedFor = self
        self.TermNumber = self.TermNumber + 1
        self.state = 'Candidate'
        self.votedFor = self.ID
        
        lastLogTerm = 0 #PDF
        
        if self.commitIndex > 0:
            # At initalization of the network, lastTerm is 0
            lastLogTerm = self.LOG[self.logLength-1].get_term()
                
        self.message = (RequestVotes(), self, self.logLength, self.lastTerm)
        for follower in self.followers:
            follower.on_receive_message(message)
            
        
    def on_receiving_votes(voter, v_term, vote):
        if ...:
            self.state = 'Leader'
            self.myLeader = self
            
            for follower in self.followers:
                self.ackdLength[follower] = self.logLength #length of the log that we think we've sent to this follower
                self.ackedLength[follower] = 0 #length of the log of entries acknowledged by the follower to have been received
                self.ReplicateLog(self, follower)
            
        
        
        elif v_term > self.TermNumber:
            self.TermNumber = v_term
            self.state = 'Follower'
            self.votedFor = None
            
    #      Leader                    (leaderID, currentTerm, i,          prevLogTerm, commitLength, entries)
    #      Follwr                    (leaderID, term,        logLength,  logTerm,     leaderCommit, entries)   
    def on_receiving_log_request(self, Leader, L_TermNumber, my_logLength_from_L, L_commitLength, L_prevLogTerm, newEntries):
    
        if L_TermNumber > self.TermNumber:
            self.TermNumber = L_TermNumber
            self.votedFor = Leader
        
            
        logAssert  = (self.logLength >=  my_logLength_from_L)
        if logAssert and my_logLength_from_L>0:
            logAssert = (L_prevLogTerm == self.LOG[my_logLength_from_L-1].TermNumber)
        
        if L_TermNumber == self.TermNumber and logAssert:
            self.state = 'Follower'
            self.votedFor = Leader
            
            AppendToLog(my_logLength_from_L, L_commitLength, entries)
            ack = my_logLength_from_L + len(entries)
            Leader.on_receive()
            
            
            
            
            
            
            
        
        

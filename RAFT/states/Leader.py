#import python libraries
from collections import Counter
import time
from time import sleep
import threading
import datetime 

#import local modueles
from classes.LogEntry import LogEntry
from states.Follower import Follower
from states.Node import Node



class Leader(Node):
	    	
    def __init__(self, ID):
        super().__init__(ID)
        self.nextIndex = None
        self.candidate_object = None
        self.heartbeat_timeout = 1 #50*1e-3
        self.shutdown_counter = 3 #SIMULATION      
        
    
    ################
    # STATIC METHODS
    ################
    @staticmethod
    def become_leader(candidate_object):
    
        candidate_object.state = 'Leader'
        leader = Leader(candidate_object.ID)
        leader.candidate_object = candidate_object
        # Copy my information from when I was a Follower/Candidate
        leader.nextIndex = 2
        leader.LOG = candidate_object.LOG
        leader.TermNumber = candidate_object.TermNumber
        leader.followers = candidate_object.followers
        leader.votedFor = candidate_object.votedFor
        leader.commitIndex = candidate_object.commitIndex
        
        # Debug Printing
        leader.lastTimeStamp, leader.lastTimeStampString = leader.get_timestamp()
        leader.print_node(leader.lastTimeStampString) #PRINT
        
        del candidate_object
        
        return leader
    ################
        
           
    ###################
    # PRIVATE FUNCTIONS
    ###################    
    def _leader_on(self):

        #1. Now I am a LEADER,
        #   I periodically send hearbeats to Followers
        while True:
        
            '''SIMULATE SHUTDOWN OF LEADER'''
            print(f"------ COUNTER={self.shutdown_counter}")
            self.shutdown_counter -= 1
            if self.shutdown_counter == 0:
            
                print("LEADER IS DEAD")
                self.leader_on_death()
                return
                
            
            print("\n------------------------------\n"\
                    "📤 Sending HEARTBEAT RPCs 💓!" )
            self.AppendEntries(RPC='heartbeat') #💓
            sleep(self.heartbeat_timeout) 
        
    ###################        
    
             
    ###########################
    # LIST OF CLIENT OPERATIONS
    ###########################
    def incrementY(self, flag):
        flag = (flag[0], flag[1]+1)
        return flag
        
    def sum(self, data):
        # Example

        return None
        
    def sub(self, data):
        # Example
        return None
    ###########################     
    
    
    ##################
    # PUBLIC FUNCTIONS 
    ##################
    def leader_on_death(self):
       """
       # Copy info from my previous state as a Leader to Follower object
       Return: Follower object
       """
       self.candidate_object.become_follower(self)

        

    def leader_begin(self):
        infinite_heartbeats = threading.Thread(target=self._leader_on, name = "Thread Leader ❤️")
        infinite_heartbeats.start()
         
    def leader_on_receive_request(self, flag, command='incrementY', reps=100):
               
        if command == 'incrementY':
            function = incrementY
            
        for _ in range(reps):
            #1. LEADER makes the change request from CLIENT
            
            entry = function(flag)
            sleep(3) # For simulation
            print("----------------------\n"\
                 f"entry now is = {entry}...\n"\
                  "----------------------")
            
            """
            #2. LEADER adds changes to LEADER's log
            self.commitIndex += 1
            newLogEntry = LogEntry(command, entry, self.TermNumber, self.commitIndex)
            self.LOG.append(newLogEntry)
            
            #3. LEADER replicates the logs to FOLLOWERs: send AppendEntries RPCs 
            commit_indexOfThePrecedingEntry = self.commitIndex - 1
            TermNumberOfThePrecedingEntry = self.TermNumber - 1
            
            responses = self.AppendEntries(RPC='replicatelog',
                    newLogEntry=newLogEntry,
                    commit_indexOfThePrecedingEntry=commit_indexOfThePrecedingEntry,
                    TermNumberOfThePrecedingEntry=TermNumberOfThePrecedingEntry)
            
            #4. LEADER checks if the entry is committed,
            # i.e. if majority of nodes have written the entry
            max_response = Counter(responses).most_common()[0]
            
            if max_response == True:
            # entry is committed
            # The cluster has now come to consensus about the system state.
            # inform nodes to commit?
            
            
            else:
            # entry not committed
            # cluster is not on consensus
            # roll back
                self.commitIndex -= 1
            
            """
        # committed entry
        flag = entry
        return flag
        
    def AppendEntries(self, RPC='heartebeat', newLogEntry=None, commit_indexOfThePrecedingEntry=None, TermNumberOfThePrecedingEntry=None):
        """
        args:
            followers: nodes
            newLogEntry
            commit_indexOfThePrecedingEntry
            TermNumberOfThePrecedingEntry
        return: FOLLOWERS and their responses (True or False)
        """
        
        # OPTION (1): REPLICATE LOGS TO FOLLOWERS
        def _replicate_logs(newLogEntry, commit_indexOfThePrecedingEntry, TermNumberOfThePrecedingEntry):
        	
            #Get rejections or acceptances of the RPC        	
        	RPC_responses = []
        	for follower in self.followers:
        	    RPC_responses.append(follower.replicate_from_leader(
        	                                            newLogEntry,
        	                                            commit_indexOfThePrecedingEntry,
        	                                            TermNumberOfThePrecedingEntry))
        	
        	
        	return RPC_responses         	
    	
    	# OPTION (2): SEND HEARTBEATS TO FOLLOWERS
        def _send_heartbeat():
            RPC_responses = []
            print(f"\n*===============================================================================\n"\
                    f"[+] I am Leader [{self.ID}], I will send RPCs to my {len(self.followers)} followers."\
                    f"\n*==============================================================================\n")
            for follower in self.followers:
                RPC_responses.append(follower.on_receive_heartbeat(self, self.TermNumber))
            
            return RPC_responses
        
        """
        CHOOSE RPC OPTION (1) or OPTION (2)
        """
        
        if RPC == 'heartbeat':
            responses = _send_heartbeat()
        
        if RPC == 'replicatelog':
             
            responses = _replicate_logs(newLogEntry=newLogEntry,
                    commit_indexOfThePrecedingEntry=commit_indexOfThePrecedingEntry,
                    TermNumberOfThePrecedingEntry=TermNumberOfThePrecedingEntry)
        
        # responses: rejections or acceptances of the RPC [True/False]
        return responses
        
        
 
    	    
    

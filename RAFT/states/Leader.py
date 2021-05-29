#import python libraries
from collections import Counter
import time
from time import sleep
import threading
import datetime 
import math 

#import local modueles
from classes.Colors import Colors
from classes.LogEntry import LogEntry
from states.Node import Node



class Leader(Node):
	    	
    def __init__(self, ID, verbose=True):
        super().__init__(ID)
        self.candidate_object = None
        self.heartbeat_timeout = 1 #50*1e-3
        self.shutdown_counter = 3 #SIMULATION   
        self.idle = True
        self.my_followers = None
        self.verbose = verbose
        
    
    ################
    # STATIC METHODS
    ################
    @staticmethod
    def become_leader(candidate_object):
    
        leader = Leader(candidate_object.ID)
        leader.candidate_object = candidate_object
        
        # Copy my information from when I was a Follower/Candidate
        leader.commitLength = candidate_object.commitLength
        leader.LOG = candidate_object.LOG
        leader.verbose = candidate_object.verbose
        leader.TermNumber = candidate_object.TermNumber
        leader.followers = candidate_object.followers
        
        # Exclude self from the list of voters
        my_followers = leader.followers[:leader.ID-1]
        if leader.ID < len(leader.followers):
            my_followers += leader.followers[leader.ID:]
        leader.my_followers = my_followers  
        
        leader.votedFor = candidate_object.votedFor
        leader.Leader = leader
        
        leader.sentLength = candidate_object.sentLength
        leader.ackdLength = candidate_object.ackdLength
        
        # Debug Printing
        leader.lastTimeStamp, leader.lastTimeStampString = leader.get_timestamp()
        leader.print_node(leader.lastTimeStampString) #PRINT
        
       
        del candidate_object
        
        return leader
    ################
        
           

    
    ##################
    # PUBLIC FUNCTIONS 
    ##################
    def leader_on_death(self):
       """
       # Copy info from my previous state as a Leader to Follower object
       Return: Follower object
       """
       
       leader_index = self.Leader.ID - 1
       self.followers[leader_index] = self.candidate_object
       self.candidate_object.become_follower_again(self)
       
       return self.candidate_object


    def leader_begin(self):
        infinite_heartbeats = threading.Thread(target=self._leader_on, name = "Thread Leader ❤️ + 📑️")
        infinite_heartbeats.start()
         
    def on_receive_CLIENT_request(self, flag, command='incrementY', reps=100):
        self.idle = False
               
        if command == 'incrementY':
            function = self.incrementY
        
        entry = flag   
        for _ in range(reps):
            #1. LEADER makes the change request from CLIENT
            
            entry = function(entry)
            if self.verbose:
                sleep(1) # For simulation
            
            if self.verbose:
                print("--------------------------------------\n"\
                      "✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦LOG✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦\n"\
                     f"entry now is = {entry}...\n"\
                      "✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦✦\n"\
                      "--------------------------------------\n")
            
            #2. LEADER SAVES THIS TO LOG
            newLogEntry = LogEntry(command, entry, self.TermNumber, self.commitLength) #, self.commitIndex)
            self.LOG.append(newLogEntry)
            self.ackdLength[self] = len(self.LOG)  
            
            #3. LEADER replicates the entry
            self.send(RPC='REPLICATELOG')
        
                
        flag = entry
        self.idle = True     
        return flag
     
    def replicate_log(self, follower):
    
        ids = [k.ID for k,v in self.sentLength.items()]
        
        if self.verbose:
            print(f"\n*[ID={self.ID}]* I AM REPLICATING LOG")
            print(f"📤️ Append Log to follower [{follower.ID}]. "\
                  f"(Followers={ids})")
              
        f_logLength_from_L = self.sentLength[follower]
        newEntries = self.LOG[f_logLength_from_L:len(self.LOG)]
        
        # Get Term Number of the latest Log Entry SENT to the Follower
        prevLogTerm = 0 
        if f_logLength_from_L>0: 
            prevLogTerm = self.LOG[f_logLength_from_L-1].TermNumber
        
        # (leaderID, currentTerm, i, prevLogTerm, commitLength, entries)
        follower.receive(RPC='REPLICATELOG',
                         sender=self, newEntries=newEntries,    sender_TermNumber=self.TermNumber,
                         sender_logLength=None,                 f_logLength_from_s=f_logLength_from_L,
                         sender_commitLength=self.commitLength, sender_prevLogTerm=prevLogTerm, vote=None)
     
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
    # RECEIVE FUNCTION
    ##################
    def receive(self, RCP, follower, follower_TermNumber, ack, appended):
        
        if RCP == 'LogResponse':
            if follower_TermNumber == self.TermNumber and self.state == 'Leader':
            
                if appended == True and ack >= ackdLength[follower]:
                    self.sentLength[follower] = ack
                    self.ackdLength[follower] = ack
                    self._CommitLogEntries()
                
                elif self.sentLength[follower] > 0:
                    self.sentLength[follower] -= 1
                    #consistency check
                    #retry with one entry earlier in the LOG (send extera entries)
                    #to fill the GAP between Follower and Leader
                    self.replicate_log(follower)
            
            elif follower_TermNumber > self.TermNumber:
                self.state = 'Follower'
                self.votedFor = None
                self.TermNumber = follower_TermNumber
                
                # Copy my information to my original Follower state
                # AND REPLACE the old Follower object
                self.leader_on_death()
            
                
    #################
    #  SEND  FUNCTION
    #################
    def send(self, RPC='heartebeat'):
        """
        args:
            followers: nodes
            newLogEntry
            commit_indexOfThePrecedingEntry
            TermNumberOfThePrecedingEntry
        return: FOLLOWERS and their responses (True or False)
        """
        if self.verbose:
            print(f"\n*=======================================================*\n"\
                  f"[+] I am Leader [{self.ID}], I will send RPCs to my {len(self.my_followers)} followers."\
                  f"\n*=======================================================*")
        if RPC == 'heartbeat':
            self._send_heartbeat_to_all()
        
        if RPC == 'REPLICATELOG': 
            self._replicate_log_to_all()
        
        # responses: rejections or acceptances of the RPC [True/False]
        
        
    #*****************************************************************************************#  
    
    ###################
    # PRIVATE FUNCTIONS
    ###################    
    
    
    
    def _leader_on(self):
            
        # RUNS AS A THREAD
        initial_counter = self.shutdown_counter
        #1. Now I am a LEADER,
        #   I periodically send hearbeats to Followers
        while self.idle:
        
            '''SIMULATE SHUTDOWN OF LEADER'''
            if self.verbose:
                self.shutdown_counter -= 1
                print(f"\nSIMULATE SHUTDOWN OF LEADER ⏰️ {self.shutdown_counter}/{initial_counter}")
                if self.shutdown_counter == 0:
                    print(f"☠️  ☠️  ☠️  {Colors.FAIL}LEADER [{self.ID}] IS DEAD{Colors.END}   ☠️  ☠️  ☠️\n")
                    self.leader_on_death()
                    return
                
            if self.verbose:
                print("\n------------------------(1)---------------------------\n"\
                        "📤 Periodically Sending HEARTBEATs (if request on halt)💓!" )
            self.send(RPC='heartbeat')    #periodically heartbeat
            
            if self.verbose:
                print("\n--------------------(2)----------------------\n"\
                        "📤 Periodically Sending APPENDENTRIES RPCs 📑️" )    
            self.send(RPC='REPLICATELOG') #preiodically replicate
            
            sleep(self.heartbeat_timeout) 
     
    def _CommitLogEntries(self):
        """
        Commit Entries to Leader's LOG
        A LEADER can commit an entry if it's been acknolwedged by a quorum of nodes
        Quorum = more than half of the nodes
        """
        def acks(length):
            """
            Return: the number of nodes tha have an ackdLength >= length
                    i.e. how many nodes acknowledged a log up to "length" or later
            """
            return len([n for n in self.followers if self.ackdLength[n] >= length])
        
       
        minAcks = math.ceil(len([self.followers+1])/2)
        
        # - Look at all the possible LOG lengths
        # - For each length, find those for which
        #   the number of acks is >= to the minimum_quorum
        ready   = [l for l in range(1, len(self.LOG)) if acks(l) >= minAcks]
        
        # - If ready is not an empty set, it means there is
        #   at least one LOG entry that is ready to be committed
        # - max is the latest entry ready to be committed
        if len(ready) != 0 and\
           max(ready) > self.commitLength and\
           self.LOG[max(ready)-1].TermNumber == self.TermNumber: 
            
            # COMMIT
            self.commitLength = max(ready) 
            
                     
    ##########------#########
    # PRIVATE SEND  FUNCTIONS
    ##########-------########         
    
    # OPTION (1): SEND REPLICATE LOG RPCs TO ALL FOLLOWERS
    def _replicate_log_to_all(self):
          
        for follower in self.my_followers:
            self.replicate_log(follower)
                                              
	
    # OPTION (2): SEND HEARTBEATS TO FOLLOWERS
    def _send_heartbeat_to_all(self):              
        for follower in self.my_followers:
            follower.receive(RPC='heartbeat',
                             sender=self, newEntries=None, sender_TermNumber=self.TermNumber,
                             sender_logLength=None,        f_logLength_from_s=None,
                             sender_commitLength=None,     sender_prevLogTerm=None, vote=None)
    
   
    ###################        
    
    
             
 
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
 
    	    
    

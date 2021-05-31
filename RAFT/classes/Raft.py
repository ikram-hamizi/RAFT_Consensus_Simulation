from states.Node import Node
from states.Leader import Leader
from states.Follower import Follower
import threading
from time import sleep
from collections import Counter
from threading import Event

class Raft():
    
    def __init__(self, n=5, verbose=True):
        self.LEADER = None
        self.verbose = verbose
        self.election_end = Event() #False
        self.followers = [Follower(i+1, verbose=verbose, election_end=self.election_end) for i in range(n)]
        
        # RAFT on start 
        # 1. Initialize Followers with timeouts
        # 2. Elect a Leader
        # 3. Leader starts periodically sending hearbeats to Followers
        # 4. Follower's timeouts are reset unless the Leader is down
        # 5. If a Leader is down, repeat step 2
        self._raft_on_start()
           
    
    def _getLeader(self):
        for f in self.followers:
            if f.state == 'Leader':
                return f.Leader
    
    def waitForLeader(self):
        while self.LEADER == None:
            self.LEADER = self._getLeader()
            continue;
        
        self.election_end.set() #True
        return self.LEADER
        
    def command(self, flag, command='incrementY', reps=100):
        leader = None
        result = False
        while result == False:
                
            if self.LEADER is not None and self.LEADER.state != 'Leader'\
                or self.LEADER is None:
                   
                self.LEADER = None
                self.LEADER = self.waitForLeader() #WAIT
                self.election_end.clear()
                    
                if self.verbose:
                    print("\n\n**************✰***************\n"\
                         f"RAFT: NEW LEADER WAS FOUND [{self.LEADER.ID}]"\
                          "\n**************✰***************\n")
              
                result = self.LEADER.on_receive_CLIENT_request(flag, command, reps=reps)#, APP_LOG=self.LOG)  

        return result
   
    # PRIVATE FUNCTION
    ###########################
    def _raft_on_start(self):
        
        #   Spawn the FOLLOWERs           
        #   Followers run as Threads #(THREAD 🧵)
        #   - They die when they reach timeout and become candidates
        #   - Else, they wait for heartbeats to reset the timeout
        
        print("Initialization of Servers...\n")
        for Follower in self.followers:
            Follower.reset_timeout(self.followers) #(THREAD 🧵)
        
        
        # RUN NETWORK INFINITELY
        print("\n*************" \
              "Start Network" \
              "*************\n")
        

          
               
               
               
               
               

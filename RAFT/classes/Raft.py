from states.Node import Node
from states.Leader import Leader
from states.Follower import Follower
import threading
from time import sleep
from collections import Counter

class Raft():
    
    def __init__(self, n=5, verbose=True):
        self.followers = [Follower(i+1, verbose=verbose) for i in range(n)]
        self.LEADER = None
        self.ElectionStart = False
        self.verbose = verbose
        
        # RAFT on start 
        # 1. Initialize Followers with timeouts
        # 2. Elect a Leader
        # 3. Leader starts periodically sending hearbeats to Followers
        # 4. Follower's timeouts are reset unless the Leader is down
        # 5. If a Leader is down, repeat step 2
        self._raft_on_start()
           
    
    def getLeader(self):
        for f in self.followers:
            if f.state == 'Leader':
                return f.Leader
        
    def command(self, flag, command='incrementY', reps=100):
        leader = None
        
        while leader == None:
            leader = self.getLeader()
            continue;
          
        self.LEADER = leader
        
        if self.verbose:
            print("\n\n************✰*************\n"\
                 f"RAFT: LEADER WAS FOUND [{self.LEADER.ID}]"\
                  "\n************✰*************\n")
              
        result =self.LEADER.on_receive_CLIENT_request(flag, command, reps=reps)
        
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
        

          
               
               
               
               
               

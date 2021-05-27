from states.Node import Node
from states.Leader import Leader
from states.Follower import Follower
import threading
from time import sleep
from collections import Counter

class Raft():
    
    def __init__(self, n=5):
        self.followers = [Follower(i+1) for i in range(n)]
        self.followers_threads = None
        self.LEADER = None
        self.ElectionStart = False
        self.votesDict = {}
        
        # RAFT on start 
        # 1. Initialize Followers with timeouts
        # 2. Elect a Leader
        # 3. Leader starts periodically sending hearbeats to Followers
        # 4. Follower's timeouts are reset unless the Leader is down
        # 5. If a Leader is down, repeat step 2
        self._raft_on_start()
           
    
    def command(self, command='incrementY', flag, reps=100):
        if command == 'incrementY':
            
    # LIST OF CLIENT OPERATIONS
    ###########################
    def incrementY(self, flag, reps=100):
        self.LEADER.leader_on_receive_request(flag, 'incrementY', reps=reps)
        
    def sum(self, data):
        # Example
        #self.LEADER.leader_on_receive_request(data, 'sum')
        return None
        
    def sub(self, data):
        # Example
        #self.LEADER.leader_on_receive_request(data, 'sub')
        return None
    ###########################
   
   
    # PRIVATE FUNCTION
    ###########################
    def _raft_on_start(self):
            
        self.ElectionStart = False
        
        # 1. Spawn the FOLLOWERs           
        #   Followers run as Threads #(THREAD 🧵)
        #   - They die when they reach timeout and become candidates
        #   - Else, they wait for heartbeats to reset the timeout
        
        print("Initialization of Servers...\n")
        for Follower in self.followers:
            voters = [f for f in self.followers if f is not Follower]
            Follower.reset_timeout(voters) #(THREAD 🧵)
        
        
        # RUN NETWORK INFINITELY
        print("\n*************" \
              "Start Network" \
              "*************\n")
                 
        while True: 
                        
            if self.LEADER != None:
                print(f"\n\n"\
                       "-"*8 \
                       f"TERM NUMBER = {self.LEADER.TermNumber}"\
                       "-"*8)
            # ----------------------------------------------------
            # * LEADER IS PERIODICALLY SENDING HEARTBEATS 💓
            # * THE PROGRAM CHECKS IF A FOLLOWER BEAME A CANDIDATE
            #   IF SO, IT STARTS A NEW LEADER ELECTION
            # ----------------------------------------------------       
            
            FOLLOWERS = self.followers if self.LEADER is None else self.LEADER.followers 
            
            for Follower in FOLLOWERS:
                 
                # 2. Check if a FOLLOWER reaches timeout
                #   If so, collect the votes to compare 
                #   them and finalize the LEADER Election 
                
                if Follower.state == 'Candidate': 
                    # A Candidate automatically increment its TermNumber 
                    # and sends RequestVotes RPCs to the rest of the nodes      
                    print("Candidate:", Follower.ID)
                    
                    if self.ElectionStart == False:                       
                        ###############
                        #Start Election
                        ###############
                        self.ElectionStart = True
                        print("Start Election")
                                                       
                        # Save the vote on the Candidate
                        # ONLY IF it won a majority vote
                        if Follower.voteGranted:
                             self.votesDict[Follower] = True
                        
            # ------------------------------------------------------
            # IF AN ELECTION HAPPENED, FIND THE WINNING LEADER
            # ELSE, THE CURRENT LEADER WILL KEEP SENDING HEARTBEATS
            # ------------------------------------------------------
            if self.ElectionStart == True:
                
                #3. Find a LEADER (majority votes in elections)
                candidates = [Candidate for (Candidate, vote) in self.votesDict.items() if vote == True]
                
                if len(candidates) == 1:
                    # Find the winner
                    self.LEADER = candidates[0]
                       
                elif len(candidates) > 1:
                    winners = Counter(candidates).most_common(n=2) #returns tuples
                       
                    # Check for split-vote! (when 2 Candidates have a vote tie)
                    # If so, the term will have no Leader. Repeat.
                    if winners[0][1] == winners[1][1]:
                        print("Oh No! Split Vote! No LEADER for this Term\n")
                        continue;
                            
                    # Else, get the winner
                    else:
                        self.LEADER = winners[1]
                            
                else:
                    # If no leader is elected, we repeat the process of Election
                    if self.LEADER == None:
                       continue;
                    # Else, election ends by finding a new leader
                    
                ###############
                # END ELECTION
                ###############
                
                self.ElectionStart = False
                print("------"*3 \
                      "NEW LEADER!! ID: {self.LEADER.ID}"
                      "------"*3)
                      
                # Convert self.LEADER to instance of class "Leader"
                self.LEADER = Leader.become_leader(self.LEADER)
             
                #4. When LEADER is elected, start the Leader's role
                #   Leader informs Followers that it's the Leader with hearthbeats 💓
                #   Leader infinitely and periodically sends heartbeat AppendEntries() RPCs
                  
                self.LEADER.leader_begin() #INFINITE LOOP PROCESS (THREAD 🧵)
                #NOW, the Leader is ready to accept CLIENT requests
    
    ###########################               
               
               
               
               
               
               

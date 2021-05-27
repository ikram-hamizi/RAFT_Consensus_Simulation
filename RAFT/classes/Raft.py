from states.Node import Node
from states.Leader import Leader
from states.Follower import Follower
import threading
from time import sleep
from collections import Counter

class Raft():
    
    def __init__(self, n=5):
        self.replicas = 3
        self.followers = [Follower(i+1) for i in range(self.replicas)]
        self.followers_threads = None
        self.LEADER = None
        self.ElectionStart = False
        self.votesDict = {}
        
        
        self._raft_on_start()
           
    
    # LIST OF CLIENT OPERATIONS
    ###########################
    def incrementY(self, flag, reps=100):
        self.LEADER.leader_on_receive_request(flag, 'incrementY', reps=reps)
        
    def sum(self, data):
        #self.LEADER.leader_on_receive_request(data, 'sum')
        return None
        
    def sub(self, data):
        #self.LEADER.leader_on_receive_request(data, 'sub')
        return None
    ###########################
        
    def _raft_on_start(self):
            
        self.ElectionStart = False
        
        #1. Spawn the FOLLOWERs           
        #   Followers run as threads #(THREAD 🧵)
        #   - They die when they reach timeout and become candidates
        #   - Else, they wait for heartbeats to reset the timeout
        
        for Follower in self.followers:
            voters = [f for f in self.followers if f is not Follower]
            Follower.reset_timeout(voters) #(THREAD 🧵)
        
        print("*************")
        print("Start Network")
        print("*************\n")
        # RUN NETWORK INFINITELY


        while True: 
            sleep(1)
            
            if self.LEADER != None:
                print(f"\n\n-------TERM NUMBER = {self.LEADER.TermNumber}---------")
            # --------------------------------------------------
            # LEADER IS PERIODICALLY SENDING HEARTBEATS 💓
            # PROGRAM CHECKS IF ANY FOLLOWER IS DOWN/LEADERLESS
            # IF SO, IT STARTS A NEW LEADER ELECTION
            # --------------------------------------------------        
            
            FOLLOWERS = self.followers if self.LEADER is None else self.LEADER.followers 
            
            
            for Follower in FOLLOWERS:
               
                #   check if a FOLLOWER reaches timeout
                #2. If so, Start LEADER Election 
                if Follower.state == 'Candidate': 
                    # The Candidate will immediately increment its TermNumber 
                    # and send RequestVotes RPCs to the rest of the nodes
                    
                    print("Candidate:", Follower.ID)
                    
                    if self.ElectionStart == False:
                                               
                        ###############
                        #Start Election
                        ###############
                        self.ElectionStart = True
                                                       
                        # Save the vote on the Candidate if it won a majority vote
                        if Follower.voteGranted:
                             self.votesDict[Follower] = True
                        
            # ------------------------------------------------------
            # IF THERE AN ELECTION HAPPENED, FIND THE WINNING LEADER
            # ELSE, THE CURRENT LEADER WILL KEEP SENDING HEARTBEATS
            # ------------------------------------------------------
            if self.ElectionStart == True:
                print("Start Election")
                #3. Find LEADER (majority votes in elections)
                candidates = [Candidate for (Candidate, vote) in self.votesDict.items() if vote == True]
                   
                if   len(candidates) == 1:
                    self.LEADER = candidates[0]
                       
                elif len(candidates) > 1:
                    winners = Counter(candidates).most_common(n=2) #returns tuples
                       
                    # check for split-vote
                    # i.e. when two Candidates have a vote tie
                    # if so, the term will have no Leader
                    # we continue, and we repeat the process
                    if winners[0][1] == winners[1][1]:
                        print("Oh No! Split Vote! No LEADER for this Term\n")
                        continue;
                            
                    # else, we have a new LEADER!
                    else:
                        self.LEADER = winners[1]
                            
                else:
                    #if no leader is elected, we repeat the process of Election
                    if self.LEADER == None:
                       continue;
                   
                # ELECTION ENDS BY ELECTING A NEW LEADER
                ###############
                # END ELECTION
                ###############
                self.ElectionStart = False
                print("------"*3)
                print("NEW LEADER!! ID:", self.LEADER.ID) 
                print("------"*3)
                # Convert self.LEADER to instance of class "Leader"
                
                self.LEADER = Leader.become_leader(self.LEADER)
             
                #4. When LEADER is elected, start the Leader's role
                #   Leader informs Followers that it's the Leader with hearthbeats 💓
                #   Leader infinitely and periodically sends heartbeat AppendEntries() RPCs
                  
                self.LEADER.leader_begin() #INFINITE LOOP PROCESS (THREAD 🧵)
                #NOW, the Leader is ready to accept CLIENT requests
               
               
               
               
               
               
               
               

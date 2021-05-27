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
        self.index_of_LEADER = None
        
        # RAFT on start 
        # 1. Initialize Followers with timeouts
        # 2. Elect a Leader
        # 3. Leader starts periodically sending hearbeats to Followers
        # 4. Follower's timeouts are reset unless the Leader is down
        # 5. If a Leader is down, repeat step 2
        self._raft_on_start()
           
    
    def command(self, flag, command='incrementY', reps=100):
        result = self.LEADER.leader_on_receive_request(flag, command, reps=reps)
        return result
   
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
            sleep(1)
            if self.LEADER != None:
                print("\n\n"\
                       "---------------------\n"
                       f"TERM NUMBER = {self.LEADER.TermNumber}\n"\
                       "---------------------\n")
                 
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
                        
                        # - If there was a LEADER before, it has to
                        #   step down in the presence of a Candidate
                        # - Copy the Leader's content back to the 
                        #   original Follower object
                        if self.LEADER is not None:
                            self.LEADER.leader_on_death()
                            self.LEADER = None
                                                                               
                        # Save the vote on the Candidate
                        # ONLY IF it won a majority vote
                        if Follower.voteGranted:
                             self.votesDict[Follower] = True
                        
            # ------------------------------------------------------
            # IF AN ELECTION HAPPENED, FIND THE WINNING LEADER
            # ELSE, THE CURRENT LEADER WILL KEEP SENDING HEARTBEATS
            # ------------------------------------------------------
            if self.ElectionStart == True:
                print("Start Election")
                winner = None
                #3. Find a LEADER (majority votes in elections)
                candidates = [Candidate for (Candidate, vote) in self.votesDict.items() if vote == True]
                
                if len(candidates) == 1:
                    # Find the winner
                    winner = candidates[0]
                       
                elif len(candidates) > 1:
                    winners = Counter(candidates).most_common(n=2) #returns tuples
                       
                    # Check for split-vote! (when 2 Candidates have a vote tie)
                    # If so, the term will have no Leader. Repeat.
                    if winners[0][1] == winners[1][1]:
                        print("Oh No! Split-Vote! No LEADER for this Term. Followers' Timeouts are re-initialized\n")
                        for Follower in self.followers:
                            voters = [f for f in self.followers if f is not Follower]
                            Follower.reset_timeout(voters) #(THREAD 🧵)
                        continue;
                            
                    # Else, get the winner
                    else:
                        winner = winners[1]
                            
                else:
                    # If no leader is elected, we repeat the process of Election
                    if winner == None:
                       continue;
                    # Else, election ends by finding a new leader
                    
                ###############
                # END ELECTION
                ###############
                
                self.ElectionStart = False
                                
                # Get index of the Follower that became LEADER
                self.index_of_LEADER = self.followers.index(winner)
                     
                    
                # Convert self.LEADER to instance of class "Leader"
                self.LEADER = Leader.become_leader(winner)
                
                print("\n--✰--✰--✰--✰--✰--✰--\n" \
                      f"NEW LEADER!! ID: {self.LEADER.ID}\n"
                      "--✰--✰--✰--✰--✰--✰--\n")
                      
                #4. When LEADER is elected, start the Leader's role
                #   Leader informs Followers that it's the Leader with hearthbeats 💓
                #   Leader infinitely and periodically sends heartbeat AppendEntries() RPCs
                  
                self.LEADER.leader_begin() #INFINITE LOOP PROCESS (THREAD 🧵)
                #NOW, the Leader is ready to accept CLIENT requests
                
                    
    ###########################               
               
               
               
               
               
               

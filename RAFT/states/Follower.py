#import python libraries
from time import sleep 
from random import random, randint
import threading 
from collections import Counter
import datetime
from threading import Event
import math

#import local modules
from states.Node import Node
from states.Leader import Leader
from classes.Colors import Colors

class Follower(Node):
                
    def __init__(self, ID, verbose=True):
        super().__init__(ID)
        self.state = 'Follower' 
        self.interrupt_countdown = Event()
        self.countdown_stopped = True #for mutex
        self.votesReceived = []
        self.verbose = verbose
         
    #*****************
    # PUBLIC FUNCTIONS
    #*****************
    
    def reset_timeout(self, followers):
        """
        Behavior:     
            reset timeout of the Follower by:
             - The LEADER, OR
             - At initialization
        """
        
        #1. Kill the previous timeout thread
        self.interrupt_countdown.set()    #countdown interrupt button is clicked (True)
        
        # MUTEX LOCK the interrupt_countdown Event
        while self.countdown_stopped == False:
            continue;       
        
        # Unclick the button
        self.interrupt_countdown.clear()  #False
            
        #2. Start a new timeout thread 
        self.followers = followers
        reset_follower_timeout_thread = threading.Thread(target=self._follower_on,
                                                         name=f'Thread [ID={self.ID}] Timeout Reset')
        reset_follower_timeout_thread.start()
       
             
    #Recovery on crash       
    def become_follower_again(self, leader_object):
        """
        Behavior: 
            Copy my information from when I was a Leader to a Follower object
        Return: Follower object
        """
               
        # 1. COPY THE CONTENT OF THE LEADER
        self.LOG = leader_object.LOG
        self.TermNumber = leader_object.TermNumber
        self.followers = leader_object.followers
        self.commitLength = leader_object.commitLength
        
        # 2. RESET THE FOLLOWER'S INITIAL PROPERTIES
        # The Follower:
        # - waits for a new Leader
        # - to reset its timeout and restart it.
        # - Meanwhile, it remains idle
        self.state = 'Follower'
        self.votedFor = None
        self.Leader   = None 
        self.interrupt_countdown = Event()
        self.countdown_stopped = True #for mutex
        self.votesReceived = []
        self.sentLength = {}
        self.ackdLength = {}
         
        # Debug Printing
        self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
        self.print_node(self.lastTimeStampString) #PRINT
              
        return self          
     
    #################
    #RECEIVE FUNCTION
    #################
    def receive(self, RPC,
                      sender, newEntries,   sender_TermNumber,
                      sender_logLength,     f_logLength_from_s,
                      sender_commitLength,  sender_prevLogTerm, vote):
        """
        RECEIVE FUNCITON
        """
        if self.state == 'Candidate':          
            if RPC == 'VoteResponse':
                VoterForMe = sender
                v_term     = sender_TermNumber
                self._c_on_receive_votes(VoterForMe, v_term, vote)
        
        
        if self.state == 'Follower':
            if RPC == 'heartbeat':
                leader = sender
                L_TermNumber = sender_TermNumber
                self._on_receive_heartbeat(leader, L_TermNumber)
                
            if RPC == 'RequestVotes':
                candidate             = sender
                candidate_TermNumber  = sender_TermNumber
                candidate_logLength   = sender_logLength
                candidate_lastLogTerm = sender_prevLogTerm            
                self._on_receive_vote_request(candidate, candidate_TermNumber, candidate_logLength, candidate_lastLogTerm)
                 
            if RPC == 'REPLICATELOG':
                leader             = sender
                L_TermNumber       = sender_TermNumber
                f_logLength_from_L = f_logLength_from_s
                L_commitLength     = sender_commitLength
                L_prevLogTerm      = sender_prevLogTerm
                self._on_receive_replicate_log(leader, L_TermNumber, f_logLength_from_L, L_commitLength, L_prevLogTerm, newEntries)
            
     
    ################
    # SEND FUNCTION
    ################  
    def send(self, RPC='RequestVotes', Candidate=None, vote=None, ack=None, appended=False):
        """
        SEND
            - Only if state == 'Candidate'
            - Only send to Followers
            - Only if state == 'Follower' and RPC == 'VoteResponse' or 'LogResponse'
        """
        if self.state == 'Candidate':
        
            if RPC == 'RequestVotes':
                self._c_RequestVotes()
            
        if self.state == 'Follower': 
            if RPC == 'VoteResponse':
                self._send_vote_responses(Candidate=Candidate, vote=vote) 
       
            if RPC == 'LogResponse':
                self._send_log_response(follower=self, follower_TermNumber=self.TermNumber, ack=ack, appended=appended)
    
    #******************
    # PRIVATE FUNCTIONS
    #******************

    #########################
    # PRIVATE  SEND FUNCTIONS 
    #########################   
    """
    RPC='LogResponse'
    """
    def _send_log_response(self, follower, ack, appended):
        Leader.receive(RPC='LogResponse',
                       follower=self, follower_TermNumber=self.TermNumber,
                       ack=ack, appended=appended)
          
    """
    RPC='VoteResponse'  
    """ 
    def _send_vote_responses(self, Candidate, vote):
        # send my vote to Candidate
        if self.state == 'Follower': 
            Candidate.receive(RPC='VoteResponse',
                              sender=self, newEntries=None,   sender_TermNumber=self.TermNumber,
                              sender_logLength=None,          f_logLength_from_s=None,
                              sender_commitLength=None,       sender_prevLogTerm=None, vote=vote)
    """
    RPC='RequestVotes'  
    """         
    def _c_RequestVotes(self):            
        if self.state == 'Candidate':
        
            # 1. CANDIDATE increments ts term + votes on itself
            self.TermNumber += 1
            self.votedFor = self
            self.votesReceived.append(self)
            
            # Print for debugging
            if self.verbose:
                print(f"\n {Colors.WARNING}[ID={self.ID}] I REQUESTED VOTES ⚠️ Term={self.TermNumber}{Colors.END}")
            
                        
            # 2. send RequestVotes RPCs to FOLLOWERs
            # Start Election
            lastLogTerm = 0
            if len(self.LOG) >0:
                lastLogTerm = self.LOG[len(self.LOG)-1].TermNumber # 0 if LOG is empty
            
            # Exclude self from the list of voters
            voters = self.followers[:self.ID-1]
            if self.ID < len(self.followers):
                voters += self.followers[self.ID:]
                        
            for follower in voters:
                follower.receive(RPC='RequestVotes', 
                                       sender=self, newEntries=None,   sender_TermNumber=self.TermNumber,
                                       sender_logLength=len(self.LOG), f_logLength_from_s=None,
                                       sender_commitLength=None,       sender_prevLogTerm=lastLogTerm, vote=None)
     
    ###########################
    # PRIVATE RECEIVE FUNCTIONS
    ###########################
    """
    RPC = 'REPLICATELOG'
    """
    def _on_receive_replicate_log(self, leader, L_TermNumber, f_logLength_from_L, L_commitLength, L_prevLogTerm, newEntries):
        #Follower receives entries from the Leader
        #We want our Term to be == to the Leader's Term
        #This means that all our latest LOG entries are consistent with the Leader 
        
        #1. Compare Terms
        if L_TermNumber > self.TermNumber:
            self.TermNumber = L_TermNumber
            self.votedFor = None
            self.Leader = leader
            self.state = 'Follower'
        
        
        if (L_TermNumber == self.TermNumber) and (self.state == 'Candidate'):
            self.state = 'Follower'
            self.Leader = leader
        
        #2. If Terms are the same, check if the LOG is the same
        #   Otherwise, send to the Leader to give us earlier LOG entries
        logAssert  = ((len(self.LOG) >=  f_logLength_from_L) and
                      ((f_logLength_from_L == 0) or 
                       (L_prevLogTerm == self.LOG[f_logLength_from_L-1].TermNumber)))
                      

        ack = 0
        appended = False
        
        #3. If Term is ok and LOG is ok, Follower appends to its LOG
        #  And send to the Leader the length of the acknolwedged LOG
        if L_TermNumber == self.TermNumber and logAssert:
            self.append_entries(f_logLength_from_L, L_commitLength, newEntries)
            ack = f_logLength_from_L + len(newEntries) # we received LOG up to ack length
        
        self.send(RPC='RequestVotes', Candidate=None, vote=None, ack=ack, appended=appended)
                     
    """
    RPC='heartbeat'  
    """                     
    def _on_receive_heartbeat(self, leader, Leader_TermNumber):
        """
        When Follower receives heartbeat, it
        kills its previous thread and restarts
        a new one with a new timeout.
        """
        
        if self.verbose:
            print(f"\n📥 [ID={self.ID}] I received a HEARTBEAT from [{leader.ID}]"\
                  f"(Leader Term = {Leader_TermNumber}. Mine is = {self.TermNumber})")
                            
        # Follower rejects Leader's heartbeat
        # If the Leader is stale, Follower becomes a Candidate
        if Leader_TermNumber < self.TermNumber:
            self.votedFor = None
            self.Leader = None
            
            print("❌ [ID={self.ID}] HEARTBEAT REJECTED [{leader.ID}]. LEADER's TermNumber is obsolete.")
        
        # Follower sets the Leader to the Term's Leader
        # After receiving a heartbeat from it 💓
        else:
            self.Leader = leader
            self.state = 'Follower'
            self.reset_timeout(self.followers)
            
            if self.verbose:
                print(f"✅ [ID={self.ID}] Confirmed HEARTBEAT: I reset my timeout, {self.state}") 

            
    """
    RPC='RequestVote'  
    """ 
    def _on_receive_vote_request(self, candidate, candidate_TermNumber, candidate_logLength, candidate_lastLogTerm):
        """
        Follower votes on a Candidate if it has
        a higher term number + an updated LOG + has not already voted
        If so, set the vote of this Follower to the Candidate.
        """

        vote = False 
        myLogTerm = 0
        
        if len(self.LOG) > 0:  
            myLogTerm = self.LOG[len(self.LOG)-1].TermNumber # 0 if LOG is empty
        
        logAssert  = ((candidate_lastLogTerm >  myLogTerm) or\
                     ((candidate_lastLogTerm == myLogTerm) and (candidate_logLength>=len(self.LOG))))
                     
        termAssert = ((candidate_TermNumber >  self.TermNumber) or\
                     ((candidate_TermNumber == self.TermNumber) and (self.votedFor in [None, candidate])))
        

        if logAssert and termAssert:
            self.TermNumber = candidate_TermNumber #MAKE v_TermNumber == c_TermNumber
            self.state = 'Follower'           
            self.votedFor = candidate
            vote = True       
        
        # send vote response
        self.send(RPC='VoteResponse', Candidate=candidate, vote=vote)
                
    """
    RPC='VoteResponse'  
    """ 
    def _c_on_receive_votes(self, voter, v_term, vote):
        if self.state == 'Candidate':
        
            if v_term == self.TermNumber and vote:
                #SUCESSFUL VOTE
                self.votesReceived.append(voter) #append new voter
                
                if len(self.votesReceived) >= math.ceil((len([self.followers])+1)/2): # This avoids split-vote          
                    voters = [v.ID for v in self.votesReceived]    
                    self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
                    
                    print("\n**✰✰**✰✰**✰✰**✰✰**✰✰**✰✰**✰**✰✰**✰✰**✰✰**✰✰**"\
                         f"\n👑️ [ID={self.ID}] End of Election. {Colors.OKGREEN}I BECAME LEADER!{Colors.END}"\
                         f"\n   voters={voters} | {self.lastTimeStampString}"\
                          "\n**✰✰**✰✰**✰✰**✰✰**✰✰**✰✰**✰**✰✰**✰✰**✰✰**✰✰**\n")
                   
                    self.state = 'Leader'
                                     
                    # Exclude self from the list of voters
                    voters = self.followers[:self.ID-1]
                    if self.ID < len(self.followers):
                        voters += self.followers[self.ID:]
                        
                    for follower in voters:
                        self.sentLength[follower] = len(self.LOG)  #length of log we sent to follower
                        self.ackdLength[follower] = 0              #length of log acknwoledged to be received by follower
                        
                    # convert to Leader object
                    self.Leader = Leader.become_leader(self)
                    # Send heartbeats + REPLICATE LOG
                    self.Leader.leader_begin()
                    #self.Leader.send('REPLICATELOG')
                    sleep(2)
                    
                                                
                        
            elif v_term > self.TermNumber:
                #UNSUCCESSFUL VOTE
                if self.verbose:
                    print(f"[ID{self.ID}] I did not become a Leader :( ")
                self.TermNumber = v_term
                self.state = 'Follower'
                self.votedFor = None    
                # Finish Election
                
           
   
    #************************
    # OTHER PRIVATE FUNCTIONS 
    #************************     
       
          
    def _start_countdown(self):
        """
        Behavior
            while stop_countdown is not set yet
            or timeout did not reach the end yet
            keep the countdown
        """
        mini, maxi = (3,6) #mini, maxi = (150*1e-3,300*1e-3)
        self.timeout = randint(mini, maxi) #self.timeout = random()*(maxi-mini)+mini
        sec = 0
        
        while sec < self.timeout and (self.interrupt_countdown.is_set() == False):
            self.interrupt_countdown.wait(1) #WAIT
            sec+=1
        
        if self.verbose:           
            print(f"[ID={self.ID}] Timer ended at {sec}/{self.timeout}")   
    

    def _follower_on(self):      
        """
        Behavior:
            Follower starts random timeout
            and becomes a Candidate as soon as
            it reaches the end of its timeout if 
            it doesn't receive a hearbeat
        """  
        
        #1. Assert Follower Properties
        self.state = 'Follower'
        self.countdown_stopped = False
        
        # Print for Debugging
        self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
        
        if self.verbose:
            print(f"\n[ID={self.ID}][ ] {self.lastTimeStampString} Timeout will restart. "\
                  f"I am Follower now - {self.print_node(print_fn=False)}")   
           
        #2. Start Timeout countdown   
        self._start_countdown() # < < < < < < < (WAIT)
        
        
        #3.a Check if a new Leader was elected
        if self.interrupt_countdown.is_set() == True:
        # If the timeout was interrupted
        # It means a new LEADER was elected
        # Follower waits until reset_timeout() is called at heartbeat RPC
            if self.verbose:
                print(f"[self.ID] ~ ~ ~ Leader killed my countdown and will restart it again")
            self.countdown_stopped = True
            sleep(0.01)
            return
        
        #3.b Else, become a Candidate and collect votes  
        else:  
            # This means that the timeout was reached  
            
            # print for Debugging
            self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
            
            if self.verbose:
                print(f"\n [ID={self.ID}][x] {self.lastTimeStampString} {Colors.WARNING}/!\ Timeout reached /!\ "\
                      f"I am Candidate now.{Colors.END}")

            self.state = 'Candidate'      
            self.Leader = None
            self.votedFor = None
                        
            # The Candidate will immediately increment its TermNumber 
            # and send RequestVotes RPCs to the rest of the nodes
            self.send(RPC='RequestVotes')
            
            
    def append_entries(self, f_logLength_from_L, L_commitLength, newEntries):
    
        if len(newEntries) > 0 and len(self.LOG) > f_logLength_from_L:
            #Check if Terms are cosnsitent. For example:
            #If Follower had entries from a Leader that crashed, it may have advanced Terms
            #If the Terms of the same LOG entry are different, we truncate the LOG and
            #discard LOG entries that are not committed
            if self.LOG[f_logLength_from_L].TermNumber != newEntries[0].TermNumber:
                self.LOG = self.LOG[:f_logLength_from_L-1]
        
        if f_logLength_from_L + len(newEntries) > len(self.LOG):
            a = len(self.LOG) - f_logLength_from_L
            b = len(newEntries) - 1
            for i in range(a,b):
                self.LOG.append(newEntries[i])
        
        # At this point, we have a LOG consistent with the LEADER's
        
        if L_commitLength > self.commitLength:
            self.commitLength = L_commitLength
            
    #################      
            

    
    
    
          
        
        
     
	
	

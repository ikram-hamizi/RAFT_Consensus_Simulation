from states.Node import Node
from time import sleep 
from random import random, randint
import threading 
from collections import Counter
import datetime
from threading import Event

class Follower(Node):

    def __init__(self, ID):
    	
        super().__init__(ID)
        self.state = 'Follower' 
        self.voteGranted = False 
        self.interrupt_countdown = Event()
        self.countdown_stopped = True #for mutex
        

           
    ###################
    # PRIVATE FUNCTIONS 
    ###################       
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
                   
        print(f"[ID={self.ID}] Timer ended at {sec}/{self.timeout}")   
    
    
    def _follower_on(self):      
        """
        Behavior:
            Follower starts random timeout
            and becomes a Candidate as soon as
            it reaches the end of its timeout if 
            it doesn't receive a hearbeat
        """  
        
        self.voteGranted = False
        self.state = 'Follower'
        self.countdown_stopped = False
        
        self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
        print(f"\n[ID={self.ID}][ ] {self.lastTimeStampString} Timeout will restart. I am Follower now - {self.print_node(print_fn=False)}")   
           
        self._start_countdown() # < < < < < < < TIMEEOUT STARTS (WAIT)
        
        # If the timeout was interrupted (thread KILLED)
        # It means a new LEADER was elected
        # Follower Behavior: do nothing
        if self.interrupt_countdown.is_set() == True:
            print("~ ~ ~ ~ ~ LEADER KILLED MY COUNTDOWN")
            self.countdown_stopped = True
            sleep(0.01)
            return
            
        # Else, it means that the timeout was reached  
        self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
        print(f"\n [ID={self.ID}][x] {self.lastTimeStampString} /!\ Timeout reached /!\ I am Candidate now.")
        self.heartbeat = False
        self.state = 'Candidate'
        self.Leader = None
        self.votedFor = None
                   
        # The Candidate will immediately increment its TermNumber 
        # and send RequestVotes RPCs to the rest of the nodes
        self.RequestVotes(self.followers)
       
    ##################
    # PUBLIC FUNCTIONS
    ##################
    
    def reset_timeout(self, followers):
        """
        Behavior:     
            reset timeout of the Follower by:
             - the LEADER
             - or at initialization
        """
        
        self.interrupt_countdown.set()    #set to True - countdown interrupt button is clicked
        
        # MUTEX LOCK the interrupt_countdown Event
        while self.countdown_stopped == False:
            continue;       
        
        self.interrupt_countdown.clear()  #set to False 
             
        self.followers = followers
        reset_follower_timeout_thread = threading.Thread(target=self._follower_on,
                                                         name=f'Thread [ID={self.ID}] Timeout Reset')
        reset_follower_timeout_thread.start() 
        
           
        def become_follower(self, leader_object):
        """
        # Copy my information from when I was a Leader to a Follower object
        Return: Follower object
        """
               
        # 1. COPY THE CONTENT OF THE LEADER
        #follower.nextIndex = 2
        self.LOG = leader_object.LOG
        self.TermNumber = leader_object.TermNumber
        self.followers = leader_object.followers
        self.commitIndex = leader_object.commitIndex
                
        # 2. RESET THE FOLLOWER'S INITIAL PROPERTIES
        # The Follower waits for a new Leader to 
        # reset its timeout and restarts it
        # meanwhile, it remains idle
        self.state = 'Follower'
        self.votedFor = None
        self.voteGranted = False
        self.heartbeat = True
        self.interrupt_countdown = Event()
        self.countdown_stopped = True #for mutex
        
        
        # Debug Printing
        self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
        self.print_node(self.lastTimeStampString) #PRINT
        
        del leader_object
        
        return self
        
               
    def on_receive_heartbeat(self, Leader, Leader_TermNumber):
        """
        Behavior:
            When Follower receives heartbeat, it kills its previous thread and resets its timeout.
        """
        
        print(f"\n📥 [ID={self.ID}] I received a HEARTBEAT. (Leader Term = {Leader_TermNumber}. Mine is = {self.TermNumber})")
                        
        # Follower receives rejects Leader's heartbeat
        # If the Leader is stale
        # Follower remains a Candidate
        if Leader_TermNumber < self.TermNumber:
            self.TermNumber = Leader_TermNumber
            self.votedFor = None
            self.LEADER = None
            self.heartbeat = False
            print("❌ [ID={self.ID}] HEARTBEAT REJECTED. LEADER's TermNumber is obsolete.")
            return False
        
        # Follower sets the Leader to the Term's Leader
        # After receiving a heartbeat from it 💓
        # Follower remains a Follower as long as it receives
        # heartbeats. If not, it becomes a candidate at the end
        # of the timeout countdown
        else:
            self.votedFor = Leader
            self.LEADER = Leader
            self.heartbeat = True
            response = self.reset_timeout(self.followers)
            print(f"✅ [ID={self.ID}] Confirmed HEARTBEAT: I reset my timeout, {self.state}") 
            return True
    
    
    def on_receive_replicate_log(self, newLogEntry, commit_indexOfThePrecedingEntry, TermNumberOfThePrecedingEntry):
        #comparison = compareLOGs(
        return None
                
    
    def get_vote(self, candidate):
        """
        Behavior:
            Follower votes on a Candidate if it has
            a higher term number and an updated LOG and has not already voted
            If so, set the vote of this Follower to the Candidate.
        """
        
        #TODO add and _compareWithMyLOG(candidate.LOG)>0:                 
        if candidate.TermNumber > self.TermNumber and self.votedFor is None:
            
            self.votedFor = candidate
            #Log Replciation
        
        return self.votedFor
       
       
    def RequestVotes(self, followers):       
        
        if self.state == 'Candidate':
            # 1. CANDIDATE start by voting on itself
            self.votedFor = self
            self.TermNumber = self.TermNumber + 1
            print(f" [ID={self.ID}] I REQUESTED VOTES Term={self.TermNumber}")
            #self.start_countdown() #reset timeout
            voteGranted = True
            
            # 2. senf RequestVotes RPCs to FOLLOWERs
            candidates = [follower.get_vote(self) for follower in followers] #except self
            
            # 3. Check if I can become a LEADER
            # If the majority of votes is my ID,
            # then I become a Leader
            winner = Counter(candidates).most_common(1)[0]
            winnerID = winner[0].ID
            
            if winnerID == self.ID:
                print(f" [ID={self.ID}][=] I may become Leader. I have {winner[1]} votes")
                
            else:
                print(f"[ID={self.ID}][x] I cannot become a leader :(")
                voteGranted = False
                self.votedFor = winner[0]
                self.TermNumber -= 1
        else:
            voteGranted = False
            self.TermNumber -= 1
            print("Follower should not send RequestVotes RPCs") 
            
            
        self.voteGranted = voteGranted
                
            

#ctrl + . shows emojis list on linux' text editor!
    
    
    
        
        
        
     
	
	

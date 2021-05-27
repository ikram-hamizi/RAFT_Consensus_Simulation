from states.Node import Node
from time import sleep 
from random import random, randint
import threading 
from collections import Counter
import datetime
from threading import Event

class Follower(Node):

    def __init__(self, ID):
    	# start timeout
        super().__init__(ID)
        self.state = 'Follower' 
        self.voteGranted = False 
        self.interrupt_countdown = Event()
        self.reset_follower_timeout_thread = None
        self.countdown_stopped = True
        
    ''' 
    # PRIVATE FUNCTIONS 
    '''
    def _kill_countdown(self):
        self.interrupt_countdown.set()
        
    def _start_countdown(self):
        #mini, maxi = (150*1e-3,300*1e-3)
        mini, maxi = (3,6)
        #self.timeout = random()*(maxi-mini)+mini
        self.timeout = randint(mini, maxi)
        sec = 0
        #if stop_countdown is not set yet, keep the countdown
        while sec < self.timeout and (self.interrupt_countdown.is_set() == False):
            self.interrupt_countdown.wait(1) #WAIT
            sec+=1
                   
        print(f"\n[ID={self.ID}] Timer ended at {sec}/{self.timeout}")   
    
    # Follower starts random timeout
    # and becomes a Candidate as soon as
    # it reaches the end of its timeout if 
    # it doesn't receive a hearbeat
    def _follower_on(self):        
        self.voteGranted = False
        self.state = 'Follower'
        self.countdown_stopped = False
        
        now = datetime.datetime.now()
        print(f"\n[ID={self.ID}][ ] [{now.day}-{now.month}-{now.year} {now.hour}:{now.minute}:{now.second}.{int(str(now.microsecond)[:3])}] Timeout will restart. I am Follower now - {self.print_node(print_fn=False)}")   
           
        self._start_countdown() #TIMEEOUT STARTS
        
        # If the timeout was interrupted manually
        # It means a new LEADER was elected
        # Follower Behavior: do nothing
        if self.interrupt_countdown.is_set() == True:
            print("------------------------------LEADER KILLED MY COUNTDOWN")
            self.countdown_stopped = True
            sleep(0.1)
            return
            
        # Else, it means that the timeout was reached  
        now = datetime.datetime.now()
        print(f"\n [ID={self.ID}][x] [{now.day}-{now.month}-{now.year} {now.hour}:{now.minute}:{now.second}.{int(str(now.microsecond)[:3])}] /!\ Timeout reached /!\ I am Candidate now.")
        self.heartbeat = False
        self.state = 'Candidate'
                   
        # The Candidate will immediately increment its TermNumber 
        # and send RequestVotes RPCs to the rest of the nodes
        self.RequestVotes(self.followers)
       
    '''
    # PUBLIC FUNCTIONS
    '''    
    #reset timeout of the Follower
    #by the LEADER
    #or at initialization
    def reset_timeout(self, followers):
        
        self.interrupt_countdown.set()    #True - countdown interrupt button is clicked
        
        # LOCK the interrupt_countdown Event
        while self.countdown_stopped == False:
            continue;       
        
        self.interrupt_countdown.clear()  #False 
             
        self.followers = followers
        self.reset_follower_timeout_thread = threading.Thread(target=self._follower_on,
                                         name =f'Thread [ID={self.ID}] Timeout Reset')
        self.reset_follower_timeout_thread.start() 
        
           
    #when Follower receives heartbeat, it resets its timeout        
    def on_receive_heartbeat(self, Leader, Leader_TermNumber):
        
        print(f"\n📥 [ID={self.ID}] I received a HEARTBEAT check. Leader Term = {Leader_TermNumber}, mine is = {self.TermNumber}")
                        
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
                
    # Follower votes on a Candidate if it has
    # a higher term number and an updated LOG and has not already voted
    # If so, set the vote of this Follower to the Candidate.
    def get_vote(self, candidate):
        
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
            print(f"[ID={self.ID}] I REQUESTED VOTES Term={self.TermNumber}")
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
                print(f"[ID={self.ID}][=] I may become Leader. I have {winner[1]} votes")
                
            else:
                print(f"[ID={self.ID}][x] I cannot become a leader :(")
                voteGranted = False
                self.votedFor = winner[0]
                self.TermNumber -= 1
        else:
            voteGranted = False
            print("Follower should not send RequestVotes RPCs") 
            
            
        self.voteGranted = voteGranted
                
            
        
    
    
    
        
        
        
     
	
	

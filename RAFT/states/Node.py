from time import sleep
import datetime

class Node():
    
    def __init__(self, ID, verbose=True):
        
        self.ID = ID
        self.TermNumber = 0
        
        self.LOG = []
        self.commitLength = 0
        self.sentLength = {} #length of log we sent to follower
        self.ackdLength = {} #length of log acknwoledged to be received by follower
        
        #self.commitIndex = 1
        self.votedFor = None
        self.Leader = None
        self.heartbeat = True
        self.timeout = None
        self.followers = None
        self.lastTimeStamp = None
        self.lastTimeStampString = None
        self.verbose = True
        
            
    
    ##################
    # PUBLIC FUNCTIONS 
    ##################
    def print_node(self, print_fn=True, additional=""):
        info = f"ID = {self.ID} | TermNumber = {self.TermNumber} | followers = {len(self.followers)} | {additional}"
        if print_fn:
            print(info)
        else:
            return info
        
    def get_timestamp(self):
        now = datetime.datetime.now()
        timestamp = f"[{now.day}-{now.month}-{now.year} {now.hour}:{now.minute}:{now.second}.{int(str(now.microsecond)[:3])}]"
        return now, timestamp
        

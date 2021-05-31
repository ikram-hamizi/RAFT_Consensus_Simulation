#import time
class LogEntry():
    
    
    def __init__(self, command, entry, TermNumber, commitLength, iteration):

    	self.command = command
    	self.entry = entry
    	
    	# unique LOG identifiers
    	self.TermNumber = TermNumber
    	self.commitIndex = commitLength
    	self.iteration = iteration
    	
    
    def print_entry(self):
    	return f"LOG[#Term={self.TermNumber}, commitLength={self.commitLength}: Command={self.command} → Entry={self.entry}, Iteration={self.iteration}"
    	
    	


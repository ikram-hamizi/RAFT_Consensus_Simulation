#import time
class LogEntry():
    
    
    def __init__(self, command, entry, TermNumber, commitIndex):

    	self.command = command
    	self.entry = entry
    	
    	# unique LOG identifiers
    	self.TermNumber = TermNumber
    	self.commitIndex = commitIndex
    	
    
    def print_entry(self):
    	return f"LOG[#Term={self.TermNumber}, #commitIndex={self.commitIndex}: Command={self.command} → Entry={self.entry}"
    	
    	


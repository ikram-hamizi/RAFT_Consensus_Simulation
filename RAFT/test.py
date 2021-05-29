#import python libraries
from time import sleep
#import local modules
from classes.Raft import Raft
import classes.Colors

n = 3       # number of replicas
flag = (0,0) # goal make it (0,100)

# 1. initialize raft network
verbose = False
raft = Raft(n, verbose=verbose)

# Test if heartbeats work by simulating an idle Leader state
if verbose:
    sleep(3) 

# 2. send CLIENT request

print("\nClient request => Increment flag(0,0) to flag(0,100)")
result = raft.command(flag=flag, command='incrementY', reps=100)
print("\nClient command: Increment flag(0,0) to flag(0,100)")
print("Server result :", result)
assert result == (0,100)

print("Press on ctrl+c to end the test.py program")


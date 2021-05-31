#import python libraries
from time import sleep

#import local modules
from classes.Raft import Raft
from classes.Colors import Colors

n = 3
flag = (0,0) 

# 1. initialize raft network
verbose = False
raft = Raft(n, verbose=verbose)

# 2. Test if heartbeats work
#    by simulating an idle Leader state
#    i.e., no request from Client yet
if verbose:
    sleep(3) 

# 2. send CLIENT request
desired_result = (0,100)
print(f"\n{Colors.BG_WHITE} Client request => Increment flag(0,0) to flag{desired_result}.{Colors.END}")

result = raft.command(flag=flag, command='incrementY', reps=100)
correct = (result == desired_result)

COLOR = Colors.BG_BLUE if correct else Colors.BG_RED

print(f"\n{COLOR} Client request => Increment flag(0,0) to flag{desired_result}{Colors.END}")
print(f"{COLOR} Server result : {result}{Colors.END}")
assert correct, f"{Colors.FAIL}{result} is not the desired result {desired_result}.{Colors.END}"

print("Press on ctrl+c to end the test.py program")
    

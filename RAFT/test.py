from classes.Raft import Raft


n = 3       # number of replicas
flag = (0,0) # goal make it (0,100)
raft = Raft(n)

result = raft.incrementY(flag)

print("Client command: Increment flag(0,0) to flag(0,100)")
print("Server result :", result)

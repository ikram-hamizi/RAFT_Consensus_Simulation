# RAFT_Consensus_Simulation

This project implements a simulation of the RAFT consensus between node objects. Instead of using real servers/VMs/containers for the simulation, these nodes run timeouts on threads and are reset by the Leader.

In `test.py` a flag (0,0) is escorted in the network by Followers and modified by the Leader to be (0,100), +1 at a time.
The Leader does so by persisting the entries and replicating them to nodes. In the case of a Leader crash, a new Leader is elected, and continues the process.

### Steps:
- Donwload the repository
- Run `test.py`
- Modify `verbose = False` to `verbose = False` in `test.py` if you want to visualize:
  1. The Leader election 
  2. Leader sending hearbeats and Follower timeout resets
  3. Leader replication
  4. Simulation of Leader shutdowns (After every 3 successful heartbeats)

### Usage:
```python
from classes.Raft import Raft

n = 3       # number of replicas
raft = Raft(n, verbose=False)

flag = (0,0) 
result = raft.command(flag=flag, command='incrementY', reps=100)
>>> result = (0,100)
```

### RAFT folder structure
```bash
├── classes                     <- Folder with python class files
│   ├── Colors.py               <- Defines color fileds for printing to the terminal
│   ├── LogEntry.py             <- Defines a Log entr
│   └── Raft.py                 <- Defines the program that runs a RAFT network
│
├── states                      <- Folder with python class files of RAFT node states
│   ├── Follower.py             <- Node in Follower state ('Candidate' is transinet)
│   ├── Leader.py               <- Node in Leader state
│   └── Node.py                 <- Node is the parent class of Leader and Follower
└── test.py
```


Source: https://www.cl.cam.ac.uk/teaching/2021/ConcDisSys/materials.html

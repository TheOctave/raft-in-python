# raftconfig.py

# Mapping of logical server numbers to network addresses. All network
# operations in Raft will use the logical serber numbers (e.g., send
# a message form node 2 to node 4).
SERVERS = {
    1: ('localhost', 15000),
    2: ('localhost', 16000),
    3: ('localhost', 17000),
    4: ('localhost', 18000),
    5: ('localhost', 19000),
}
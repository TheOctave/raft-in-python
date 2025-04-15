from raftnet import RaftNetActor
from raftlogic import RaftLogic, AppendEntriesResponse, AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse
import threading
import time
import pickle

class RaftServer:
    def __init__(self, nodenum: int, cluster_size: int, manual_mode=True, debug_mode=False):
        # Networking handler
        self.nodenum = nodenum
        self.debug_mode = debug_mode
        self.network = RaftNetActor(self.nodenum)
        
        # The state machine of the application
        self.state_machine = None

        # Raft logic (Handles the logic of the application, including the log state)
        self.logic = RaftLogic(nodenum=nodenum, cluster_size=cluster_size, manual_mode=manual_mode, debug_mode=debug_mode)

        # Start listening for messages and processing both internal and external messages
        threading.Thread(target=self.listen_for_messages).start()
        threading.Thread(target=self.process_messages).start()

    
    def submit(self, command):
        """
        Submit a command to the raft server

            Parameters:
                command: the transaction to record in the system
        """
        self.logic.add_new_command(command)
    
    def listen_for_messages(self):
        """
        consistently polls for new messages in the network and queues
        them for processing if necessary
        """
        while True:
            msg = pickle.loads(self.network.receive())
            if self.debug_mode:
                print("> Received msg:", msg)
            self.logic.queue_request(self.nodenum, msg)
    
    def process_messages(self):
        """
        Consistently polls the queue for new messages and processes them appropriately based
        on the type of message
        """
        while True:
            if len(self.logic._request_queue) == 0:
                time.sleep(.002) # sleep for 2 milliseconds if there's no message in the queue
                continue
            
            dest, msg = self.logic._request_queue.popleft()
            if isinstance(msg, AppendEntriesRequest):
                if dest == self.nodenum:
                    self.logic.handle_append_entries(msg)
                else:
                    threading.Thread(target=self.network.send, args=[dest, pickle.dumps(msg)]).start()
            elif isinstance(msg, AppendEntriesResponse):
                if dest == self.nodenum:
                    self.logic.handle_append_entries_response(msg)
                else:
                    threading.Thread(target=self.network.send, args=[dest, pickle.dumps(msg)]).start()
            elif isinstance(msg, RequestVoteRequest):
                if dest == self.nodenum:
                    self.logic.handle_vote_request_request(msg)
                else:
                    threading.Thread(target=self.network.send, args=[dest, pickle.dumps(msg)]).start()
            elif isinstance(msg, RequestVoteResponse):
                if dest == self.nodenum:
                    self.logic.handle_vote_request_response(msg)
                else:
                    threading.Thread(target=self.network.send, args=[dest, pickle.dumps(msg)]).start()
            else:
                raise "Invalid message type detected"

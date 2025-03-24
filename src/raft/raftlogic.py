from dataclasses import dataclass
from raftlog import RaftLog, LogEntry
import raftconfig
from enum import Enum
from collections import deque, defaultdict
import pytest

@dataclass
class AppendEntriesRequest:
    term: int                   # Sender of the message
    leader_id: int              # leader's server id
    prev_log_index: int         # leader's term
    prev_log_term: int          # index of log entry immediately preceding new ones
    leader_commit:  int         # term of prev_log_index entry
    entries: list[LogEntry]     # log entries to store (empty for heartbeat). May send more than one for efficiency

@dataclass
class AppendEntriesResponse:
    term: int                   # current term, for leader to update itself
    success: bool               # true if follower contained entry matching prev_log_index and prev_log_term
    responder_id: bool          # id of the responder. This is helpful since we're using actor model messaging
    last_log_idx: int           # index of the last updated log entry. Only necessary when the append-entries request is successful

@dataclass
class RequestVoteRequest:
    term: int                   # current term of the candidate
    candidate_id: int           # candidate's server id
    last_log_term: int          # candidate's last log term
    last_log_index: int         # candidate's last log index

@dataclass
class RequestVoteResponse:
    term: int                   # current term, for the candidate to update itself
    vote_granted: bool          # true means candidate received vote
    vote_from: int              # the id of the server that voted
    vote_term: int              # the term for which the vote was cast

class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftLogic:
    def __init__(self, nodenum: int, cluster_size: int):

        assert cluster_size > 0 and cluster_size % 2 == 1 # We must have a non-empty and even raft cluster size

        self.role = Role.FOLLOWER
        self.nodenum = nodenum
        self.cluster_size = cluster_size
        self._request_queue = deque([])

        # Persistent states
        self.current_term = 0
        self.voted_for = None
        self.log = RaftLog([LogEntry(0, '')])
        
        # Volatile state on all servers
        self.commit_index = 0
        self.last_applied = 0
        self.votes = 0

        # volatile state on all leaders
        self.next_index = { n: 1 for n in range(1, cluster_size + 1) }
        self.match_index = { n: 0 for n in range(1, cluster_size + 1) }

    def add_new_command(self, command: str):
        """
        Adds a new command to a raft leader's logs. The command is only executed by the
        state machine when it is commited int the raft cluster

            Parameters:
                command: command for the state machine to execute
        """
        assert self.is_leader()
        self.log.append_command(leader_term=self.current_term, command=command)
        self.match_index[self.nodenum] = self.log.get_last_log_index()
        self.next_index[self.nodenum] = self.match_index[self.nodenum] + 1

        for server in range(1, self.cluster_size + 1):
            if server != self.nodenum:
                self.update_follower(server)

    
    def update_follower(self, follower: int, heartbeat=False):
        """
        Updates the state of a follower. This method is also used to send heartbeat
        messages to a follower

            Parameters:
                follower: server number to update
                heartbeat: indicates whether the requested update is a heartbeat or not
        """
        # Send the specified follower an AppendEntries message. Only on leaders.
        assert self.is_leader()
        prev_idx = self.next_index[follower] - 1
        prev_term = self.log.get_entry(prev_idx).term

        self.queue_request(follower,
                           AppendEntriesRequest(
                               term=self.current_term,
                               leader_id=self.nodenum,
                               prev_log_index=self.next_index[follower] - 1,
                               prev_log_term=prev_term,
                               leader_commit=self.commit_index,
                               entries=self.log.entries[self.next_index[follower]:] if not heartbeat else []
                            )
        )
    
    def queue_request(self, server: int, request: any):
        """
        Adds an rpc request/response to the execution queue for processing or sending over the network.

        A server can either send a request over the network to a different server in the raft network
        by specifying the server's node number or queue a request for self-processing
        by specifying its node number in the 'server' param.

            Parameters:
                server: use to indicate what server the request should be sent to
        """
        self._request_queue.append((server, request))

    def handle_append_entries(self, msg: AppendEntriesRequest):

        """
        Handles a raft AppendEntries request. The request is unsuccessful if
        the sender's current term is less than the server's term. Otherwise it is
        processed and the success of the request is determined by the parity between
        the log entries in the leader (request sender) and the current server.

        The current server is also converted to a follower if it's term is less than the requester's term

            Parameters:
                msg: the AppendEntries request
        """

        if self.current_term > msg.term:
            response = AppendEntriesResponse(
                                term = self.current_term,
                                success = False,
                                responder_id = self.nodenum,
                                last_log_idx = self.log.get_last_log_index(),
                        )
        else:
            append_success = self.log.append_entries(prev_index=msg.prev_log_index,
                                                    prev_term=msg.prev_log_term,
                                                    entries=msg.entries)
            response = AppendEntriesResponse(
                                term = self.current_term,
                                success = append_success,
                                responder_id = self.nodenum,
                                last_log_idx = self.log.get_last_log_index(),
                        )
        
        if self.current_term < msg.term:
            self.update_current_term(msg.term)
            if not self.is_follower():
                self.become_follower()
        
        self.queue_request(msg.leader_id, response)

    def handle_append_entries_response(self, response: AppendEntriesResponse):

        """
        Handles an AppendEntries response from another server in the raft network.

        If the request is updated 

        If the request is successful, the next_index and match_index for the requested server
        is updated, otherwise the current server is either demoted to follower -
        if the current term is less than the response term - or a follow-up AppendEntries request
        is made to the requested server
        """
        if self.current_term < response.term:
            self.update_current_term(response.term)
            self.become_follower()
            return
        if response.success == False:
            self.next_index[response.responder_id] -= 1
            self.update_follower(response.responder_id)
            return
        
        self.next_index[response.responder_id] = response.last_log_idx + 1
        self.match_index[response.responder_id] = response.last_log_idx
        self.attempt_commit()
    
    def handle_vote_request_request(self, msg: RequestVoteRequest):
        """
        Handles a Vote request from another server.
        There are three scenarios to handle here (in order):

        
            1. If the candidate's term is less than the current server's term, a failure
                response is immediately returned
            2. If the candidate has previously voted for the request term, return a success
                response if the vote was for the current requester, false otherwise
            3. If the candidate's log is at least up to date with the current log, the vote is granted,
                else the vote is not granted

            Parameters:
                msg: the AppendEntries RequestVoteRequest
        """
        candidate_log_up_to_date = lambda: msg.last_log_term > self.log.get_last_term() or \
                                    (msg.last_log_term == self.log.get_last_term() and msg.last_log_index >= self.log.get_last_log_index())
        
        if self.current_term > msg.term:
            response = RequestVoteResponse(
                term=self.current_term,
                vote_granted=False,
                vote_from=self.nodenum,
                vote_term=msg.term
            )
        elif self.current_term == msg.term:
            if self.voted_for != None:
                response = RequestVoteResponse(
                    term=self.current_term,
                    vote_granted=self.voted_for == msg.candidate_id,
                    vote_from=self.nodenum,
                    vote_term=msg.term
                )
            else:
                self.update_current_term(self.current_term, msg.term if candidate_log_up_to_date() else None)
                response = RequestVoteResponse(
                    term=self.current_term,
                    vote_granted=candidate_log_up_to_date(),
                    vote_from=self.nodenum,
                    vote_term=msg.term
                )
        else: # it means we haven't seen anything for this term, including any RequestVote request for this term up till now.
            former_term = self.current_term
            self.update_current_term(msg.term, msg.candidate_id if candidate_log_up_to_date() else None)
            self.become_follower()
            response = RequestVoteResponse(
                term=former_term,
                vote_granted=candidate_log_up_to_date(),
                vote_from=self.nodenum,
                vote_term=msg.term
            )
        
        self.queue_request(msg.candidate_id, response)
    
    def handle_vote_request_response(self, msg: RequestVoteResponse):
        """
        Handles a VoteRequest response from another server in the raft cluster
        There are two scenarios to handle here (in order).

            1. The term in the response is higher than the current server's term. In this case, the
                current server is demoted to follower and its term is updated
            2. Update the accrued vote count for the user and promote to leader if the vote count is greater
                than half the cluster size
        """
        if self.current_term < msg.term:
            self.update_current_term(msg.term)
            if not self.is_follower():
                self.become_follower()
        
        if not self.is_candidate() or self.current_term != msg.vote_term:
            return
        
        self.votes += 1
        if self.votes > self.cluster_size // 2:
            self.become_leader()
    
    def attempt_commit(self):
        """
        This is invoked after processing a successful `append_entries` response.
        The current raft server's commit index is updated if majority servers have reached the desired commit
        index AND if the server's commit index is behind the desired index
        """

        assert self.is_leader()

        idx_counts = defaultdict(int)
        for server in range(1, self.cluster_size + 1):
            idx_counts[self.match_index[server]] = idx_counts.get(self.match_index[server], 0) + 1
        
        n_seen = 0
        commit_index = self.commit_index
        for idx in sorted(idx_counts.keys(), reverse=True): # start accumulating from the largest match index to smallest
            if idx <= commit_index: # no need to commit, since we can't get a higher commit index
                break
            
            n_seen += idx_counts[idx]

            if n_seen > self.cluster_size // 2:
                commit_index = idx
                break

        if commit_index <= self.commit_index:
            return

        self.commit_index, prev_commit_index = commit_index, self.commit_index
        for idx in range(prev_commit_index + 1, self.commit_index + 1):
             self.apply_command(self.log.get_entry(idx))
    
    def apply_command(self, command: str):
        """
        Applies a command (transaction to the K-V store)

            Parameters:
                command: command to apply
        """
        print("APPLYING COMMAND:", command)

    def become_leader(self):
        """
        Promotes the current server to leader
        """
        self.next_index[self.nodenum] = self.current_term
        self.match_index[self.nodenum] = self.current_term
        self.role = Role.LEADER

    def is_leader(self) -> bool:
        """
        Indicates whether the current server is a leader
        """
        return self.role == Role.LEADER
    
    def become_follower(self):
        """
        Changes the current server's role to follower
        """
        self.role = Role.FOLLOWER

    def is_follower(self) -> bool:
        """
        Indicates whether the current server is a follower
        """
        return self.role == Role.FOLLOWER
    
    def become_candidate(self):
        """
        Changes the current server's role to candidate
        """
        self.role = Role.CANDIDATE

    def is_candidate(self) -> bool:
        """
        Indicates whether the current server is a candidate
        """
        return self.role == Role.CANDIDATE
    
    def update_current_term(self, term, vote_for: int = None):
        """
        Updates the current term of the server and sets the vote for the current term
        if it is provided, else sets the current vote to None

            Parameters:
                term: the server's new term
                vote_for: optional vote for the current term
        """
        self.current_term = term
        self.voted_for = vote_for

def test_handle_append_entries():
    logic = RaftLogic(nodenum=1, cluster_size=3)
    
    # queues failure response
    logic.current_term = 1
    logic.log.entries = [LogEntry(0, ''), LogEntry(0, 'x=1')]
    logic.handle_append_entries(AppendEntriesRequest(
                                    term = 1,
                                    leader_id = 2,
                                    prev_log_index = 1,
                                    prev_log_term = 1,
                                    leader_commit = 1,
                                    entries = [LogEntry(term=1, command='x=3')]
                                ))
    assert logic.log.entries == [LogEntry(0, ''), LogEntry(0, 'x=1')]
    assert logic._request_queue.pop() == (2, AppendEntriesResponse(
                                                    term = 1,
                                                    success = False,
                                                    responder_id = 1,
                                                    last_log_idx = 1,
                                                )
                                            )
    
    # queues success response when handling append_entries
    logic.handle_append_entries(AppendEntriesRequest(
                                    term = 1,
                                    leader_id = 2,
                                    prev_log_index = 1,
                                    prev_log_term = 0,
                                    leader_commit = 1,
                                    entries = [LogEntry(term=1, command='x=3')]
                                ))
    assert logic.log.entries == [LogEntry(0, ''), LogEntry(0, 'x=1'), LogEntry(term=1, command='x=3')]
    assert logic._request_queue.pop() == (2, AppendEntriesResponse(
                                                    term = 1,
                                                    success = True,
                                                    responder_id = 1,
                                                    last_log_idx = 2,
                                                )
                                            )
    
    # updates current term when the leader's term is greater
    logic.handle_append_entries(AppendEntriesRequest(
                                    term = 2,
                                    leader_id = 2,
                                    prev_log_index = 1,
                                    prev_log_term = 0,
                                    leader_commit = 1,
                                    entries = [LogEntry(term=1, command='x=3')]
                                ))
    assert logic.current_term == 2

    # responds with failure if term of request is less than the server's current term
    logic.handle_append_entries(AppendEntriesRequest(
                                    term = 1,
                                    leader_id = 2,
                                    prev_log_index = 1,
                                    prev_log_term = 0,
                                    leader_commit = 1,
                                    entries = [LogEntry(term=1, command='x=4')]
                                ))
    assert logic.log.entries == [LogEntry(0, ''), LogEntry(0, 'x=1'), LogEntry(term=1, command='x=3')]
    assert logic._request_queue.pop() == (2, AppendEntriesResponse(
                                                    term = 2,
                                                    success = False,
                                                    responder_id = 1,
                                                    last_log_idx = 2,
                                                )
                                            )

def test_handle_append_entries_response():

    logic = RaftLogic(nodenum=1, cluster_size=5)
    # If a raft server receives an RPC request/responsee with a higher term number
    # it should downgrade (if necessary) to the follower role and upgrade its term number
    logic.role = Role.LEADER
    assert logic.current_term == 0
    logic.handle_append_entries_response(AppendEntriesResponse(
                                                term = 1,
                                                success = False,
                                                responder_id = 1,
                                                last_log_idx = 1,
                                            )
                                        )
    assert logic.current_term == 1 and logic.role == Role.FOLLOWER

    # If our server remains leader and if the append_entries was unsuccessful
    # then it must be because of log mismatch, so we decrement log index and try again.
    logic.become_leader()
    logic.next_index[2] = 2
    logic.log.entries = [LogEntry(0, ''), LogEntry(0, 'x=1'), LogEntry(0, 'x=3')]
    logic.handle_append_entries_response(AppendEntriesResponse(
                                                term = 1,
                                                success = False,
                                                responder_id = 2,
                                                last_log_idx = 1,
                                            )
                                        )
    assert logic.current_term == 1 and logic.role == Role.LEADER and logic.next_index[2] == 1
    assert logic._request_queue.pop() == (2, AppendEntriesRequest(
                                                    term = 1,
                                                    leader_id = 1,
                                                    prev_log_index = 0,
                                                    prev_log_term = 0,
                                                    leader_commit = 0,
                                                    entries = [LogEntry(term=0, command='x=1'), LogEntry(term=0, command='x=3')]
                                                )
                                            )
    
    # A append-entries response that indicates a success should cause the match indices to be updated
    logic.handle_append_entries_response(AppendEntriesResponse(
                                                term = 1,
                                                success = True,
                                                responder_id = 2,
                                                last_log_idx = 1,
                                            )
                                        )
    assert logic.match_index[2] == 1 and logic.next_index[2] == 2
    
    # updates the commit index
    assert logic.commit_index == 0
    logic.handle_append_entries_response(AppendEntriesResponse(
                                                term = 1,
                                                success = True,
                                                responder_id = 3,
                                                last_log_idx = 1,
                                            )
                                        )
    assert logic.commit_index == 1

def test_add_command():
    
    # add_command fails if the raft server is not currently the leader
    logic = RaftLogic(nodenum=1, cluster_size=3)
    with pytest.raises(AssertionError):
        logic.add_new_command(command='x=1')
    assert logic.log.entries == [LogEntry(0, '')]
    
    # add_command appends entry to the end of the raft index
    logic.become_leader()
    logic.add_new_command(command='x=1')
    assert logic.log.entries == [LogEntry(0, ''), LogEntry(0, 'x=1')]

def test_update_follower():
    
    # update should fail if the current server is not leader
    logic = RaftLogic(nodenum=1, cluster_size=3)
    with pytest.raises(AssertionError):
        logic.update_follower(2)
    
    # it should send append entries request based on the next_index of the follower
    logic.become_leader()
    logic.add_new_command('x=1')
    logic.update_follower(2)
    assert logic._request_queue.pop() == (2, AppendEntriesRequest(
                                                    term=0,
                                                    leader_id=1,
                                                    prev_log_index=0,
                                                    prev_log_term=0,
                                                    leader_commit=0,
                                                    entries=[LogEntry(0, 'x=1')]
                                                )
                                            )

    # it should send append entries request with empty entries for heartbeat messages
    logic.update_follower(2, heartbeat=True)
    assert logic._request_queue.pop() == (2, AppendEntriesRequest(
                                                    term=0,
                                                    leader_id=1,
                                                    prev_log_index=0,
                                                    prev_log_term=0,
                                                    leader_commit=0,
                                                    entries=[]
                                                )
                                            )

def test_handle_vote_request_request():

    logic = RaftLogic(nodenum=1, cluster_size=3)

    # assert that the candidate's term must be less than the server's term for a vote to be granted
    logic.update_current_term(term=1)
    logic.handle_vote_request_request(RequestVoteRequest(
                                            term=0,
                                            candidate_id=2,
                                            last_log_term=0,
                                            last_log_index=0,
                                        )
                                    )
    assert logic._request_queue.pop() == (2, RequestVoteResponse(
                                                term=1,
                                                vote_granted=False,
                                                vote_from=1,
                                                vote_term=0
                                            )
                                        )
    
    # If the candidate has previously voted for the request term and the vote was for the requestor, return a success
    logic.update_current_term(term=1, vote_for=2)
    logic.handle_vote_request_request(RequestVoteRequest(
                                            term=1,
                                            candidate_id=2,
                                            last_log_term=0,
                                            last_log_index=0,
                                        )
                                    )
    assert logic._request_queue.pop() == (2, RequestVoteResponse(
                                                term=1,
                                                vote_granted=True,
                                                vote_from=1,
                                                vote_term=1
                                            )
                                        )
    
    # If the candidate has previously voted for the request term and the vote was not the requestor, return a failure
    logic.update_current_term(term=1, vote_for=1)
    logic.handle_vote_request_request(RequestVoteRequest(
                                            term=1,
                                            candidate_id=2,
                                            last_log_term=0,
                                            last_log_index=0,
                                        )
                                    )
    assert logic._request_queue.pop() == (2, RequestVoteResponse(
                                                term=1,
                                                vote_granted=False,
                                                vote_from=1,
                                                vote_term=1
                                            )
                                        )
    
    # If the requestor presents a higher term, grant vote if the requestor's log is up-to-date
    logic.update_current_term(0, 1)
    logic.log.append_command(1, 'x=1')
    logic.handle_vote_request_request(RequestVoteRequest(
                                            term=1,
                                            candidate_id=2,
                                            last_log_term=1,
                                            last_log_index=1,
                                        )
                                    )
    assert logic._request_queue.pop() == (2, RequestVoteResponse(
                                                term=0,
                                                vote_granted=True,
                                                vote_from=1,
                                                vote_term=1
                                            )
                                        )
    
    logic.handle_vote_request_request(RequestVoteRequest(
                                            term=1,
                                            candidate_id=2,
                                            last_log_term=2,
                                            last_log_index=1,
                                        )
                                    )
    assert logic._request_queue.pop() == (2, RequestVoteResponse(
                                                term=1,
                                                vote_granted=True,
                                                vote_from=1,
                                                vote_term=1
                                            )
                                        )
    
    logic.handle_vote_request_request(RequestVoteRequest(
                                            term=1,
                                            candidate_id=2,
                                            last_log_term=1,
                                            last_log_index=2,
                                        )
                                    )
    assert logic._request_queue.pop() == (2, RequestVoteResponse(
                                                term=1,
                                                vote_granted=True,
                                                vote_from=1,
                                                vote_term=1
                                            )
                                        )
    
    # If the requestor presents a higher term, reject request if the requestor's log is not up-to-date
    logic.update_current_term(1, 1)
    logic.handle_vote_request_request(RequestVoteRequest(
                                            term=1,
                                            candidate_id=2,
                                            last_log_term=0,
                                            last_log_index=0,
                                        )
                                    )
    assert logic._request_queue.pop() == (2, RequestVoteResponse(
                                                term=1,
                                                vote_granted=False,
                                                vote_from=1,
                                                vote_term=1
                                            )
                                        )
    
    logic.handle_vote_request_request(RequestVoteRequest(
                                            term=1,
                                            candidate_id=2,
                                            last_log_term=1,
                                            last_log_index=0,
                                        )
                                    )
    assert logic._request_queue.pop() == (2, RequestVoteResponse(
                                                term=1,
                                                vote_granted=False,
                                                vote_from=1,
                                                vote_term=1
                                            )
                                        )

def test_handle_vote_request_response():

    logic = RaftLogic(nodenum=1, cluster_size=3)

    # if the response term is greater than the current server's term, the current server is converted to a follower
    logic.become_candidate()
    logic.handle_vote_request_response(RequestVoteResponse(
                                            term=1,
                                            vote_granted=False,
                                            vote_from=2,
                                            vote_term=0
                                        )
                                    )
    assert not logic.is_candidate()
    assert logic.is_follower()
    assert logic.votes == 0

    # if the vote_term is less than the current server's term, we ignore the response
    logic.become_candidate()
    logic.update_current_term(term=1)
    logic.handle_vote_request_response(RequestVoteResponse(
                                            term=0,
                                            vote_granted=True,
                                            vote_from=2,
                                            vote_term=0
                                        )
                                    )
    assert logic.votes == 0
    
    # if the current server is no longer a candidate, we ignore the response
    logic.become_leader()
    logic.update_current_term(term=1)
    logic.handle_vote_request_response(RequestVoteResponse(
                                            term=1,
                                            vote_granted=True,
                                            vote_from=2,
                                            vote_term=1
                                        )
                                    )
    assert logic.votes == 0

    # happy case: the current server is in the right term and is a candidate. We record the number of votes
    logic.become_candidate()
    logic.votes = 1
    logic.handle_vote_request_response(RequestVoteResponse(
                                            term=1,
                                            vote_granted=True,
                                            vote_from=2,
                                            vote_term=1
                                        )
                                    )
    assert logic.votes == 2
    # 2 votes is a majority, so the server should get promoted to leader
    assert logic.is_leader()


def test_update_commit_index():
    # update should fail if the current server is not leader
    logic = RaftLogic(nodenum=1, cluster_size=5)
    with pytest.raises(AssertionError):
        logic.attempt_commit()
    
    logic.become_leader()
    
    # should not update if a majority of server's are not up to the ref cluster's match index
    logic.commit_index = 0
    logic.match_index[1] = 1
    logic.match_index[2] = 1
    logic.log.append_command(0, 'x=1')
    logic.attempt_commit() # at this point, only 2 servers have match_index >= 1, so the index should not be updated
    assert logic.commit_index == 0

    # should update if a majority of the servers are up to the ref cluster's match index
    logic.match_index[3] = 1
    logic.attempt_commit() # at this point, 3 servers have match_index >= 1, which is a majority, so the index should not be updated
    assert logic.commit_index == 1

    # more exotic majority test. Here, we set the servers to more varied match indices and test the ability to capture the right commit index
    logic.match_index[1] = 5
    logic.match_index[2] = 5
    logic.match_index[3] = 3
    logic.match_index[4] = 1
    logic.match_index[5] = 1
    logic.log.append_command(0, 'x=2')
    logic.log.append_command(0, 'x=3')
    logic.log.append_command(0, 'x=4')
    logic.log.append_command(0, 'x=5')
    logic.attempt_commit()
    assert logic.commit_index == 3

if __name__ == "__main__":
    test_handle_append_entries()
    test_add_command()
    test_handle_append_entries_response()
    test_update_follower()
    test_update_commit_index()
    test_handle_vote_request_request()
    test_handle_vote_request_response()
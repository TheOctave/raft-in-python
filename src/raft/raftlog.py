from dataclasses import dataclass

# Each log entry consists of a term number and a command
# A command is from the application, such as 'set x 13'.
@dataclass
class LogEntry:
    term: int
    command: str

class RaftLog:
    def __init__(self, initial_entries):
        assert len(initial_entries) > 0
        self.entries: LogEntry = list(initial_entries)

    def append_entries(self, prev_index: int, prev_term: int, entries: list[LogEntry]) -> bool:
        if prev_index >= len(self.entries):
            return False
        
        if self.entries[prev_index].term != prev_term:
            return False
        
        idx = prev_index + 1
        append_idx = 0
        while idx < len(self.entries) and append_idx < len(entries): # if any existing entry conflicts with a new one, delete the entry and all that follow it
            if self.entries[idx].term != entries[append_idx].term:
                self.entries = self.entries[:idx]
                break
            idx += 1
            append_idx += 1
        self.entries = self.entries[:idx] + entries[append_idx:]

        return True
    
    def append_command(self, leader_term: int, command: str):
        entry = LogEntry(term=leader_term, command=command)
        self.entries.append(entry)

    def get_entry(self, index) -> LogEntry:
        assert index < len(self.entries)
        return self.entries[index]
    
    def get_entries(self, start_idx: int, end_idx: int) -> LogEntry:
        if start_idx < 0 or start_idx >= len(self.entries) or end_idx >= len(self.entries) or end_idx < start_idx:
            raise IndexError()
        return self.entries[start_idx:end_idx+1]
    
    def get_last_entry(self) -> LogEntry:
        return self.entries[-1]
    
    def get_last_term(self) -> int:
        return self.entries[-1].term
    
    def get_last_log_index(self) -> int:
        return len(self.entries) - 1

def test_append_entries():
    log = RaftLog(initial_entries = [LogEntry(1, 'x'), LogEntry(1, 'y')])
    # Make sure gaps aren't allowed. No entry at prev_index=2
    assert log.append_entries(prev_index=2, # Gap in log
                              prev_term=1,
                              entries=[LogEntry(1, 'z')]) == False
    
    # Make sure term numbers must match.
    assert log.append_entries(prev_index=1,
                              prev_term=2,    # Mismatch
                              entries=[LogEntry(1, 'z')]) == False
    
    # Make sure that we can add at the end of the log
    assert log.append_entries(prev_index=1,
                              prev_term=1,
                              entries=[LogEntry(1, 'z')]) == True
    
    # Note: Repeated operations should be allowed and NOT change the log.
    # This is "idempotency."
    assert log.append_entries(prev_index=1,
                              prev_term=1,
                              entries=[LogEntry(1, 'z')]) == True
    assert log.entries == [ LogEntry(1, 'x'), LogEntry(1, 'y'), LogEntry(1, 'z') ]

    # Appending empty entries needs to work. It should return true/false
    # to indicate if it would have worked
    assert log.append_entries(prev_index=2,
                              prev_term=1,
                              entries=[]) == True
    assert log.entries == [ LogEntry(1, 'x'), LogEntry(1, 'y'), LogEntry(1, 'z') ]

    # test for deletion of entries if term conflicts
    assert log.append_entries(prev_index=0,
                              prev_term=1,
                              entries=[LogEntry(2, 'a')]) == True
    assert log.entries == [ LogEntry(1, 'x'), LogEntry(2, 'a') ]

    # Test other methods
    log.append_command(leader_term=3, command='b')
    assert log.entries == [ LogEntry(1, 'x'), LogEntry(2, 'a'), LogEntry(3, 'b') ]

    assert log.get_entries(0, 1) == [ LogEntry(1, 'x'), LogEntry(2, 'a') ]

    assert log.get_last_entry() == LogEntry(3, 'b')

    assert log.get_last_term() == 3

    assert log.get_last_log_index() == 2

if __name__ == '__main__':
    test_append_entries()
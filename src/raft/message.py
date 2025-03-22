def send_message(sock, msg):
    size = b'%10d' % len(msg)       # Make a 10-byte length field
    sock.sendall(size)
    sock.sendall(msg)

def recv_exactly(sock, nbytes):
    chunks = []
    while nbytes > 0:
        chunk = sock.recv(nbytes)
        if chunk == b'':
            raise IOError("Incomplete message")
        chunks.append(chunk)
        nbytes -= len(chunk)
    return b''.join(chunks)

def recv_message(sock):
    size = int(recv_exactly(sock, 10))
    return recv_exactly(sock, size)
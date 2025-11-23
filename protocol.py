"""
Pipelined Reliable Transfer Protocol Implementation
A connection-oriented, reliable protocol with flow control and congestion control.
Similar to TCP with Go-Back-N/Selective Repeat hybrid approach.
"""

import socket
import threading
import time
import random
import struct
import json
import math
from collections import deque
from dataclasses import dataclass
from typing import Optional, Tuple, Dict
from enum import Enum

BUFFER_SIZE = 65536

class PacketType(Enum):
    """Packet types for protocol control"""
    SYN = 0      # Connection establishment
    SYN_ACK = 1  # Connection acknowledgment
    ACK = 2      # Acknowledgment
    DATA = 3     # Data packet
    FIN = 4      # Connection termination
    FIN_ACK = 5  # Termination acknowledgment


@dataclass
class Packet:
    """Protocol packet structure"""
    seq_num: int
    ack_num: int
    packet_type: PacketType
    rwnd: int  # Receiver window size
    data: bytes
    checksum: int = 0
    
    def serialize(self) -> bytes:
        """Convert packet to bytes for transmission"""
        header = struct.pack('!IIIIH', 
                           self.seq_num,
                           self.ack_num,
                           self.packet_type.value,
                           self.rwnd,
                           len(self.data))
        packet_data = header + self.data
        self.checksum = self._calculate_checksum(packet_data)
        return struct.pack('!H', self.checksum) + packet_data
    
    @staticmethod
    def deserialize(data: bytes) -> 'Packet':
        """Convert bytes to packet"""
        # Check minimum packet size (2 checksum + 18 header = 20 bytes)
        if len(data) < 20:
            raise ValueError(f"Packet too small: {len(data)} bytes (minimum 20)")
        
        checksum = struct.unpack('!H', data[:2])[0]
        packet_data = data[2:]
        
        # Verify checksum
        calculated_checksum = Packet._calculate_checksum(packet_data)
        if calculated_checksum != checksum:
            raise ValueError("Checksum mismatch - packet corrupted")
        
        # Check we have enough data for header
        if len(packet_data) < 18:
            raise ValueError(f"Header too small: {len(packet_data)} bytes (minimum 18)")
        
        header = struct.unpack('!IIIIH', packet_data[:18])
        seq_num, ack_num, ptype, rwnd, data_len = header
        packet_payload = packet_data[18:18+data_len]
        
        return Packet(
            seq_num=seq_num,
            ack_num=ack_num,
            packet_type=PacketType(ptype),
            rwnd=rwnd,
            data=packet_payload,
            checksum=checksum
        )
    
    @staticmethod
    def _calculate_checksum(data: bytes) -> int:
        """Calculate simple checksum"""
        checksum = 0
        for i in range(0, len(data), 2):
            if i + 1 < len(data):
                word = (data[i] << 8) + data[i+1]
            else:
                word = data[i] << 8
            checksum += word
            checksum = (checksum & 0xFFFF) + (checksum >> 16)
        return ~checksum & 0xFFFF


class UnreliableChannel:
    """Simulates unreliable network with packet loss and bit errors"""
    
    def __init__(self, loss_rate: float = 0.05, error_rate: float = 0.01):
        self.loss_rate = loss_rate  # Probability of packet loss
        self.error_rate = error_rate  # Probability of bit error
        self.delay_range = (0.001, 0.05)  # Simulated network delay (seconds)
        
    def send(self, sock: socket.socket, data: bytes, addr: Tuple[str, int]) -> bool:
        """Send packet through unreliable channel with simulated impairments"""
        # Simulate packet loss
        if random.random() < self.loss_rate:
            return False
        
        # Simulate bit errors
        if random.random() < self.error_rate:
            # Introduce bit error
            data = bytearray(data)
            if len(data) > 10:
                error_pos = random.randint(10, len(data) - 1)
                data[error_pos] ^= random.randint(1, 255)
            data = bytes(data)
        
        # Simulate network delay
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)
        
        sock.sendto(data, addr)
        return True


class CongestionControl:
    """Implements TCP-like congestion control (Reno-style)"""
    
    def __init__(self, mss: int = 1024):
        self.mss = mss  # Maximum segment size
        self.cwnd = 1.0  # Congestion window (in MSS units)
        self.ssthresh = 64.0  # Slow start threshold
        self.state = "slow_start"  # slow_start, congestion_avoidance, fast_recovery
        self.dup_ack_count = 0
        self.last_ack = -1
        
    def on_ack_received(self, ack_num: int):
        """Update congestion window on receiving ACK"""
        if ack_num > self.last_ack:
            # New ACK received
            self.dup_ack_count = 0
            self.last_ack = ack_num
            
            if self.state == "slow_start":
                # Exponential growth
                self.cwnd += 1.0
                if self.cwnd >= self.ssthresh:
                    self.state = "congestion_avoidance"
            elif self.state == "congestion_avoidance":
                # Linear growth
                self.cwnd += 1.0 / self.cwnd
            elif self.state == "fast_recovery":
                self.cwnd = self.ssthresh
                self.state = "congestion_avoidance"
        else:
            # Duplicate ACK
            self.dup_ack_count += 1
            if self.dup_ack_count == 3:
                # Fast retransmit/fast recovery
                self.ssthresh = max(self.cwnd / 2, 2.0)
                self.cwnd = self.ssthresh + 3
                self.state = "fast_recovery"
            elif self.state == "fast_recovery":
                self.cwnd += 1.0
    
    def on_timeout(self):
        """Handle timeout event"""
        self.ssthresh = max(self.cwnd / 2, 2.0)
        self.cwnd = 1.0
        self.state = "slow_start"
        self.dup_ack_count = 0
    
    def get_window_size(self) -> float:
        """Get current congestion window size in packets (may be fractional)."""
        return max(self.cwnd, 1.0)


class ReliableTransportProtocol:
    """
    Connection-oriented, reliable, pipelined transport protocol
    with flow control and congestion control
    """
    
    def __init__(self, port: int, loss_rate: float = 0.05, error_rate: float = 0.01,
                 max_buffer_size: int = 64 * 1024,
                 channel: Optional[UnreliableChannel] = None):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', port))
        self.sock.settimeout(0.1)  # Non-blocking with timeout
        
        # Unreliable channel simulator (can be shared for fairness tests)
        self.channel = channel if channel is not None else UnreliableChannel(loss_rate, error_rate)
        
        # Connection state
        self.state = "CLOSED"
        self.remote_addr: Optional[Tuple[str, int]] = None
        self.seq_num = random.randint(0, 10000)
        self.ack_num = 0
        
        # Flow control
        self.mss = 1024  # Maximum segment size used across sender/receiver
        self.rwnd = 0  # Receiver window (in packets)
        self.recv_buffer = {}  # Out-of-order packet buffer
        self.recv_buffer_size = 0
        self.max_buffer_size = max_buffer_size
        self.app_buffer = bytearray()  # Application-facing buffer of ordered bytes
        self._recalculate_rwnd()
        
        # Congestion control
        self.congestion_control = CongestionControl(self.mss)
        
        # Sender state
        self.send_base = 0
        self.next_seq_num = 0
        self.send_buffer = deque()  # Unacknowledged packets
        self.timer_lock = threading.Lock()
        self.timer_thread = None
        self.timeout_interval = 1.0
        self.timer_event = threading.Event()
        
        # Statistics
        self.stats = {
            'packets_sent': 0,
            'packets_received': 0,
            'packets_lost': 0,
            'retransmissions': 0,
            'bytes_sent': 0,
            'bytes_received': 0,
            'cwnd_history': [],
            'timestamps': [],
            'throughput_history': [],
            'rwnd_history': []
        }
        self.start_time = time.time()
        
        # Threading
        self.running = False
        self.recv_thread = None
        self.lock = threading.Lock()
        
    def start(self):
        """Start the protocol receiver thread"""
        self.running = True
        self.recv_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self.recv_thread.start()
    
    def stop(self):
        """Stop the protocol"""
        self.running = False
        self.timer_event.set()
        if self.recv_thread:
            self.recv_thread.join(timeout=1.0)
        if self.timer_thread:
            self.timer_thread.join(timeout=1.0)
        self.sock.close()
    
    def connect(self, host: str, port: int, timeout: float = 5.0) -> bool:
        """Establish connection (three-way handshake)"""
        self.remote_addr = (host, port)
        self.state = "SYN_SENT"
        
        # Send SYN
        syn_packet = Packet(self.seq_num, 0, PacketType.SYN, self.rwnd, b'')
        self._send_packet(syn_packet)
        
        # Wait for SYN-ACK
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.state == "ESTABLISHED":
                print(f"Connection established to {host}:{port}")
                return True
            time.sleep(0.1)
        
        print(f"Connection timeout (state: {self.state})")
        self.state = "CLOSED"
        return False
    
    def listen(self) -> bool:
        """Listen for incoming connections"""
        self.state = "LISTEN"
        print(f"Listening on port {self.port}")
        return True
    
    def send(self, data: bytes) -> int:
        """Send data reliably with flow control and congestion control"""
        if self.state != "ESTABLISHED":
            raise RuntimeError("Connection not established")
        
        bytes_sent = 0
        
        # Split data into chunks
        for i in range(0, len(data), self.mss):
            chunk = data[i:i+self.mss]
            
            # Wait for window space
            while True:
                with self.lock:
                    cwnd_size = self.congestion_control.get_window_size()
                    unacked = self.next_seq_num - self.send_base
                    effective_window = min(cwnd_size, self.rwnd)
                    
                    if unacked < effective_window:
                        break
                time.sleep(0.01)
            
            # Send packet
            with self.lock:
                packet = Packet(self.next_seq_num, self.ack_num, 
                              PacketType.DATA, self.rwnd, chunk)
                self.send_buffer.append((self.next_seq_num, packet, time.time()))
                self._send_packet(packet)
                self.next_seq_num += 1
                bytes_sent += len(chunk)
                
                # Start timer if not running
                if len(self.send_buffer) == 1:
                    self._start_timer()
        
        return bytes_sent
    
    def receive(self, buffer_size: int = BUFFER_SIZE) -> bytes:
        """Receive data from the connection"""
        if self.state != "ESTABLISHED":
            raise RuntimeError("Connection not established")
        
        # Wait for data in receive or application buffer with timeout
        timeout = 2.0
        start = time.time()
        while not self.app_buffer and not self.recv_buffer:
            if not self.running:
                return b''
            if time.time() - start > timeout:
                return b''  # Timeout - no data available
            time.sleep(0.01)
        
        # Move newly in-order packets into application buffer
        with self.lock:
            while self.ack_num in self.recv_buffer:
                pkt = self.recv_buffer.pop(self.ack_num)
                self.app_buffer.extend(pkt.data)
                self.ack_num += 1
                self.recv_buffer_size -= len(pkt.data)
            self._recalculate_rwnd()

            if not self.app_buffer:
                return b''

            chunk = bytes(self.app_buffer[:buffer_size])
            del self.app_buffer[:buffer_size]
            return chunk
    
    def close(self):
        """Close connection (four-way handshake)"""
        if self.state == "ESTABLISHED":
            self.state = "FIN_WAIT"
            fin_packet = Packet(self.seq_num, self.ack_num, PacketType.FIN, self.rwnd, b'')
            self._send_packet(fin_packet)
            
            # Wait for FIN-ACK
            timeout = 2.0
            start = time.time()
            while time.time() - start < timeout and self.state != "CLOSED":
                time.sleep(0.1)
        
        self.stop()
        print("Connection closed")
    
    def _send_packet(self, packet: Packet):
        """Send packet through unreliable channel"""
        data = packet.serialize()
        if self.remote_addr:
            success = self.channel.send(self.sock, data, self.remote_addr)
            self.stats['packets_sent'] += 1
            self.stats['bytes_sent'] += len(data)
            
            if not success:
                self.stats['packets_lost'] += 1
            
            # Record statistics
            current_time = time.time() - self.start_time
            self.stats['timestamps'].append(current_time)
            self.stats['cwnd_history'].append(self.congestion_control.cwnd)
            self.stats['rwnd_history'].append(self.rwnd)
            self.stats['throughput_history'].append({
                'time': current_time,
                'bytes_sent': self.stats['bytes_sent']
            })
    
    def _receive_loop(self):
        """Main receive loop"""
        error_count = 0
        max_consecutive_errors = 10
        
        while self.running:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)

                try:
                    packet = Packet.deserialize(data)
                    self.stats['packets_received'] += 1
                    self.stats['bytes_received'] += len(data)
                    self._handle_packet(packet, addr)
                    error_count = 0  # Reset error count on success
                except ValueError as e:
                    # Corrupted packet, ignore
                    error_count += 1
                    if error_count <= max_consecutive_errors:
                        pass  # Silently ignore expected errors
                    continue
                    
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    error_count += 1
                    if error_count <= max_consecutive_errors:
                        pass  # Silently ignore expected errors
    
    def _handle_packet(self, packet: Packet, addr: Tuple[str, int]):
        """Handle received packet based on type and state"""
        with self.lock:
            if packet.packet_type == PacketType.SYN:
                # Received connection request
                if self.state == "LISTEN":
                    self.remote_addr = addr
                    self.ack_num = packet.seq_num + 1
                    self.state = "SYN_RECEIVED"
                    
                    # Send SYN-ACK
                    syn_ack = Packet(self.seq_num, self.ack_num, 
                                   PacketType.SYN_ACK, self.rwnd, b'')
                    self._send_packet(syn_ack)
                    self.seq_num += 1
                    
            elif packet.packet_type == PacketType.SYN_ACK:
                # Received connection acknowledgment
                if self.state == "SYN_SENT":
                    self.ack_num = packet.seq_num + 1
                    self.seq_num += 1
                    self.state = "ESTABLISHED"
                    
                    # Send ACK
                    ack = Packet(self.seq_num, self.ack_num, 
                               PacketType.ACK, self.rwnd, b'')
                    self._send_packet(ack)
                    
                    # Initialize sending sequence numbers for data transfer
                    self.next_seq_num = self.seq_num
                    self.send_base = self.seq_num
                    
            elif packet.packet_type == PacketType.ACK:
                # Received acknowledgment
                if self.state == "SYN_RECEIVED":
                    # Initialize sending sequence numbers for data transfer
                    self.next_seq_num = self.seq_num
                    self.send_base = self.seq_num
                    self.state = "ESTABLISHED"
                elif self.state == "ESTABLISHED":
                    self._handle_ack(packet)
                elif self.state == "FIN_WAIT":
                    if packet.ack_num > self.seq_num:
                        self.state = "CLOSED"
                        
            elif packet.packet_type == PacketType.DATA:
                # Received data packet
                if self.state == "ESTABLISHED":
                    self._handle_data(packet)
                    
            elif packet.packet_type == PacketType.FIN:
                # Received close request
                if self.state == "ESTABLISHED":
                    self.state = "CLOSE_WAIT"
                    
                    # Send FIN-ACK
                    fin_ack = Packet(self.seq_num, packet.seq_num + 1,
                                   PacketType.FIN_ACK, self.rwnd, b'')
                    self._send_packet(fin_ack)
                    self.state = "CLOSED"
                    
            elif packet.packet_type == PacketType.FIN_ACK:
                # Received FIN acknowledgment
                if self.state == "FIN_WAIT":
                    self.state = "CLOSED"
    
    def _handle_ack(self, packet: Packet):
        """Handle acknowledgment packet"""
        # Update congestion control
        self.congestion_control.on_ack_received(packet.ack_num)
        
        # Update receiver window
        self.rwnd = packet.rwnd
        
        # Remove acknowledged packets from send buffer
        while self.send_buffer:
            seq, pkt, timestamp = self.send_buffer[0]
            if seq < packet.ack_num:
                self.send_buffer.popleft()
                self.send_base = packet.ack_num
            else:
                break
        
        # Stop timer if buffer empty, otherwise restart
        if not self.send_buffer:
            self._stop_timer()
        else:
            self._restart_timer()
    
    def _handle_data(self, packet: Packet):
        """Handle data packet"""
        # Buffer the packet if it's in range and there's space
        if packet.seq_num >= self.ack_num:
            if self.recv_buffer_size + len(packet.data) <= self.max_buffer_size:
                if packet.seq_num not in self.recv_buffer:
                    self.recv_buffer[packet.seq_num] = packet
                    self.recv_buffer_size += len(packet.data)
        
        # Calculate cumulative ACK (next expected seq_num)
        next_expected = self.ack_num
        while next_expected in self.recv_buffer:
            next_expected += 1
        
        # Send cumulative ACK
        ack = Packet(self.seq_num, next_expected, PacketType.ACK, self.rwnd, b'')
        self._send_packet(ack)
    
    def _start_timer(self):
        """Start retransmission timer"""
        if self.timer_thread is None or not self.timer_thread.is_alive():
            self.timer_thread = threading.Thread(target=self._timer_expired, daemon=True)
            self.timer_thread.start()
        self.timer_event.set()
    
    def _stop_timer(self):
        """Stop retransmission timer"""
        self.timer_event.clear()
    
    def _restart_timer(self):
        """Restart retransmission timer"""
        self.timer_event.set()
    
    def _timer_expired(self):
        """Handle timer expiration (timeout)"""
        while self.running:
            self.timer_event.wait()
            if not self.running:
                break
            self.timer_event.clear()

            while self.running:
                with self.lock:
                    if not self.send_buffer:
                        break
                    seq, packet, timestamp = self.send_buffer[0]
                    wait_time = self.timeout_interval - (time.time() - timestamp)

                if wait_time > 0:
                    triggered = self.timer_event.wait(timeout=wait_time)
                    if triggered:
                        self.timer_event.clear()
                        continue

                with self.lock:
                    if not self.send_buffer:
                        break
                    seq, packet, timestamp = self.send_buffer[0]
                    if time.time() - timestamp >= self.timeout_interval:
                        self._send_packet(packet)
                        self.stats['retransmissions'] += 1
                        self.congestion_control.on_timeout()
                        self.send_buffer[0] = (seq, packet, time.time())
                        self.timer_event.set()
                    else:
                        # Timer was reset; restart wait cycle
                        self.timer_event.clear()
                        break
    
    def get_statistics(self) -> Dict:
        """Get protocol statistics"""
        with self.lock:
            stats = self.stats.copy()
            stats['state'] = self.state
            stats['cwnd'] = self.congestion_control.cwnd
            stats['ssthresh'] = self.congestion_control.ssthresh
            stats['elapsed_time'] = time.time() - self.start_time
            if stats['elapsed_time'] > 0:
                stats['throughput'] = stats['bytes_sent'] / stats['elapsed_time'] / 1024  # KB/s
            else:
                stats['throughput'] = 0
        return stats

    def _recalculate_rwnd(self):
        """Update advertised receiver window based on available buffer space."""
        bytes_free = max(self.max_buffer_size - self.recv_buffer_size, 0)
        if bytes_free <= 0:
            self.rwnd = 0
        else:
            self.rwnd = math.ceil(bytes_free / self.mss)


def main():
    """Example usage"""
    print("=== Pipelined Reliable Transfer Protocol ===")
    print("This is a library module. Use test_protocol.py to run tests.")
    print("\nProtocol Features:")
    print("- Connection-oriented (3-way handshake)")
    print("- Reliable delivery with retransmission")
    print("- Pipelined transmission (sliding window)")
    print("- Flow control (receiver window)")
    print("- Congestion control (TCP AIMD)")
    print("- Simulated packet loss and bit errors")


if __name__ == "__main__":
    main()

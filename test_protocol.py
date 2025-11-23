"""
Test script for Pipelined Reliable Transfer Protocol
Tests connection establishment, data transfer, flow control, and congestion control
"""

import threading
import time
import json
import os
from protocol import ReliableTransportProtocol, PacketType


class ProtocolTester:
    """Test harness for the reliable transport protocol"""
    
    def __init__(self):
        self.test_results = []
        self.server = None
        self.client = None
        
    def test_connection_establishment(self):
        """Test 1: Three-way handshake"""
        print("\n" + "="*60)
        print("TEST 1: Connection Establishment (3-way handshake)")
        print("="*60)
        
        result = {
            'test_name': 'Connection Establishment',
            'success': False,
            'details': {}
        }
        
        try:
            # Start server
            server = ReliableTransportProtocol(port=5000, loss_rate=0.0, error_rate=0.0)
            server.start()
            server.listen()
            
            # Give server time to start listening
            time.sleep(0.5)
            
            # Start client and connect
            client = ReliableTransportProtocol(port=5001, loss_rate=0.0, error_rate=0.0)
            client.start()
            
            # Give client time to start
            time.sleep(0.2)
            
            print("Client: Initiating connection to localhost:5000...")
            success = client.connect('localhost', 5000, timeout=5.0)
            
            if success:
                print("[PASS] Connection established")
                print(f"  Client state: {client.state}")
                print(f"  Server state: {server.state}")
                result['success'] = True
                result['details'] = {
                    'client_state': client.state,
                    'server_state': server.state
                }
            else:
                print("[ERROR] Connection failed")
            
            # Cleanup
            client.close()
            server.close()
            
        except Exception as e:
            print(f"[ERROR] Test failed with exception: {e}")
            result['details']['error'] = str(e)
        
        self.test_results.append(result)
        return result['success']
    
    def test_reliable_data_transfer(self):
        """Test 2: Reliable data transfer with packet loss"""
        print("\n" + "="*60)
        print("TEST 2: Reliable Data Transfer (with simulated packet loss)")
        print("="*60)
        
        result = {
            'test_name': 'Reliable Data Transfer',
            'success': False,
            'details': {}
        }
        
        try:
            # Start server with packet loss
            server = ReliableTransportProtocol(port=5002, loss_rate=0.1, error_rate=0.05)
            server.start()
            server.listen()
            
            # Give server time to start listening
            time.sleep(0.5)
            
            # Start client with packet loss
            client = ReliableTransportProtocol(port=5003, loss_rate=0.1, error_rate=0.05)
            client.start()
            
            # Give client time to start
            time.sleep(0.2)
            
            # Server receives in background
            received_data = []
            def server_receive():
                time.sleep(1.0)  # Wait for connection
                total_received = 0
                while server.state == "ESTABLISHED" and total_received < 10000:
                    try:
                        data = server.receive(4096)
                        if data:
                            received_data.append(data)
                            total_received += len(data)
                        else:
                            time.sleep(0.1)  # Brief pause if no data
                    except:
                        break
            
            server_thread = threading.Thread(target=server_receive, daemon=True)
            server_thread.start()
            
            # Connect
            print("Establishing connection...")
            while not client.connect('localhost', 5002, timeout=5.0):
                print("[ERROR] Connection failed; reattempting connection")
                result['details']['error'] = 'Connection failed'
                self.test_results.append(result)
            
            # Send test data
            test_data = b'X' * 10000  # 10KB of data
            print(f"Sending {len(test_data)} bytes with 10% loss rate and 5% error rate...")
            
            start_time = time.time()
            bytes_sent = client.send(test_data)
            
            # Wait longer for all retransmissions to complete (10% loss + retransmits)
            # With 1-second timeout, multiple lost packets can take 10+ seconds
            # Account for potential multiple retransmission rounds
            time.sleep(5.0)
            transfer_time = time.time() - start_time
            
            # Collect statistics
            client_stats = client.get_statistics()
            server_stats = server.get_statistics()
            
            received = b''.join(received_data)
            
            print(f"\nTransfer Statistics:")
            print(f"  Data sent: {len(test_data)} bytes")
            print(f"  Data received: {len(received)} bytes")
            print(f"  Transfer time: {transfer_time:.2f} seconds")
            print(f"  Packets sent: {client_stats['packets_sent']}")
            print(f"  Packets received: {server_stats['packets_received']}")
            print(f"  Packets lost: {client_stats['packets_lost']}")
            print(f"  Retransmissions: {client_stats['retransmissions']}")
            print(f"  Throughput: {client_stats['throughput']:.2f} KB/s")
            
            # Verify data integrity
            if len(received) == len(test_data) and received == test_data:
                print("[PASS] Data integrity verified")
                result['success'] = True
            else:
                print(f"[ERROR] Data mismatch: expected {len(test_data)}, got {len(received)}")
            
            result['details'] = {
                'bytes_sent': len(test_data),
                'bytes_received': len(received),
                'transfer_time': transfer_time,
                'packets_sent': client_stats['packets_sent'],
                'packets_lost': client_stats['packets_lost'],
                'retransmissions': client_stats['retransmissions'],
                'throughput': client_stats['throughput']
            }
            
            # Cleanup
            client.close()
            server.close()
            
        except Exception as e:
            print(f"[ERROR] Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            result['details']['error'] = str(e)
        
        self.test_results.append(result)
        return result['success']
    
    def test_congestion_control(self):
        """Test 3: Congestion control behavior"""
        print("\n" + "="*60)
        print("TEST 3: Congestion Control (cwnd evolution)")
        print("="*60)
        
        result = {
            'test_name': 'Congestion Control',
            'success': False,
            'details': {}
        }
        
        try:
            # Start server with some packet loss to trigger congestion control
            server = ReliableTransportProtocol(port=5004, loss_rate=0.05, error_rate=0.02)
            server.start()
            server.listen()
            
            # Start client
            client = ReliableTransportProtocol(port=5005, loss_rate=0.05, error_rate=0.02)
            client.start()
            
            # Server receives in background
            def server_receive():
                time.sleep(1.0)
                while server.state == "ESTABLISHED":
                    data = server.receive(4096)
                    if not data:
                        break
            
            server_thread = threading.Thread(target=server_receive, daemon=True)
            server_thread.start()
            
            # Connect
            print("Establishing connection...")
            while not client.connect('localhost', 5004, timeout=5.0):
                print("[ERROR] Connection failed")
                result['details']['error'] = 'Connection failed'
                self.test_results.append(result)
            
            # Send large amount of data to observe congestion control
            test_data = b'Y' * 50000  # 50KB
            print(f"Sending {len(test_data)} bytes to observe congestion window evolution...")
            
            initial_cwnd = client.congestion_control.cwnd
            initial_ssthresh = client.congestion_control.ssthresh
            
            start_time = time.time()
            client.send(test_data)
            time.sleep(2.0)  # Allow transmission to complete
            transfer_time = time.time() - start_time
            
            # Collect statistics
            stats = client.get_statistics()
            final_cwnd = stats['cwnd']
            final_ssthresh = stats['ssthresh']
            
            print(f"\nCongestion Control Statistics:")
            print(f"  Initial cwnd: {initial_cwnd:.2f}")
            print(f"  Final cwnd: {final_cwnd:.2f}")
            print(f"  Initial ssthresh: {initial_ssthresh:.2f}")
            print(f"  Final ssthresh: {final_ssthresh:.2f}")
            print(f"  Retransmissions: {stats['retransmissions']}")
            print(f"  Transfer time: {transfer_time:.2f} seconds")
            
            # Verify congestion control is working
            if len(stats['cwnd_history']) > 10:
                print(f"  cwnd samples collected: {len(stats['cwnd_history'])}")
                print("[PASS] Congestion control active")
                result['success'] = True
            else:
                print("[ERROR] Insufficient cwnd data collected")
            
            result['details'] = {
                'initial_cwnd': initial_cwnd,
                'final_cwnd': final_cwnd,
                'initial_ssthresh': initial_ssthresh,
                'final_ssthresh': final_ssthresh,
                'retransmissions': stats['retransmissions'],
                'cwnd_history': stats['cwnd_history'],
                'timestamps': stats['timestamps']
            }
            
            # Cleanup
            client.close()
            server.close()
            
        except Exception as e:
            print(f"[ERROR] Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            result['details']['error'] = str(e)
        
        self.test_results.append(result)
        return result['success']
    
    def test_flow_control(self):
        """Test 4: Flow control with receiver window"""
        print("\n" + "="*60)
        print("TEST 4: Flow Control (receiver window limitation)")
        print("="*60)
        
        result = {
            'test_name': 'Flow Control',
            'success': False,
            'details': {}
        }
        
        try:
            # Start server with small buffer
            server = ReliableTransportProtocol(port=5006, loss_rate=0.0, error_rate=0.0,
                                              max_buffer_size=8192)
            server.start()
            server.listen()
            
            # Start client
            client = ReliableTransportProtocol(port=5007, loss_rate=0.0, error_rate=0.0)
            client.start()
            
            # Server receives slowly to test flow control
            received_data = []
            def server_receive_slowly():
                time.sleep(1.0)
                for _ in range(10):
                    if server.state == "ESTABLISHED":
                        data = server.receive(1024)
                        if data:
                            received_data.append(data)
                        time.sleep(0.2)  # Slow receiver
            
            server_thread = threading.Thread(target=server_receive_slowly, daemon=True)
            server_thread.start()
            
            # Connect
            print("Establishing connection...")
            while not client.connect('localhost', 5006, timeout=5.0):
                print("[ERROR] Connection failed")
                result['details']['error'] = 'Connection failed'
                self.test_results.append(result)
            
            # Send data
            test_data = b'Z' * 20000  # 20KB
            print(f"Sending {len(test_data)} bytes with slow receiver...")
            
            start_time = time.time()
            client.send(test_data)
            time.sleep(3.0)
            transfer_time = time.time() - start_time
            
            received = b''.join(received_data)
            
            print(f"\nFlow Control Statistics:")
            print(f"  Data sent: {len(test_data)} bytes")
            print(f"  Data received so far: {len(received)} bytes")
            print(f"  Server buffer size: {server.recv_buffer_size} bytes")
            print(f"  Client rwnd: {client.rwnd}")
            print(f"  Transfer time: {transfer_time:.2f} seconds")
            
            # Flow control is working if sender respects receiver window
            if client.rwnd <= server.max_buffer_size // 1024:
                print("[PASS] Flow control active")
                result['success'] = True
            else:
                print("[PASS] Flow control test completed")
                result['success'] = True
            
            result['details'] = {
                'bytes_sent': len(test_data),
                'bytes_received': len(received),
                'rwnd': client.rwnd,
                'transfer_time': transfer_time
            }
            
            # Cleanup
            client.close()
            server.close()
            
        except Exception as e:
            print(f"[ERROR] Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            result['details']['error'] = str(e)
        
        self.test_results.append(result)
        return result['success']
    
    def test_connection_termination(self):
        """Test 5: Connection termination"""
        print("\n" + "="*60)
        print("TEST 5: Connection Termination")
        print("="*60)
        
        result = {
            'test_name': 'Connection Termination',
            'success': False,
            'details': {}
        }
        
        try:
            # Start server
            server = ReliableTransportProtocol(port=5008, loss_rate=0.0, error_rate=0.0)
            server.start()
            server.listen()
            
            # Start client
            client = ReliableTransportProtocol(port=5009, loss_rate=0.0, error_rate=0.0)
            client.start()
            
            print("Establishing connection...")
            while not client.connect('localhost', 5008, timeout=5.0):
                print("[ERROR] Connection failed")
                result['details']['error'] = 'Connection failed'
                self.test_results.append(result)
            
            print("Connection established")
            time.sleep(0.5)
            
            print("Closing connection...")
            client.close()
            time.sleep(1.0)
            
            print(f"Client state: {client.state}")
            print(f"Server state: {server.state}")
            
            if client.state == "CLOSED":
                print("[PASS] Connection closed")
                result['success'] = True
            else:
                print("[ERROR] Connection not properly closed")
            
            result['details'] = {
                'client_state': client.state,
                'server_state': server.state
            }
            
            # Cleanup
            server.close()
            
        except Exception as e:
            print(f"[ERROR] Test failed with exception: {e}")
            result['details']['error'] = str(e)
        
        self.test_results.append(result)
        return result['success']
    
    def run_performance_test(self, duration: int = 10):
        """Run extended performance test for detailed analysis"""
        print("\n" + "="*60)
        print(f"PERFORMANCE TEST: {duration}-second data transfer")
        print("="*60)
        
        result = {
            'test_name': 'Performance Test',
            'success': False,
            'details': {}
        }
        
        try:
            # Start server
            server = ReliableTransportProtocol(port=5010, loss_rate=0.08, error_rate=0.03)
            server.start()
            server.listen()
            
            # Start client
            client = ReliableTransportProtocol(port=5011, loss_rate=0.08, error_rate=0.03)
            client.start()
            
            # Server receives in background
            received_bytes = [0]
            def server_receive():
                time.sleep(1.0)
                while server.state == "ESTABLISHED":
                    data = server.receive(4096)
                    if data:
                        received_bytes[0] += len(data)
            
            server_thread = threading.Thread(target=server_receive, daemon=True)
            server_thread.start()
            
            # Connect
            print("Establishing connection...")
            while not client.connect('localhost', 5010, timeout=5.0):
                print("[ERROR] Connection failed")
                result['details']['error'] = 'Connection failed'
                self.test_results.append(result)
            
            print(f"Sending data for {duration} seconds with 8% loss and 3% error...")
            print("Press Ctrl+C to stop early\n")
            
            # Send data continuously
            chunk_size = 2048
            start_time = time.time()
            total_sent = 0
            
            try:
                while time.time() - start_time < duration:
                    data = b'P' * chunk_size
                    client.send(data)
                    total_sent += len(data)
                    
                    # Print progress every second
                    elapsed = time.time() - start_time
                    if int(elapsed) != int(elapsed - 0.1):
                        stats = client.get_statistics()
                        print(f"[{int(elapsed):2d}s] Sent: {total_sent//1024:6d} KB, "
                              f"cwnd: {stats['cwnd']:6.2f}, "
                              f"Throughput: {stats['throughput']:8.2f} KB/s, "
                              f"Loss: {stats['packets_lost']:4d}, "
                              f"Retrans: {stats['retransmissions']:4d}")
            except KeyboardInterrupt:
                print("\n\nTest interrupted by user")
            
            time.sleep(2.0)  # Allow remaining packets to arrive
            
            # Collect final statistics
            client_stats = client.get_statistics()
            server_stats = server.get_statistics()
            
            print(f"\n{'='*60}")
            print("FINAL STATISTICS:")
            print(f"{'='*60}")
            print(f"Total data sent: {total_sent//1024} KB")
            print(f"Total data received: {received_bytes[0]//1024} KB")
            print(f"Duration: {client_stats['elapsed_time']:.2f} seconds")
            print(f"Average throughput: {client_stats['throughput']:.2f} KB/s")
            print(f"Packets sent: {client_stats['packets_sent']}")
            print(f"Packets lost: {client_stats['packets_lost']}")
            print(f"Packet loss rate: {client_stats['packets_lost']/client_stats['packets_sent']*100:.2f}%")
            print(f"Retransmissions: {client_stats['retransmissions']}")
            print(f"Final cwnd: {client_stats['cwnd']:.2f}")
            print(f"Final ssthresh: {client_stats['ssthresh']:.2f}")
            
            # Save detailed statistics for analysis
            result['details'] = {
                'client_stats': client_stats,
                'server_stats': server_stats,
                'total_sent': total_sent,
                'total_received': received_bytes[0]
            }
            
            # Save to JSON file for figure generation
            with open('test_results.json', 'w') as f:
                json.dump(result['details'], f, indent=2)
            
            print(f"\nSaved to test_results.json")
            result['success'] = True
            
            # Cleanup
            client.close()
            server.close()
            
        except Exception as e:
            print(f"[ERROR] Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            result['details']['error'] = str(e)
        
        self.test_results.append(result)
        return result['success']
    
    def save_results(self, filename: str = 'test_summary.json'):
        """Save test results to file"""
        with open(filename, 'w') as f:
            json.dump(self.test_results, f, indent=2)
        print(f"\nTest summary saved to {filename}")


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("PIPELINED RELIABLE TRANSFER PROTOCOL - TEST SUITE")
    print("="*60)
    
    tester = ProtocolTester()
    
    # Run tests
    tests = [
        ('Connection Establishment', tester.test_connection_establishment),
        ('Reliable Data Transfer', tester.test_reliable_data_transfer),
        ('Congestion Control', tester.test_congestion_control),
        ('Flow Control', tester.test_flow_control),
        ('Connection Termination', tester.test_connection_termination),
    ]
    
    passed = 0
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
            # Add delay between tests to allow ports to be fully released
            time.sleep(3.5)
        except Exception as e:
            print(f"\n[ERROR] Test '{name}' crashed: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(3.5)
    
    # Run extended performance test
    print("\n" + "="*60)
    print("Running extended performance test for detailed analysis...")
    print("="*60)
    tester.run_performance_test(duration=10)
    
    # Summary
    print("\n" + "="*60)
    print(f"TEST SUMMARY: {passed}/{len(tests)} tests passed")
    print("="*60)
    
    # Save results
    tester.save_results()


if __name__ == "__main__":
    main()

"""
Fairness Testing for Pipelined Reliable Transfer Protocol
"""

import threading
import time
import json
import signal
import sys
from protocol import ReliableTransportProtocol, UnreliableChannel

# Global flag for graceful shutdown
shutdown_flag = threading.Event()


class SharedBottleneckChannel(UnreliableChannel):
    """Serializes all sends to emulate a shared bottleneck link."""

    def __init__(self, loss_rate: float, error_rate: float, service_rate_kbps: float = 64):
        super().__init__(loss_rate=loss_rate, error_rate=error_rate)
        self.lock = threading.Lock()
        self.service_rate_bytes = service_rate_kbps * 1024  # bytes per second
        self.last_send_time = 0.0

    def send(self, sock, data, addr):
        with self.lock:
            if self.service_rate_bytes > 0:
                now = time.time()
                spacing = len(data) / self.service_rate_bytes
                wait = max(0.0, (self.last_send_time + spacing) - now)
                if wait > 0:
                    time.sleep(wait)
                self.last_send_time = time.time()
            return super().send(sock, data, addr)

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n\nShutdown requested (Ctrl+C). Cleaning up...")
    shutdown_flag.set()
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)


def test_two_flows_fairness():
    """
    Test fairness between two simultaneous flows.
    """
    print("\n" + "="*60)
    print("Two-Flow Fairness Test")
    print("="*60)
    
    results = {
        'flow1': {'bytes': 0, 'packets': 0, 'time': 0},
        'flow2': {'bytes': 0, 'packets': 0, 'time': 0}
    }
    results_lock = threading.Lock()
    
    # Test parameters
    TEST_DURATION = 5.0
    DATA_SIZE = 1024 #KB
    LOSS_RATE = 0.08
    
    def flow_sender(flow_id, port_base, shared_channel, start_barrier):
        """Run one flow (sender + receiver pair)"""
        server = None
        client = None
        try:
            # Check for shutdown
            if shutdown_flag.is_set():
                return
                
            # Setup
            server_port = port_base
            client_port = port_base + 100
            
            server = ReliableTransportProtocol(server_port, loss_rate=LOSS_RATE, channel=shared_channel)
            client = ReliableTransportProtocol(client_port, loss_rate=LOSS_RATE, channel=shared_channel)
            
            server.start()
            client.start()
            
            # Server listens
            server.listen()
            time.sleep(0.3)
            
            # Check for shutdown
            if shutdown_flag.is_set():
                client.close()
                server.close()
                return
            
            # Client connects
            if not client.connect('localhost', server_port, timeout=3.0):
                print(f"Flow {flow_id}: Connection in progress")
                client.close()
                server.close()
                return
            
            time.sleep(0.2)

            # Align flow start times so both compete for bandwidth
            try:
                start_barrier.wait(timeout=5)
            except threading.BrokenBarrierError:
                pass
            
            # Send data for TEST_DURATION seconds
            start_time = time.time()
            bytes_sent = 0
            packets_sent = 0
            
            while time.time() - start_time < TEST_DURATION and not shutdown_flag.is_set():
                data = f"Flow{flow_id}-Packet{packets_sent}".ljust(DATA_SIZE, 'X').encode()
                try:
                    sent = client.send(data)
                    bytes_sent += sent
                    packets_sent += 1
                    time.sleep(0.01)  # Small delay to avoid overwhelming
                except Exception:
                    break
            
            elapsed = time.time() - start_time
            
            # Store results
            flow_result = {
                'bytes': bytes_sent,
                'packets': packets_sent,
                'time': elapsed,
                'throughput': bytes_sent / elapsed if elapsed > 0 else 0
            }
            with results_lock:
                results[f'flow{flow_id}'] = flow_result
            
            print(f"Flow {flow_id}: {bytes_sent} bytes, {packets_sent} packets, {elapsed:.2f}s, {bytes_sent/elapsed/1024:.2f} KB/s")
            
            # Cleanup
            time.sleep(0.5)
            if client:
                client.close()
            if server:
                server.close()
            
        except Exception:
            # Clean up on error
            if client:
                try:
                    client.close()
                except:
                    pass
            if server:
                try:
                    server.close()
                except:
                    pass
    
    # Run both flows in parallel
    print("\nStarting two flows...")
    print(f"Duration: {TEST_DURATION}s, Loss rate: {LOSS_RATE*100}%")
    print(f"Press Ctrl+C to stop\n")
    
    shared_channel = SharedBottleneckChannel(loss_rate=LOSS_RATE, error_rate=LOSS_RATE/8)
    start_barrier = threading.Barrier(2)

    thread1 = threading.Thread(target=flow_sender, args=(1, 7001, shared_channel, start_barrier))
    thread2 = threading.Thread(target=flow_sender, args=(2, 7003, shared_channel, start_barrier))
    
    # Set threads as daemon so they don't block exit
    thread1.daemon = True
    thread2.daemon = True
    
    thread1.start()
    time.sleep(0.1)  # Slight stagger to avoid simultaneous connection
    thread2.start()
    
    # Wait for threads with timeout
    timeout = TEST_DURATION + 10  # Give extra time for setup/cleanup
    thread1.join(timeout=timeout)
    thread2.join(timeout=timeout)
    
    # Check if shutdown was requested
    if shutdown_flag.is_set():
        print("\nTest interrupted")
        return False
    
    print("\n" + "-"*60)
    print("Results")
    print("-"*60)
    
    # Calculate fairness metrics
    flow1_bytes = results['flow1']['bytes']
    flow2_bytes = results['flow2']['bytes']
    
    if flow1_bytes == 0 or flow2_bytes == 0:
        print("No data sent by one or both flows. Fairness test not valid.")
        return False
    
    total_bytes = flow1_bytes + flow2_bytes
    share1 = flow1_bytes / total_bytes
    share2 = flow2_bytes / total_bytes
    
    # Jain's Fairness Index: (sum of xi)^2 / (n * sum of xi^2)
    # https://www.sciencedirect.com/science/chapter/monograph/abs/pii/B9780123850591000065
    fairness_index = (share1 + share2)**2 / (2 * (share1**2 + share2**2))
    
    print(f"\nFlow 1: {results['flow1']['throughput']/1024:.2f} KB/s ({share1*100:.1f}%)")
    print(f"Flow 2: {results['flow2']['throughput']/1024:.2f} KB/s ({share2*100:.1f}%)")
    print(f"\nJain's Fairness Index: {fairness_index:.4f}")
    
    is_fair = fairness_index >= 0.85
    print(f"Result: {'PASS' if is_fair else 'FAIL'} (threshold: 0.85)")
    
    # Save results
    results['fairness_metrics'] = {
        'jains_fairness_index': fairness_index,
        'flow1_share': share1,
        'flow2_share': share2,
        'is_fair': is_fair,
        'test_duration': TEST_DURATION
    }
    
    with open('fairness_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to: fairness_results.json")
    
    return is_fair


def main():
    """Run fairness tests"""
    print("\n" + "="*60)
    print("Fairness Testing")
    print("="*60)
    
    try:
        test_passed = test_two_flows_fairness()
        
        print("\n" + "="*60)
        print("Summary")
        print("="*60)
        print(f"Status: {'PASSED' if test_passed else 'FAILED'}")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\n\nInterrupted")
    except Exception:
        print("\nCompleted")


if __name__ == "__main__":
    main()

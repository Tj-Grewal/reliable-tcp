# Reliable TCP - Custom Transport Protocol

A Python implementation of a reliable data transfer protocol built on top of UDP, mimicking TCP's behavior with congestion control and flow control mechanisms.

## What is This Project?

Think of the internet like a postal service. When you send a letter, you trust it will arrive safely. But what if letters sometimes get lost, damaged, or arrive out of order? 

This project creates a **reliable delivery system** for data packets traveling across networks. Just like TCP (the protocol that powers most of the internet), our protocol ensures:
- **All data arrives correctly** - No lost or corrupted bytes
- **Data arrives in order** - Even if packets take different routes
- **No overwhelming the receiver** - Adapts to how fast the receiver can process data
- **Fair sharing of bandwidth** - Multiple connections share network resources fairly

## Key Features

### **Reliable Delivery**
- Automatically detects and retransmits lost packets
- Uses checksums to catch corrupted data
- Guarantees 100% data integrity even with 10% packet loss

### **Smart Speed Control**
- **Slow Start**: Starts cautiously, then rapidly increases speed
- **Congestion Avoidance**: Carefully probes for available bandwidth
- **Fast Recovery**: Quickly adapts when packets are lost

### **Fair Bandwidth Sharing**
- Multiple connections share network fairly (fairness score: 0.998/1.0)
- TCP-friendly: plays nice with other internet traffic
- Uses proven AIMD (Additive Increase, Multiplicative Decrease) algorithm

### **Flow Control**
- Prevents overwhelming slower receivers
- Dynamically adjusts sending rate based on receiver capacity
- Sliding window mechanism for efficient pipeline transmission

## How It Works:

1. **Connection Setup** (Handshake)
   - Like saying "Hello, can we talk?" before starting a conversation
   - Three-way handshake ensures both sides are ready

2. **Sending Data**
   - Breaks large messages into small packets (1024 bytes each)
   - Sends multiple packets at once (pipelining) for efficiency
   - Each packet has a sequence number, like pages in a book

3. **Acknowledgments**
   - Receiver says "I got packet #5" back to sender
   - Sender knows it's safe to send more packets

4. **Handling Problems**
   - **Packet lost?** Resend it after timeout or duplicate acknowledgments
   - **Network congested?** Slow down sending rate
   - **Data corrupted?** Checksum detects it, triggers retransmission

5. **Closing Connection**
   - Clean shutdown with goodbye messages
   - Ensures all data was delivered before closing

## Performance

Our tests show impressive reliability:

| Metric | Result |
|--------|--------|
| **Data Integrity** | 100% (perfect delivery) |
| **Configured Loss** | 10% packet loss rate |
| **Actual Loss** | 8.91% in real tests |
| **Throughput** | 8.31 KB/s under lossy conditions |
| **Fairness** | 0.998/1.0 (near-perfect sharing) |
| **Test Success** | 5/5 tests passed (100%) |

Even with intentionally introduced packet loss and network errors, the protocol delivered every single byte correctly!

## Quick Start

### Prerequisites
```bash
pip install -r requirements.txt
```

### Run the Tests
```bash
# Run all protocol tests (connection, reliability, congestion control, etc.)
python test_protocol.py

# Run fairness tests (multiple competing connections)
python test_fairness.py
```

### Generate Visualizations
```bash
# Creates graphs showing how the protocol behaves
python generate_figures.py
```

The figures show:
- How sending speed adapts over time
- Packet loss and retransmission patterns
- Throughput performance
- Fair bandwidth sharing between flows

## What Makes This Interesting?

### High Level:
This project shows how video calls, file downloads, and web browsing work reliably even though the underlying internet is unreliable. Packets get lost all the time, but protocols like this ensure you don't notice!

### Low Level:
- Full TCP Reno congestion control implementation
- Sliding window with flow control
- Simulated packet loss for realistic testing
- Comprehensive metrics and visualization
- Achieves near-perfect fairness (Jain's Index: 0.998)

## Project Structure

```
reliable-tcp/
├── protocol.py           # Main protocol implementation
├── test_protocol.py      # Test suite (5 functional tests)
├── test_fairness.py      # Fairness testing (multiple flows)
├── generate_figures.py   # Creates visualization graphs
├── requirements.txt      # Python dependencies
├── report.tex           # Detailed technical report
└── figures/             # Generated performance graphs
```

## Technical Details

- **Header Size**: 20 bytes (like TCP)
- **Max Segment Size**: 1024 bytes
- **Window-Based**: Sliding window flow control
- **Congestion Control**: TCP Reno (Slow Start, Congestion Avoidance, Fast Retransmit/Recovery)
- **Error Detection**: 16-bit checksum
- **Sequence Numbers**: Packet-based (simpler than TCP's byte-based)
- **Built on**: UDP sockets (unreliable transport layer)

## Why UDP Instead of TCP?

TCP already does reliable delivery, so why rebuild it? This is an educational project that:
- Demonstrates how reliability can be built from unreliable foundations
- Shows TCP's inner workings (congestion control, flow control, etc.)
- Allows experimentation with custom reliability mechanisms
- Provides controlled testing environment with simulated packet loss

## Test Results Summary

**Test 1**: Connection establishment works perfectly  
**Test 2**: Reliable delivery (10,000 bytes, 100% integrity)  
**Test 3**: Congestion control adapts correctly  
**Test 4**: Flow control respects receiver limits  
**Test 5**: Clean connection termination  
**Bonus**: Fair bandwidth sharing (Jain's Index: 0.998)

## Learn More

For detailed technical information, see the included `report.tex` (LaTeX report) which covers:
- Packet format specification
- State machine diagrams
- Mathematical analysis of congestion control
- Performance metrics and graphs
- Comparison with TCP Reno

## License

This is an educational project for learning about transport layer protocols and network programming.

---

**Built with:** Python, UDP sockets, threading, and mathematical modeling of TCP behavior.

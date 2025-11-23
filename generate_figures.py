"""
Generate analysis figures from protocol test results
Creates visualizations for protocol behavior analysis
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path


def load_test_results(filename='test_results.json'):
    """Load test results from JSON file"""
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {filename} not found. Please run test_protocol.py first.")
        return None


def compute_throughput_series(history, window_size=1.0):
    """Convert cumulative byte history into instantaneous throughput samples."""
    if not history:
        return [], []

    throughput_times = []
    throughput_values = []
    start_idx = 0

    for i in range(len(history)):
        current = history[i]
        t_i = current['time']
        bytes_i = current['bytes_sent']

        while start_idx < i and t_i - history[start_idx]['time'] > window_size:
            start_idx += 1

        if start_idx == i:
            continue

        elapsed = t_i - history[start_idx]['time']
        if elapsed <= 0:
            continue

        delta_bytes = bytes_i - history[start_idx]['bytes_sent']
        throughput = (delta_bytes / elapsed) / 1024  # KB/s
        throughput_times.append(t_i)
        throughput_values.append(throughput)

    return throughput_times, throughput_values


def plot_congestion_window(data, output_file='figure1_cwnd_evolution.png'):
    """
    Figure 1: Congestion Window Evolution Over Time
    Shows how cwnd changes during transmission
    """
    client_stats = data.get('client_stats', {})
    cwnd_history = client_stats.get('cwnd_history', [])
    timestamps = client_stats.get('timestamps', [])
    
    if not cwnd_history or not timestamps:
        print("Warning: No cwnd history data available")
        return
    
    plt.figure(figsize=(12, 6))
    plt.plot(timestamps, cwnd_history, 'b-', linewidth=2, label='Congestion Window (cwnd)')
    plt.axhline(y=client_stats.get('ssthresh', 64), color='r', 
                linestyle='--', linewidth=1.5, label='Slow Start Threshold (ssthresh)')
    
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Congestion Window Size (packets)', fontsize=12)
    plt.title('Congestion Window Evolution Over Time', fontsize=14, fontweight='bold')
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    
    # Annotate key phases
    if len(cwnd_history) > 10:
        # Find slow start phase (exponential growth)
        for i in range(1, min(len(cwnd_history), 20)):
            if cwnd_history[i] < cwnd_history[i-1]:
                plt.annotate('Timeout/Loss', xy=(timestamps[i], cwnd_history[i]),
                           xytext=(timestamps[i]+1, cwnd_history[i]+5),
                           arrowprops=dict(arrowstyle='->', color='red', lw=1.5),
                           fontsize=9, color='red')
                break
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()


def plot_throughput(data, output_file='figure2_throughput.png'):
    """
    Figure 2: Throughput Over Time
    Shows effective data transfer rate
    """
    client_stats = data.get('client_stats', {})
    history = client_stats.get('throughput_history', [])
    
    if len(history) < 2:
        print("Warning: Insufficient throughput history data available")
        return
    
    throughput_times, throughput_values = compute_throughput_series(history)
    
    plt.figure(figsize=(12, 6))
    plt.plot(throughput_times, throughput_values, 'g-', linewidth=2, label='Throughput')
    
    avg_throughput = client_stats.get('throughput', 0)
    plt.axhline(y=avg_throughput, color='orange', linestyle='--', 
                linewidth=1.5, label=f'Average: {avg_throughput:.2f} KB/s')
    
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Throughput (KB/s)', fontsize=12)
    plt.title('Network Throughput Over Time', fontsize=14, fontweight='bold')
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.ylim(bottom=0)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()


def plot_packet_statistics(data, output_file='figure3_packet_stats.png'):
    """
    Figure 3: Packet Statistics
    Shows packets sent, lost, and retransmitted
    """
    client_stats = data.get('client_stats', {})
    
    packets_sent = client_stats.get('packets_sent', 0)
    packets_lost = client_stats.get('packets_lost', 0)
    retransmissions = client_stats.get('retransmissions', 0)
    packets_delivered = packets_sent - packets_lost
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Pie chart of packet distribution
    labels = ['Successfully Delivered', 'Lost', 'Retransmitted']
    sizes = [packets_delivered, packets_lost, retransmissions]
    colors = ['#2ecc71', '#e74c3c', '#f39c12']
    explode = (0.05, 0.05, 0.05)
    
    ax1.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
            shadow=True, startangle=90, textprops={'fontsize': 11})
    ax1.set_title('Packet Distribution', fontsize=14, fontweight='bold')
    
    # Bar chart of packet counts
    categories = ['Sent', 'Delivered', 'Lost', 'Retrans.']
    values = [packets_sent, packets_delivered, packets_lost, retransmissions]
    colors_bar = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12']
    
    bars = ax2.bar(categories, values, color=colors_bar, alpha=0.8, edgecolor='black')
    ax2.set_ylabel('Packet Count', fontsize=12)
    ax2.set_title('Packet Statistics', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()


def plot_protocol_behavior(data, output_file='figure4_protocol_behavior.png'):
    """
    Figure 4: Protocol Behavior Analysis
    Multi-panel view of protocol operation
    """
    client_stats = data.get('client_stats', {})
    cwnd_history = client_stats.get('cwnd_history', [])
    timestamps = client_stats.get('timestamps', [])
    rwnd_history = client_stats.get('rwnd_history', [])
    throughput_history = client_stats.get('throughput_history', [])
    
    if not cwnd_history or not timestamps or len(cwnd_history) != len(timestamps):
        print("Warning: Insufficient data for protocol behavior plot")
        return
    
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))
    
    # Panel 1: Congestion Window and ssthresh
    ax1 = axes[0]
    ax1.plot(timestamps, cwnd_history, 'b-', linewidth=2, label='cwnd')
    ax1.axhline(y=client_stats.get('ssthresh', 64), color='r', 
                linestyle='--', linewidth=1.5, label='ssthresh')
    ax1.set_ylabel('Window Size (packets)', fontsize=11)
    ax1.set_title('Congestion Control Behavior', fontsize=12, fontweight='bold')
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)
    
    # Panel 2: Advertised receiver window history
    ax2 = axes[1]
    if rwnd_history and len(rwnd_history) == len(timestamps):
        ax2.plot(timestamps, rwnd_history, 'orange', linewidth=2, label='Receiver Window (rwnd)')
        ax2.set_ylabel('Window Size (packets)', fontsize=11)
        ax2.set_title('Receiver Window Advertisement', fontsize=12, fontweight='bold')
        ax2.legend(fontsize=9)
    else:
        ax2.text(0.5, 0.5, 'rwnd history unavailable', ha='center', va='center', fontsize=11)
    ax2.grid(True, alpha=0.3)
    
    # Panel 3: Estimated Throughput
    ax3 = axes[2]
    t_times, t_values = compute_throughput_series(throughput_history)
    if t_times:
        ax3.plot(t_times, t_values, 'purple', linewidth=2, label='Throughput (KB/s)')
        ax3.set_ylabel('Throughput (KB/s)', fontsize=11)
        ax3.set_title('Measured Throughput', fontsize=12, fontweight='bold')
        ax3.legend(fontsize=9)
    else:
        ax3.text(0.5, 0.5, 'Throughput history unavailable', ha='center', va='center', fontsize=11)
        ax3.set_title('Measured Throughput', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Throughput (KB/s)', fontsize=11)
    ax3.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()


def plot_performance_summary(data, output_file='figure5_performance_summary.png'):
    """
    Figure 5: Performance Summary
    Key performance metrics in a dashboard
    """
    client_stats = data.get('client_stats', {})
    
    # Extract key metrics
    throughput = client_stats.get('throughput', 0)
    packets_sent = client_stats.get('packets_sent', 0)
    packets_lost = client_stats.get('packets_lost', 0)
    loss_rate = (packets_lost / packets_sent * 100) if packets_sent > 0 else 0
    retrans = client_stats.get('retransmissions', 0)
    elapsed = client_stats.get('elapsed_time', 0)
    bytes_sent = client_stats.get('bytes_sent', 0)
    
    fig = plt.figure(figsize=(14, 8))
    gs = fig.add_gridspec(3, 3, hspace=0.4, wspace=0.4)
    
    # Metric boxes
    metrics = [
        ('Throughput', f'{throughput:.2f} KB/s', '#3498db'),
        ('Packets Sent', f'{packets_sent}', '#2ecc71'),
        ('Packet Loss', f'{loss_rate:.2f}%', '#e74c3c'),
        ('Retransmissions', f'{retrans}', '#f39c12'),
        ('Duration', f'{elapsed:.2f} sec', '#9b59b6'),
        ('Data Sent', f'{bytes_sent/1024:.2f} KB', '#1abc9c'),
    ]
    
    for idx, (label, value, color) in enumerate(metrics):
        row = idx // 3
        col = idx % 3
        ax = fig.add_subplot(gs[row, col])
        ax.text(0.5, 0.6, value, ha='center', va='center', 
                fontsize=24, fontweight='bold', color=color)
        ax.text(0.5, 0.3, label, ha='center', va='center', 
                fontsize=12, color='gray')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        # Add box border
        from matplotlib.patches import Rectangle
        rect = Rectangle((0.05, 0.15), 0.9, 0.7, linewidth=2, 
                        edgecolor=color, facecolor='none')
        ax.add_patch(rect)
    
    fig.suptitle('Protocol Performance Summary', fontsize=16, fontweight='bold', y=0.98)
    
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()


def plot_flow_control_demo(data, output_file='figure6_flow_control.png'):
    """
    Figure 6: Flow Control Demonstration
    Shows relationship between sender and receiver windows
    """
    client_stats = data.get('client_stats', {})
    cwnd_history = client_stats.get('cwnd_history', [])
    rwnd_history = client_stats.get('rwnd_history', [])
    timestamps = client_stats.get('timestamps', [])
    
    if not cwnd_history or not timestamps or len(cwnd_history) != len(timestamps) or len(rwnd_history) != len(timestamps):
        print("Warning: Insufficient data for flow control plot")
        return
    
    effective_window = [min(c, r) for c, r in zip(cwnd_history, rwnd_history)]
    
    plt.figure(figsize=(12, 6))
    plt.plot(timestamps, cwnd_history, 'b-', linewidth=2, label='Congestion Window (cwnd)', alpha=0.7)
    plt.plot(timestamps, rwnd_history, 'r-', linewidth=2, label='Receiver Window (rwnd)', alpha=0.7)
    plt.plot(timestamps, effective_window, 'g-', linewidth=3, 
            label='Effective Window (min)', alpha=0.8)
    
    plt.fill_between(timestamps, 0, effective_window, alpha=0.2, color='green')
    
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Window Size (packets)', fontsize=12)
    plt.title('Flow Control: Sender vs Receiver Windows', fontsize=14, fontweight='bold')
    plt.legend(fontsize=10, loc='best')
    plt.grid(True, alpha=0.3)
    
    # Add annotation
    if len(timestamps) > len(timestamps)//2:
        mid_idx = len(timestamps) // 2
        plt.annotate('Effective window limited by min(cwnd, rwnd)',
                    xy=(timestamps[mid_idx], effective_window[mid_idx]),
                    xytext=(timestamps[mid_idx] + 1, effective_window[mid_idx] + 10),
                    arrowprops=dict(arrowstyle='->', color='green', lw=2),
                    fontsize=10, color='green', fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()


def generate_all_figures():
    """Generate all analysis figures"""
    print("\n" + "="*60)
    print("GENERATING ANALYSIS FIGURES")
    print("="*60)
    
    # Load test results
    data = load_test_results()
    if data is None:
        print("\n Cannot generate figures without test results.")
        print("  Please run: python test_protocol.py")
        return False
    
    print("\nGenerating figures...\n")
    
    # Create output directory
    output_dir = Path('figures')
    output_dir.mkdir(exist_ok=True)
    
    # Generate each figure
    try:
        plot_congestion_window(data, f'{output_dir}/figure1_cwnd_evolution.png')
        plot_throughput(data, f'{output_dir}/figure2_throughput.png')
        plot_packet_statistics(data, f'{output_dir}/figure3_packet_stats.png')
        plot_protocol_behavior(data, f'{output_dir}/figure4_protocol_behavior.png')
        plot_performance_summary(data, f'{output_dir}/figure5_performance_summary.png')
        plot_flow_control_demo(data, f'{output_dir}/figure6_flow_control.png')

        return True
        
    except Exception as e:
        print(f"\n Error generating figures: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""
    success = generate_all_figures()
    
    if success:
        print("\nFigures generated")
    else:
        print("\nPlease resolve errors and try again.")


if __name__ == "__main__":
    main()

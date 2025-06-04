#!/usr/bin/env python3

import json
import math
import re
import sys
import statistics
from pathlib import Path
from typing import List, Dict, Any, Optional

class LogAnalyzer:
    def __init__(self):
        """
        Initialize the log analyzer.
        """
        self.metrics = []
        
    def extract_json_from_line(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Extract the JSON output from replica logs (stats overview)
        """
        # Look for JSON pattern in the line
        json_match = re.search(r'\{.*\}', line)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                return None
        return None
    
    def calculate_metrics(self, data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate latency and throughput metrics from log data.
        """
        metrics = {}
        raw_prop_ns     = data.get('propose_pre_prepare_time', 0)
        raw_prepare_ns  = data.get('prepare_time', 0)
        raw_commit_ns   = data.get('commit_time', 0)
        raw_exec_ns     = data.get('execution_time', 0)

        metrics['raw_propose_time']   = raw_prop_ns
        metrics['raw_prepare_time']   = raw_prepare_ns
        metrics['raw_commit_time']    = raw_commit_ns
        metrics['raw_execution_time'] = raw_exec_ns

        # convert nano -> ms:
        execution_time_ms           = (int(raw_exec_ns) / 1e6) if raw_exec_ns > 0 else None
        propose_pre_prepare_time_ms = (int(raw_prop_ns) / 1e6) if raw_prop_ns > 0 else None
        commit_time_ms              = (int(raw_commit_ns) / 1e6) if raw_commit_ns > 0 else None
        prepare_time_ms             = (int(raw_prepare_ns) / 1e6) if raw_prepare_ns > 0 else None

        
        if execution_time_ms is not None and propose_pre_prepare_time_ms is not None:
            metrics['end_to_end_latency_ms'] = execution_time_ms - propose_pre_prepare_time_ms
        else:
            metrics['end_to_end_latency_ms'] = None

        if raw_exec_ns > 0 and raw_commit_ns > 0:
            metrics['execution_latency_ms'] = (int(raw_exec_ns) - int(raw_commit_ns))/1e6
        else:
            metrics['execution_latency_ms'] = None

        if raw_prepare_ns > 0 and raw_prop_ns > 0:
            metrics['consensus_latency_ms'] = (int(raw_prepare_ns) - int(raw_prop_ns))/1e6
        else:
            metrics['consensus_latency_ms'] = None

        if raw_commit_ns > 0 and raw_prepare_ns > 0:
            metrics['commit_latency_ms'] = (int(raw_commit_ns) - int(raw_prepare_ns))/1e6
        else:
            metrics['commit_latency_ms'] = None
        
        # Store additional info
        metrics['txn_number'] = data.get('txn_number',0)
        metrics['replica_id'] = data.get('replica_id', 0)
        metrics['primary_id'] = data.get('primary_id', 0)
        
        return metrics
    
    def process_log_file(self, filepath: str) -> List[Dict[str, Any]]:
        """
        Process a single log file and extract metrics.
        """
        file_metrics = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    
                    if 'stats.cpp:350]' in line and 'commit_time' in line:
                        json_data = self.extract_json_from_line(line)
                        if json_data:
                            metrics = self.calculate_metrics(json_data)
                            metrics['source_file'] = filepath
                            metrics['line_number'] = line_num
                            file_metrics.append(metrics)
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
        
        return file_metrics
    
    def analyze_logs(self, log_files: List[str]) -> Dict[str, Any]:
        """
        Analyze specified log files.
        """
        if not log_files:
            print("No log files specified!")
            return {}
        
        print(f"Analyzing {len(log_files)} log files")
        
        all_metrics = []
        
        for log_file in log_files:
            if not Path(log_file).exists():
                print(f"Warning: File {log_file} does not exist, skipping...")
                continue
                
            print(f"Processing {log_file}...")
            file_metrics = self.process_log_file(log_file)
            all_metrics.extend(file_metrics)
            print(f"  Found {len(file_metrics)} relevant log entries")
        
        if not all_metrics:
            print("No relevant log entries found!")
            return {}
        
        # Calculate summary stats and throughput
        return self.calculate_summary_stats(all_metrics)
    
    def calculate_summary_stats(self, metrics_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate summary statistics from all metrics.
        """
        if not metrics_list:
            return {}
        
        # Filter out extreme latency values 
        def filter_valid_latencies(values):
            return [v for v in values if v is not None and 0 <= v <= 60000]
        
        # Extract numeric metrics
        latency_fields = [
            'end_to_end_latency_ms',
            'execution_latency_ms', 
            'consensus_latency_ms',
            'commit_latency_ms'
        ]

        summary = {
            'total_entries': len(metrics_list),
            'averages': {},
            'min_values': {},
            'max_values': {},
            'valid_counts': {}
        }
        
        # Process latency fields with filtering
        for field in latency_fields:
            all_values = [m[field] for m in metrics_list if field in m and m[field] is not None]
            valid_values = filter_valid_latencies(all_values)
            
            summary['valid_counts'][field] = len(valid_values)
            
            if valid_values:
                summary['averages'][field] = statistics.mean(valid_values)
                summary['min_values'][field] = min(valid_values)
                summary['max_values'][field] = max(valid_values)
            
            # Report filtered values
            if len(all_values) != len(valid_values):
                print(f"Warning: {field} had {len(all_values) - len(valid_values)} invalid values filtered out")
        
        # Calculate throughput
        self.calculate_throughput(metrics_list, summary)
        
        return summary
    
    def calculate_throughput(self, metrics_list: List[Dict[str, Any]], summary: Dict[str, Any]):
        """
        Calculate throughput metrics.
        """
        if not metrics_list:
            return
        
        #First transaction seen
        pre_propose_times = [m['raw_propose_time'] for m in metrics_list if m.get('raw_propose_time', 0) > 0]
        first_propose = min(pre_propose_times)

        #Last transaction committed
        execution_times = [m['raw_execution_time'] for m in metrics_list if m.get('raw_execution_time', 0) > 0]
        last_execute =max(execution_times)

        # Get time range
        time_range = (last_execute - first_propose)
        time_range_s = time_range/1e9

        #Get the total transactions
        total_transactions = len(metrics_list)

        if time_range_s > 0:
            summary['throughput_tps'] = total_transactions / time_range_s
            summary['time_range_seconds'] = time_range_s
            summary['total_transactions'] = total_transactions
        
        # Per-replica throughput
        replica_data = {}
        for i in range(16):
            replica_prepropose = []
            replica_execution = []
            transaction_count = 0

            for m in metrics_list:
                rep_id = m.get('replica_id')
                if rep_id == i:
                    raw_prop = m.get("raw_propose_time", 0)
                    raw_exec = m.get("raw_execution_time", 0)
                    if raw_prop > 0 and raw_exec > 0:
                        replica_prepropose.append(raw_prop)
                        replica_execution.append(raw_exec)
                        transaction_count += 1

            if replica_prepropose and replica_execution:
                replica_first_time = min(replica_prepropose)
                replica_last_time  = max(replica_execution)
                replica_timerange_s = (replica_last_time - replica_first_time) / 1e9
                if replica_timerange_s > 0:
                    replica_tps = transaction_count / replica_timerange_s
                else:
                    replica_tps = 0.0
            else:
                replica_tps = 0.0

            replica_data[f"replica_{i}_tps"] = replica_tps
        
        
        #Store in summary
        summary["per_replica_tps"] = replica_data
        
    def print_results(self, summary: Dict[str, Any]):
        """
        Print formatted results.
        """
        if not summary:
            print("No data to display")
            return
        
        print("\n" + "="*60)
        print("PERFORMANCE ANALYSIS RESULTS")
        print("="*60)
        
        print(f"\nTotal log entries analyzed: {summary['total_entries']}")
        
        # Throughput metrics
        if 'throughput_tps' in summary:
            print(f"\nTHROUGHPUT METRICS:")
            print(f"Time Range: {summary.get('time_range_seconds', 0):.2f} seconds")
            print(f"Total Transactions: {summary.get('total_transactions', 0)}")
            print(f"Overall Throughput: {summary['throughput_tps']:.2f} TPS")
            
            if 'per_replica_tps' in summary:
                print(f"\nPer-Replica Throughput:")
                for replica_id, tps in sorted(summary['per_replica_tps'].items()):
                    print(f"  Replica {replica_id}: {tps:.2f} TPS")
        
        print(f"\nLATENCY METRICS (milliseconds):")
        print(f"{'Metric':<25} {'Count':<8} {'Average':<12} {'Min':<12} {'Max':<12} ")
        print("-" * 100)
        
        latency_metrics = [
            ('End-to-End Latency', 'end_to_end_latency_ms'),
            ('Consensus Latency', 'consensus_latency_ms'),
            ('Commit Latency', 'commit_latency_ms'),
            ('Execution Latency', 'execution_latency_ms')
        ]
        
        for name, field in latency_metrics:
            if field in summary['averages']:
                count = summary['valid_counts'].get(field, 0)
                avg = summary['averages'][field]
                min_val = summary['min_values'][field]
                max_val = summary['max_values'][field]
                print(f"{name:<25} {count:<8} {avg:<12.3f} {min_val:<12.3f} {max_val:<12.3f}")
        
def main():
    
    print("Distributed System Log Analyzer")
    print("="*40)
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python log_analyzer.py <log_file1> <log_file2> ... <log_fileN>")
        print("Example: python log_analyzer.py server0.log server1.log server2.log")
        sys.exit(1)
    
    # Get log files from command line arguments
    log_files = sys.argv[1:]
    
    print(f"Log files to analyze: {', '.join(log_files)}")
    
    # Initialize analyzer
    analyzer = LogAnalyzer()
    
    # Run analysis
    results = analyzer.analyze_logs(log_files)
    
    # Print results
    analyzer.print_results(results)
    
    # Save results to JSON
    if results:
        with open('analysis_results_3.json', 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nDetailed results saved to 'analysis_results.json'")

if __name__ == "__main__":
    main()
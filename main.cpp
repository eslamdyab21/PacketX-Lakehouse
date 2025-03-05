#include "packets_monitor/packets_monitor.h"
#include <string>
#include <thread>
#include <iostream>
#include <cstdlib>


void checkNewTcpdump(PacketsMonitor *Monitor) {
    Monitor->checkNewTcpdumpDataThread();
}



int main() {
    PacketsMonitor Monitor;
    std::string total_bytes;
    std::unordered_map<std::string, packet>* packets_hashmap;

    // Check new tcpdump data thread
    std::thread checkNewTcpdumpDataThread(checkNewTcpdump, &Monitor);

    // Process new tcpdump data main thread
    while (true) {
        std::unique_lock<std::mutex> lock(Monitor.queue_mutex);
        // Monitor.queue_cond.wait(lock, [&] { return !Monitor.tcpdump_data_queue.empty(); });
        Monitor.queue_cond.wait(lock);

        std::string traffic_captured_file_path = Monitor.tcpdump_data_queue.front();
        Monitor.tcpdump_data_queue.pop();
        lock.unlock();
  
        Monitor.processNewTcpdumpTsharkTotalBytes(traffic_captured_file_path);
        
        Monitor.processNewTcpdumpTsharkIPBytes(traffic_captured_file_path);
        
                
        std::cout << traffic_captured_file_path << std::endl;
        for(auto kv : Monitor.packets_hashmap) {
            std::cout << kv.second.source_ip << ", " << kv.second.destination_ip << ", " << kv.second.total_k_bytes_bandwidth_for_ip << ", " << Monitor.total_bytes_all_ips << std::endl;
        } 

        std::cout << "----------------------------" << std::endl;
    }



    return 0;
}
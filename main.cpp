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
    int i = 0;

    // Check new tcpdump data thread
    std::thread checkNewTcpdumpDataThread(checkNewTcpdump, &Monitor);

    // Process new tcpdump data main thread
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex);
        queue_cond.wait(lock, [] { return !tcpdump_data_queue.empty(); });

        std::string traffic_captured_file_path = tcpdump_data_queue.front();
        tcpdump_data_queue.pop();
        lock.unlock();
  
        // std::cout << traffic_captured_file_path << std::endl; 
        total_bytes = Monitor.processNewTcpdumpTsharkTotalBytes(traffic_captured_file_path);
        
        Monitor.processNewTcpdumpTsharkIPBytes(traffic_captured_file_path);
        
        
        // std::cout << "Total Bytes IP, " << Monitor.packets_hashmap["172.29.1.167-172.29.0.1"].total_k_bytes_bandwidth_for_ip << std::endl; 
        
        std::cout << i << std::endl;
        for(auto kv : Monitor.packets_hashmap) {
            std::cout << kv.second.source_ip << ", " << kv.second.destination_ip << ", " << kv.second.total_k_bytes_bandwidth_for_ip << std::endl;
        } 

        std::cout << "----------------------------" << std::endl;
        i++;
    }



    return 0;
}
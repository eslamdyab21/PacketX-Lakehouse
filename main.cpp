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

    // Check new tcpdump data thread
    std::thread checkNewTcpdumpDataThread(checkNewTcpdump, &Monitor);

    // Process new tcpdump data main thread
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex);
        queue_cond.wait(lock, [] { return !tcpdump_data_queue.empty(); });

        std::string traffic_captured_file_path = tcpdump_data_queue.front();
        tcpdump_data_queue.pop();
        lock.unlock();
  
        std::cout << traffic_captured_file_path << std::endl; 
        total_bytes = Monitor.processNewTcpdumpTshark(traffic_captured_file_path);
        std::cout << total_bytes << std::endl; 
        // Monitor.processFile(traffic_captured_file_path);
        
    }



    return 0;
}
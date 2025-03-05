#include "packets_monitor/packets_monitor.h"
#include <string>
#include <thread>
#include <iostream>
#include <cstdlib>
#include <iomanip>


void checkNewTcpdump(PacketsMonitor *Monitor) {
    Monitor->checkNewTcpdumpDataThread();
}



void periodicSave(PacketsMonitor *Monitor, std::string filename) {
    while (true) {
        std::this_thread::sleep_for(std::chrono::minutes(1));
        Monitor->saveToCSV(filename);
    }
}



int main() {
    PacketsMonitor Monitor;
    std::string total_bytes;
    std::unordered_map<std::string, packet>* packets_hashmap;

    // Get previously saved traffic to accumulate on
    Monitor.loadFromCSV("traffic_log.csv");

    // Check new tcpdump data thread
    std::thread checkNewTcpdumpDataThread(checkNewTcpdump, &Monitor);

    // Periodic save thread
    std::thread save_thread(periodicSave, &Monitor, "traffic_log.csv");


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

    }

    return 0;
}
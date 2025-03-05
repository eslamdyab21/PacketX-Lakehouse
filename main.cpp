#include "packets_monitor/packets_monitor.h"
#include <string>
#include <thread>
#include <iostream>
#include <cstdlib>
#include <iomanip>
#include <filesystem>



void checkNewTcpdump(PacketsMonitor *Monitor) {
    Monitor->checkNewTcpdumpDataThread();
}



void periodicSave(PacketsMonitor *Monitor, std::string filename) {
    while (true) {
        std::this_thread::sleep_for(std::chrono::minutes(1));
        Monitor->saveToCSV(filename);
    }
}



void periodicDelete(PacketsMonitor *Monitor) {
    while (true) {
        std::this_thread::sleep_for(std::chrono::minutes(1));
        
        for (auto kv : Monitor->tcp_captured_hashmap) {
            if (kv.second == 1) 
                std::filesystem::remove(kv.first);
        }
    }
}



void formatted_print(PacketsMonitor *Monitor){
    std::cout << std::left
    << std::setw(30) << "Source IP"
    << std::setw(30) << "Destination IP"
    << std::setw(20) << "KB Bandwidth"
    << std::setw(20) << "Total KB Bandwidth"
    << "\n";

    std::cout << std::string(100, '-') << "\n";

    for (auto kv : Monitor->packets_hashmap) {
    std::cout << std::left
        << std::setw(30) << kv.second.source_ip
        << std::setw(30) << kv.second.destination_ip
        << std::setw(20) << kv.second.total_k_bytes_bandwidth_for_ip
        << std::setw(20) << Monitor->total_bytes_all_ips
        << "\n";
    }

    std::cout << std::string(100, '-') << std::endl;
}



int main() {
    PacketsMonitor Monitor;
    std::string total_bytes;
    std::unordered_map<std::string, packet>* packets_hashmap;

    // Get previously saved traffic to accumulate on it
    Monitor.loadFromCSV("traffic_log.csv");

    // Check new tcpdump data thread
    std::thread check_new_tcpdump_data_thread(checkNewTcpdump, &Monitor);

    // Periodic save thread
    std::thread save_thread(periodicSave, &Monitor, "traffic_log.csv");

    // Periodic delete thread
    std::thread delete_thread(periodicDelete, &Monitor);


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

        formatted_print(&Monitor);
    }

    return 0;
}
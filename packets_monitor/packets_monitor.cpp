#include <filesystem>
#include <unordered_map>
#include <array>
#include <iostream>
#include <algorithm>
#include <thread>
#include "packets_monitor.h"




void PacketsMonitor::checkNewTcpdumpDataThread() {
    std::unordered_map<std::string, int> tcp_captured_hashmap;
    std::string path = ".tcpdump";
    std::uintmax_t file_size;
    bool  queue_cond_notify = false;

    while (true) {
        for (const auto & entry : std::filesystem::directory_iterator(path)){
            if (entry.is_regular_file() && entry.path().extension() == ".pcap"){

                file_size = std::filesystem::file_size(entry.path());
                
                // New tcpdump file
                if ((tcp_captured_hashmap.count(entry.path()) == 0) && file_size > 0){
                    
                    tcp_captured_hashmap[entry.path()] = 1;
                    
                    {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        tcpdump_data_queue.push(entry.path().string());
                    }

                    if (!queue_cond_notify)
                        queue_cond_notify = true;
                    else {
                        queue_cond.notify_one();
                        queue_cond_notify = false;
                    }     
    
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}



void PacketsMonitor::processNewTcpdumpTsharkTotalBytes(std::string filePath) {
    std::array<char, 512> buffer;
    std::string total_bytes;

    std::string cmd = "tshark -r " + filePath + " -qz io,phs | grep 'eth' | grep 'bytes' | cut -d':' -f3";

    FILE* pipe = popen(cmd.c_str(), "r");

    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }

    while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
        total_bytes += buffer.data();
    }

    if (!total_bytes.empty())
        total_bytes_all_ips += std::stoi(total_bytes) / 1024;
}


void PacketsMonitor::processNewTcpdumpTsharkIPBytes(std::string filePath) {
    std::array<char, 1024> buffer;
    std::string temp;
    std::string ip1, ip2, key;
    double total_bytes;
    
    std::string cmd = "tshark -r " + filePath + " -q -z conv,ip | grep '<->' | sed 's/[[:space:]]\\+/ /g'";
    
    FILE* pipe = popen(cmd.c_str(), "r");
    
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    
    while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
        std::istringstream iss(buffer.data());

        // Extract IPs
        std::getline(iss, ip1, ' ');  // Read first IP
        std::getline(iss, temp, ' '); // Read "<->"
        std::getline(iss, ip2, ' ');  // Read second IP

        // Persist key arrangement
        // Treat (172.29.0.1, 172.29.1.167) same as (172.29.1.167, 172.29.0.1) in the hashmap
        if (ip1 < ip2){
            temp = ip1;
            ip1 = ip2;
            ip2 = temp; 
        }
        key = ip1 + "-" + ip2;


        // Skip
        for (int i=0; i < 7; i++)
            std::getline(iss, temp, ' ');
        
        std::getline(iss, temp, ' ');
        total_bytes = std::stoi(temp); // Read total bytes

        std::getline(iss, temp, ' '); // Read Bytes or k bytes
        if (temp == "bytes")
            total_bytes = total_bytes / 1024;
        
        if (packets_hashmap.count(key) == 0){
            packets_hashmap[key].source_ip = ip1;
            packets_hashmap[key].destination_ip = ip2;
            packets_hashmap[key].total_k_bytes_bandwidth_for_ip = total_bytes;
        }
        else 
            packets_hashmap[key].total_k_bytes_bandwidth_for_ip += total_bytes;
        
    }

}
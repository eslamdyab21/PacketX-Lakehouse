#include <filesystem>
#include <unordered_map>
#include <array>
#include "packets_monitor.h"

// Define shared resources
std::mutex queue_mutex;
std::queue<std::string> tcpdump_data_queue;
std::condition_variable queue_cond;


void PacketsMonitor::checkNewTcpdumpDataThread() {
    std::unordered_map<std::string, int> tcp_captured_hashmap;
    std::string path = ".tcpdump";

    while (true) {
        for (const auto & entry : std::filesystem::directory_iterator(path)){

            // New tcpdump file
            if (tcp_captured_hashmap.count(entry.path()) == 0){
                tcp_captured_hashmap[entry.path()] = 1;

                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    tcpdump_data_queue.push(entry.path().string());
                }
                queue_cond.notify_one();
            }
        }

    }
}

// tshark -r capture.pcap -qz io,phs

// void PacketsMonitor::processNewTcpdumpTshark(std::string filePath) {
//     // std::cout << "📡 Processing: " << filePath << std::endl;
// std::string command = "tshark -r " + filePath + " -qz io,phs fields -e frame.number -e ip.src -e ip.dst";
//     system(command.c_str());
// }


std::string PacketsMonitor::processNewTcpdumpTsharkTotalBytes(std::string filePath) {
    std::array<char, 128> buffer;
    std::string total_bytes;

    std::string cmd = "tshark -r " + filePath + " -qz io,phs | grep 'eth' | grep 'bytes' | cut -d':' -f3";

    FILE* pipe = popen(cmd.c_str(), "r");

    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }

    while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
        total_bytes += buffer.data();
    }
    return total_bytes;
}
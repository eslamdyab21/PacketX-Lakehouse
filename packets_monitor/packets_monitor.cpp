#include <filesystem>
#include <unordered_map>
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
                    tcpdump_data_queue.push(entry.path());
                }
                queue_cond.notify_one();
            }
        }

    }
}
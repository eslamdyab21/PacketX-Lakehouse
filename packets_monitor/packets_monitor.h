#ifndef PACKETSMONITOR_H
#define PACKETSMONITOR_H

#include <string>
#include <queue>
#include <fstream>
#include <condition_variable>


struct packet {
    std::string source_ip;
    std::string destination_ip;
    double total_k_bytes_bandwidth_for_ip;
    double total_k_bytes_bandwidth_for_all_ips;
};


class PacketsMonitor {
public:
    void checkNewTcpdumpDataThread();
};


// Define shared resources
extern std::mutex queue_mutex;
extern std::queue<std::string> tcpdump_data_queue;
extern std::condition_variable queue_cond;



#endif // PACKETSMONITOR_H

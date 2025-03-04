#ifndef PACKETSMONITOR_H
#define PACKETSMONITOR_H

#include <string>


struct packet {
    std::string source_ip;
    std::string destination_ip;
    double total_k_bytes_bandwidth_for_ip;
    double total_k_bytes_bandwidth_for_all_ips;
};


class PacketsMonitor {
public:
    void checkNewDataThread();
};



#endif // PACKETSMONITOR_H

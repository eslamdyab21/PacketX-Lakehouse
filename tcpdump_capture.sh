#!/bin/bash

# Configurable Variables
INTERFACE="eth0"  # Change this to your actual network interface
TIME=10

mkdir -p .tcpdump
chmod -R 777 .tcpdump
echo "📡 Starting Packet Core Monitoring..."
while true; do
    tcpdump -i $INTERFACE -s 0 -B 4096 -w .tcpdump/capture-%Y%m%d%H%M%S.pcap -G "$TIME" -v
    touch .tcpdump/tcpdump_finished_writing
done
#!/bin/bash

# Configurable Variables
INTERFACE="$1"  # Change this to your actual network interface
TIME="$2"

mkdir -p .tcpdump
chmod -R 777 .tcpdump
echo "📡 Starting Packet Core Monitoring..."


while true; do
    tcpdump -i $INTERFACE -s 0 -B 4096 -w .tcpdump/capture-%Y%m%d%H%M%S.pcap -G $TIME -v
done
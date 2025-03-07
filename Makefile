main: main.o packets_monitor/packets_monitor.o logger/logger.o
	g++ main.o packets_monitor/packets_monitor.o logger/logger.o -o main

main.o: main.cpp
	g++ -c main.cpp -o main.o



# Modules 
ephemeris/ephemeris_data.o: packets_monitor/packets_monitor.cpp
	g++ -c packets_monitor/packets_monitor.cpp -o packets_monitor/packets_monitor.o


logger/logger.o: logger/logger.cpp
	g++ -c logger/logger.cpp -o logger/logger.o


# Clean compiled files
clean:
	rm -f *.o packets_monitor/*.o logger/*.o main
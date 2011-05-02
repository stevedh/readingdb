
all:
	cd c && make
	cd iface_bin && make

clean:
	cd c && make clean
	cd iface_bin && make clean

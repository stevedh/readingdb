
VAR=$1
BINDIR=$2

cat <<EOF
check process readingdb pidfile $VAR/run/readingdb.pid
	start program = "/sbin/start-stop-daemon --make-pidfile --pidfile $VAR/run/readingdb.pid --background --exec $BINDIR/reading-server --start -- -d $VAR/lib/readingdb -p 4242"
	stop program = "/sbin/start-stop-daemon --pidfile $VAR/run/readingdb.pid --stop"
EOF

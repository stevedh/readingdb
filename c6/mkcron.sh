
VAR=$1
BINDIR=$2

cat <<EOF
# run every five minutes to update readingdb data sketches.
# disabled by default -- uncomment to enable
# */5 * * * * $BINDIR/reading-sketch -d $VAR/lib/readingdb
EOF

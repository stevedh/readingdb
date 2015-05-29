#!/bin/bash

# Reference:
# http://docs.oracle.com/cd/E17275_01/html/programmer_reference/transapp_archival.html

# see the "hot backup" steps.
#
# The approach used here is to first upload the database volumes to s3
# simply by copying the database pages, reading them atomically in
# units of at least the database page size.
#
# We then incrementally upload log files until we make a new snapshot.
#
# This technique requires pausing the automatic archiving normal
# deployed with readingdb.  Instead, we will manually delete the files
# using this script once we have successfully archived them to s3.

BUCKET=dev-backup-1
BASE_BACKUP=testbackup
HNAME=$(hostname)
UPLOADPATH="/readingdb/$HNAME/$BASE_BACKUP"
CONTENTTYPE="application/x-compressed-tar"
S3KEY=XXX
S3SECRET=XXX
DB_ARCHIVE="/usr/local/bin/db_archive -h data"
DB_CHECKPOINT="/usr/local/bin/db_checkpoint -h data"

function upload() {
    FILE=$1
    DATE=$(date)
    SIGNSTRING="PUT\n\n$CONTENTTYPE\n$DATE\n/$BUCKET$UPLOADPATH/$FILE"
    SIGNATURE=`echo -en ${SIGNSTRING} | openssl sha1 -hmac ${S3SECRET} -binary | base64`

    echo $SIGNSTRING
    
    echo curl -XPUT -T "$FILE" \
         -H "Host: $BUCKET.s3.amazonaws.com" \
         -H "Date: $DATE" \
         -H "Content-Type: $CONTENTTYPE" \
         -H "Authorization: AWS $S3KEY:$SIGNATURE" \
         https://$BUCKET.sw.amazon.aws.com/$UPLOADPATH/$FILE
}

if [ -z $1 ]; then
    echo
    echo "\tUsage: $0 [base|incremental]"
    echo

    exit 1
fi

echo "Creating checkpoint"


if [ $1 == "base" ]; then
    # upload the database volumes
    for file in $($DB_ARCHIVE -s); do
        upload $file
    done
fi

for file in $(DB_ARCHIVE); do
    # upload the logs
    upload $file
    # if the upload succeeded, we can remove the log.
fi

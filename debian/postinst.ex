#!/bin/sh

adduser --system www-data 2>/dev/null

chown www-data:www-data /var/lib/readingdb

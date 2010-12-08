#!/bin/bash -eu
# -e : Exit immediatly if a command exits with non-zero status
# -u : Treat unset variables as an error while substituting

# Install SQLstream script
# Dave Foster <dfoster@asascience.com>
# Initial 7 Dec 2010

SS_INSTALLER_BIN="/home/daf/Downloads/SQLstream-2.5.0.6080-opto-x86_64.bin"
SS_SEISMIC_JAR="/usr/local/seismic/lib/ucsd-seismic.jar"
RABBITMQ_JAVA_CLIENT_ZIP="/usr/local/rabbitmq-java-client-bin-1.5.3.zip"

SS_FIXED_CLIENT="/home/daf/tmp/sqlstream/sqllineClient"

# ports are specified on the command line
if [ $# -ne 2 ]; then
    echo "Usage: $0 <SDP port ex 5570> <HSQLDB port ex 9001>"
    exit 1
fi

SDPPORT=$1
HSQLDBPORT=$2

# 1. Generate a new directory for sqlstream (and base RABBITDIR off of it)
DIRNAME=`mktemp -d -t sqlstream.XXXXXX`
RABBITBASE=`basename $RABBITMQ_JAVA_CLIENT_ZIP`
RABBITDIR=`dirname $DIRNAME`/${RABBITBASE%.zip}

# 2. Install SQLstream daemon into new temp dir
$SS_INSTALLER_BIN --mode unattended --prefix $DIRNAME

# 3. Fix daemon file
DAEMONPATH=$DIRNAME/bin/SQLstreamd
chmod +w $DAEMONPATH
patch --no-backup-if-mismatch -d $DIRNAME/bin >/dev/null <<'EOF'
--- /tmp/sqlstream.EZUzM6/bin/SQLstreamd	2010-11-17 17:49:05.000000000 -0500
+++ sqlstream/SQLstreamd	2010-12-03 09:21:52.365831757 -0500
@@ -202,13 +202,6 @@
     exit 1
 fi
 
-# restore from checkpoint, if any
-if ! $ASPEN_BIN/checkpoint --restore
-then
-    echo "----- Restore failed - Exiting -----"
-    exit 1
-fi
-
 # Start external HSQLDB process, if needed
 if [ "$START_HSQLDB" -eq 1 ]; then
     start_external_hsqldb
@@ -260,11 +253,4 @@
     exit $SERVER_EXIT_CODE
 fi
 
-# save a copy of the repository only on clean exit
-echo "----- Checkpoint repository -----";
-if ! $ASPEN_BIN/checkpoint --create; then
-    echo "----- WARNING - Checkpoint failed -----";
-    exit 1
-fi
-
 exit 0
EOF
chmod -w $DAEMONPATH

# 4. Fix client file
CLIENTPATH=$DIRNAME/bin/sqllineClient
chmod +w $CLIENTPATH
patch --no-backup-if-mismatch -d $DIRNAME/bin >/dev/null <<'EOF'
--- /tmp/sqlstream.H3AvjQ/bin/sqllineClient	2010-11-17 17:49:05.000000000 -0500
+++ sqlstream/sqllineClient	2010-12-03 09:22:30.893290333 -0500
@@ -51,19 +51,23 @@
 fi
 
 # if still have argument(s), must be connection properties files
+CONNFILE=0
 if [ -n "$1" ]; then
     ARGS=
     while [ -n "$1" ]; do
         PROP=$1.conn.properties
         if [ -e $PROP ]; then
             ARGS="$ARGS $PROP"
+            CONNFILE=1
         else
             ARGS="$ARGS $1"
         fi
         shift
     done
     echo $ARGS
-else
+fi
+
+if [ $CONNFILE -eq 0 ]; then
     # add session name
     SELF=`basename $0`
     set +e  # ignore non-zero return
@@ -86,9 +90,12 @@
     # so CLIENT_DRIVER_URL must not contain embedded blanks
 
     ARGS="\
+      $ARGS \
       -u ${CLIENT_DRIVER_URL} \
       -d com.sqlstream.aspen.vjdbc.AspenDriver \
       -n sa"
+
+    echo $ARGS
 fi
 
 #REMOTE_DBG=-agentlib:jdwp=transport=dt_socket,address=8003,server=y,suspend=n
EOF
chmod -w $CLIENTPATH

# 5. Change SDP port
echo -e "aspen.sdp.port=$SDPPORT\naspen.controlnode.url=sdp://localhost:$SDPPORT" >$DIRNAME/aspen.custom.properties

# 6. Change HSQLDB port in props file
HSQLDBPROPSTEMP=`mktemp -t`
PROPSFILE=$DIRNAME/catalog/ReposStorage.hsqldbserver.properties
HSQLDBPROPSPERMS=`stat --format=%a $PROPSFILE`
sed "s/:9001/:$HSQLDBPORT/" <$PROPSFILE >$HSQLDBPROPSTEMP
chmod +w $PROPSFILE
mv $HSQLDBPROPSTEMP $PROPSFILE
chmod $HSQLDBPROPSPERMS $PROPSFILE

# 7. Change HSQLDB port in binary file
HSQLDBBINTEMP=`mktemp -t`
HSQLDBBIN=$DIRNAME/bin/hsqldb
HSQLDBPERMS=`stat --format=%a $HSQLDBBIN`
sed "s/DB_PORT=9001/DB_PORT=$HSQLDBPORT/" <$HSQLDBBIN >$HSQLDBBINTEMP
chmod +w $HSQLDBBIN
mv $HSQLDBBINTEMP $HSQLDBBIN
chmod $HSQLDBPERMS $HSQLDBBIN

# 8. Copy UCSD seismic application jar to plugin dir
cp $SS_SEISMIC_JAR $DIRNAME/plugin

# 9. Unzip rabbitmq client zipfile to a known location
# possible security risk : using known file
if [ ! -d "${RABBITDIR}" ]; then
    unzip $RABBITMQ_JAVA_CLIENT_ZIP -d $RABBITDIR >/dev/null
fi

# 10. Symlink things in plugin/autocp dir
AUTOCPDIR=$DIRNAME/plugin/autocp
ln -s $RABBITDIR/rabbitmq-client.jar $AUTOCPDIR/rabbitmq-client.jar
ln -s $RABBITDIR/commons-cli-1.1.jar $AUTOCPDIR/commons-cli-1.1.jar
ln -s $RABBITDIR/commons-io-1.2.jar $AUTOCPDIR/commons-io-1.2.jar
ln -s $DIRNAME/plugin/AmqpAdapter.jar $AUTOCPDIR/AmqpAdapter.jar

# DONE!

# echo location of sqlstream install dir to stdout and exit
echo $DIRNAME
exit 0


#!/bin/bash

echoHelp()
{
  echo "Usage:"
  echo "    $0 command [arguments...]"
  echo "Commands:"
  echo "    help             Print HELP messages"
  echo "    status [name]    Show status of ConveyorSet(s)"
  echo "    reread           Reload the configuration files"
  echo "    update [name]    Reload config and start/stop as necessary"
  echo "    start [name]     Start ConveyorSet(s)"
  echo "    stop [name]      Stop ConveyorSet(s)"
  echo "    restart [name]   Restart ConveyorSet(s)"
  echo ""
}

if [ "$1" = "help" ]; then
  echoHelp
  exit 1
fi

# java command
cmd="/usr/lib/jvm/java-17/bin/java"

if [ "x$cmd" = "x" ]; then
  cmd=`type -p java`
fi

if [ "x$cmd" = "x" ]; then
  if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]]; then
    cmd="$JAVA_HOME/bin/java"
  else
    echo "Error: missing java"
    exit 1
  fi
  # check java version
  jvm_version=$("$cmd" -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ "$jvm_version" < "17" ]]; then
    echo "Error: java17 is required."
    exit 1
  fi
fi

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ] ; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRGDIR=`dirname "$PRG"`
cd ${PRGDIR}

# Project Version
PRG_VERSION="1.0.0"

GC_OPTS="-XX:+UseEpsilonGC"
JVM_OPTS="${GC_OPTS}"

# start program
exec $cmd ${JVM_OPTS} -jar conveyor-sets-${PRG_VERSION}.jar admin $*

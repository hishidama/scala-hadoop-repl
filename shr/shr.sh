BASEDIR=$(cd $(dirname $0);pwd)

cygwin=false
case "$(uname)" in
CYGWIN*) cygwin=true;;
esac

if $cygwin; then
  HADOOP_CLASSPATH+=:$(cygpath -u "$SCALA_HOME/lib/*")
else
  HADOOP_CLASSPATH+=:"$SCALA_HOME/lib/*"
fi
export HADOOP_CLASSPATH
export HADOOP_OPTS=-Dscala.usejavacp=true

SHR_CLASSPATH=$BASEDIR/shr.jar:$SHR_CLASSPATH
if [ "$ASAKUSA_HOME" != "" ]; then
  if $cygwin; then
    _ASAKUSA_HOME=$(cygpath -u "$ASAKUSA_HOME")
  else
    _ASAKUSA_HOME=$ASAKUSA_HOME
  fi
  for j in $_ASAKUSA_HOME/core/lib/asakusa-runtime*.jar $_ASAKUSA_HOME/ext/lib/*.jar
  do
    SHR_CLASSPATH+=:$j
  done
fi
if $cygwin; then
  SHR_CLASSPATH=$(cygpath -mp "$SHR_CLASSPATH")
fi

hadoop scala.tools.nsc.MainGenericRunner -cp "$SHR_CLASSPATH" -Yrepl-sync -i shr.scala
#hadoop scala.tools.nsc.MainGenericRunner -cp "$SHR_CLASSPATH" -i shr.scala
#hadoop scala.tools.nsc.MainGenericRunner -cp "$SHR_CLASSPATH"

if ! $cygwin; then
  reset
fi

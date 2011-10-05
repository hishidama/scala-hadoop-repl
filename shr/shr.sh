
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

SHR_CLASSPATH=shr.jar:$SHR_CLASSPATH
if $cygwin; then
  SHR_CLASSPATH=$(cygpath -mp "$SHR_CLASSPATH")
fi

#hadoop scala.tools.nsc.MainGenericRunner -cp "$SHR_CLASSPATH" -i shr.scala
hadoop scala.tools.nsc.MainGenericRunner -cp "$SHR_CLASSPATH"

if ! $cygwin; then
  reset
fi

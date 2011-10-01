
cygwin=false
case "$(uname)" in
CYGWIN*) cygwin=true;;
esac

if $cygwin; then
  HADOOP_CLASSPATH=$(cygpath -u "$SCALA_HOME/lib/*")
else
  HADOOP_CLASSPATH="$SCALA_HOME/lib/*"
fi
export HADOOP_CLASSPATH
export HADOOP_OPTS=-Dscala.usejavacp=true
#hadoop scala.tools.nsc.MainGenericRunner -cp shr.jar -i shr.scala
hadoop scala.tools.nsc.MainGenericRunner -cp shr.jar

if ! $cygwin; then
  reset
fi

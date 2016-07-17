echo Run with -help for options...
echo

BASEDIR=$(dirname "$0")
java -cp "$BASEDIR/lib/*" jmat.scalapocs.server.Main "$@"

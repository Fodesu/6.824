TMPFILE=$(mktemp /tmp/foo-XXXXX)
echo "Hello World" >> $TMPFILE
cat $TMPFILE
rm $TMPFILE

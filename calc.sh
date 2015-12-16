t1=`head -1 learner.1.log|sed 's/\[\([0-9]*\)\][0-9]*/\1/g'`
t2=`tail -1 learner.1.log|sed 's/\[\([0-9]*\)\][0-9]*/\1/g'`
line=`cat learner.1.log|wc -l`
echo $line" lines"
echo "scale=5; $line*1000/$((t2-t1))"|bc

ant clean;ant;rm ./*.log;./start-all.sh
sleep 20
for i in `ps|grep 'start-all\|java\|check-proposer'|awk '{print $1}'`;do kill $i;done
sh calc.sh

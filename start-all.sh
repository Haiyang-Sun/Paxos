./client.sh 1 config < test-input > client.1.log 2>&1 &
./client.sh 2 config < test-input2 > client.2.log 2>&1 &
./proposer.sh 1 config > proposer.log 2>&1 &
./proposer.sh 2 config > proposer.log 2>&1 &
./acceptor.sh 1 config > acceptor.1.log 2>&1 &
./acceptor.sh 2 config > acceptor.2.log 2>&1 &
./acceptor.sh 3 config > acceptor.3.log 2>&1 &
./learner.sh 1 config > learner.1.log 2>&1 &
./learner.sh 2 config > learner.2.log 2>&1 &
./learner.sh 3 config > learner.3.log 2>&1 &

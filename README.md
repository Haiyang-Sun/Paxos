#Paxos Implementation (by Haiyang and Rui)

##Installation

The code were written in Java. To compile the project, you need [*ant*](http://ant.apache.org) installed.

To build the projct in the root directory of it:

```
$ ant clean #In case the previous bin files exist
$ ant
```

##Run
The scripts and APIs are constructed the same as the specification describes. To change configurations, see [the next section](##Configurations)


##Configurations

Configurations can be changed in the file/class *src/ch/usi/inf/paxos/PaxosConfig.java*. In which:

1. To change to output log level (defaultly all set to false to pass the check), modify:
 - *debug* to enable debug infomation.
 - *msgDebug* to enable primitive message infomation.
 - *msgHeartBeat* to enable heartbeat information.
 - *submitDebug* to log submission from clients to proposers.
 - *logClock* to add a timestamp(in milliseconds) for evry normal log entry. Used for performance management.

2. *NUM_ACCEPTORS*, *NUM_QUORUM* defines the number of acceptors and number of quorum. *escapePhase1* defines if the feature of elimination of Phase1 will turn on(will make a good speedup if most of the time there is only one leader).

3. Network related settings. Normally need no changes.

4. Related time intervals (e.g. for periodical dispaching) can also be set. Mind that if some of them are set too low the latency will increase.

**Note**: After changing the configurations, you have to re-build the project.

##Examples

- The project is compatible with the testcases provided.

- Moreover, I use a simple alltest.sh as a test case, which runs the script of start-all.sh and check the OPs/sec after 20s(if *logClock* is set to true).
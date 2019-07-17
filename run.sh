#!/usr/bin/env bash
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
#if hash docker 2>/dev/null; then
    # for docker lovers
#    docker build . -t meirkhan/kafka-connect-project-1.0-SNAPSHOT
#    docker run --net=host --rm -t \
#           -v $(pwd)/offsets:/kafka-connect-project/offsets \
#           meirkhan/kafka-connect-project-1.0-SNAPSHOT
#elif hash connect-standalone 2>/dev/null; then
    # for mac users who used homebrew
#    connect-standalone config/worker.properties config/MongodbSourceConnector.properties
#elif [[ -z $KAFKA_HOME ]]; then
    # for people who installed kafka vanilla
#    $KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/etc/schema-registry/connect-avro-standalone.properties config/MongodbSourceConnector.properties
if [[ -z $CONFLUENT_HOME ]]; then
    # for people who installed kafka confluent flavour
    /usr/local/confluent-5.2.1/bin/connect-standalone /usr/local/confluent-5.2.1/etc/schema-registry/connect-avro-standalone.properties config/MongodbSourceConnector.properties
else
    printf "Couldn't find a suitable way to run kafka connect for you.\n \
Please install Docker, or download the kafka binaries and set the variable KAFKA_HOME."
fi;


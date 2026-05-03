FROM confluentinc/cp-kafka-connect:7.5.0

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.7 \
 && mkdir -p /usr/share/confluent-hub-components/google-pubsub-kafka-connector \
 && curl -fsSL "https://repo1.maven.org/maven2/com/google/cloud/pubsub-group-kafka-connector/1.3.2/pubsub-group-kafka-connector-1.3.2.jar" \
    -o /usr/share/confluent-hub-components/google-pubsub-kafka-connector/pubsub-group-kafka-connector-1.3.2.jar

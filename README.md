# README

Create topics

```
kafka-topics --create --topic streams-input --bootstrap-server localhost:9092
kafka-topics --create --topic streams-output --bootstrap-server localhost:9092
```

Consume from topics

```
kafka-console-consumer --topic streams-input --from-beginning --bootstrap-server localhost:9092
kafka-console-consumer --topic streams-output --from-beginning --bootstrap-server localhost:9092
```

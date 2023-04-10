## Instruction to run the spark streaming job

- before you run please make sure your kafka is running
- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 streaming-job.py

- - Note: I am using confluent kafka-7.x version and to produce messages to topic i am taking the help of "confluent control center".
you can always use some code to publish messages or a cmd tool like "kafka-console-publisher.sh"
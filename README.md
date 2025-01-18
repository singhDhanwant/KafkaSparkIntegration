# KafkaSparkIntegration

## To test the Streams locally

1. Make sure to have Docker Desktop Installed.
2. Go inside KafkaJs directory.
3. Execute `docker-compose.yml` file to start kafka.
    ```
    dockercompose up
    ```
4. Run the producer code and check the console for the successful message.
    ```
    node kafkaProducer.mjs
    ```
5. Run `KafkaStructuredStreamingConsumer.java` class and check the console. You will see the streams being logged there.


import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "my-client",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();
await producer.disconnect;

try {
    await producer.connect();
    console.log("Connected");
    while (true) {
        await producer.send({
            topic: "test-topic",
            messages: [{ value: "Hello Kafka" }],
        });
        console.log("Message sent");
    }
    // await producer.disconnect();
} catch (error) {
    console.log(error.reason);
}

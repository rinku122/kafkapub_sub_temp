const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();
const kafka = new Kafka({
  clientId: "user-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "user-group" });

async function publishRequest(userId) {
  const message = {
    userId,
  };

  await producer.connect();
  await producer.send({
    topic: "request-topic",
    messages: [{ value: JSON.stringify(message) }],
  });
  await producer.disconnect();
}

app.get("/posts/:userId", async (req, res) => {
  const userId = req.params.userId;

  await publishRequest(userId);

  await consumer.connect();
  await consumer.subscribe({ topic: "response-topic" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const response = JSON.parse(message.value);
      if (response.userId === userId) {
        res.json(response.posts);
        consumer.disconnect();
      }
    },
  });
});

app.listen(3000, () => {
  console.log("User service API server running on port 3000");
});

const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "post-service",
  brokers: ["localhost:9092"], // Replace with your Kafka broker address
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "post-group" });

async function publishResponse(userId, posts) {
  const message = {
    userId,
    posts,
  };

  await producer.connect();
  await producer.send({
    topic: "response-topic", // Replace with the response topic name
    messages: [{ value: JSON.stringify(message) }],
  });
  await producer.disconnect();
}

async function consumeRequest() {
  await consumer.connect();
  await consumer.subscribe({ topic: "request-topic" }); // Replace with the request topic name

  await consumer.run({
    eachMessage: async ({ message }) => {
      const request = JSON.parse(message.value);
      const userId = request.userId;

      // Fetch posts for the user (replace with your logic to fetch posts)
      const posts = ["Post 1", "Post 2", "Post 3"];

      // Send the response with the fetched posts
      await publishResponse(userId, posts);
    },
  });
}

async function start() {
  await consumeRequest();
}

start().catch((error) => {
  console.error("Error in consuming request:", error);
});

// Run the post service on a different port
const express = require("express");
const app = express();
const port = 4000; // Replace with the desired port number

app.listen(port, () => {
  console.log(`Post service API server running on port ${port}`);
});

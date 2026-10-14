import { createClient } from 'redis';

// Create a high-performance Redis client
export const redisClient = createClient({
  url: 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.error('Redis Client Error', err));
redisClient.on('connect', () => console.log('Connected to Redis Ingestion Buffer'));

// We will call this from our main server file
export const connectRedis = async () => {
  await redisClient.connect();
};
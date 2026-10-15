import Fastify from 'fastify';
import alertRoutes from './api/alerts.js';
import { connectRedis, redisClient } from './config/redis.js';
import type { SensorReading } from './types/sensor.js';
import ruleRoutes from './api/rules.js';

const app = Fastify({
  logger: true, // Built-in Pino logger for extreme speed
  bodyLimit: 5 * 1024 * 1024
});

// The ingestion endpoint for single readings and batches
app.post('/ingest', async (request, reply) => {
  const data = request.body as SensorReading | SensorReading[];
  const readings = Array.isArray(data) ? data : [data];

  try {
    // Process the batch and push to Redis Stream named 'sensor_stream'
    const pipeline = redisClient.multi();
    
    for (const reading of readings) {
      pipeline.xAdd('sensor_stream', '*', {
        sensorId: reading.sensorId,
        type: reading.type,
        value: reading.value.toString(),
        timestamp: reading.timestamp
      });
    }
    
    // Execute all Redis commands in a single network trip
    await pipeline.exec();

    // 202 Accepted: The request has been accepted for processing, but processing is not complete.
    reply.code(202).send({ status: 'queued', count: readings.length });
  } catch (error) {
    request.log.error(error);
    reply.code(500).send({ error: 'Failed to queue readings' });
  }
});
const start = async () => {
  try {
    await connectRedis();
    
    // Register the rule routes
    await app.register(ruleRoutes);
    await app.register(alertRoutes);

    await app.listen({ port: 3000, host: '0.0.0.0' });
    console.log('🚀 Server listening at http://localhost:3000');
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
};


start();

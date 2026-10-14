import { redisClient, connectRedis } from '../config/redis.js';
import { databasePool } from '../config/database.js';

const STREAM_KEY = 'sensor_stream';
const BATCH_SIZE = 1000;

async function processBatch() {
  try {
    // 1. Read a batch of records from the top of the Redis stream
    const messages = (await redisClient.xRange(STREAM_KEY, '-', '+', { COUNT: BATCH_SIZE })) ?? [];
    
    if (messages.length === 0) return 0;

    // 2. Prepare the bulk insert query
    const values: Array<string | number> = [];
    const placeholders: string[] = [];
    const messageIds: string[] = [];
    const invalidMessageIds: string[] = [];
    let paramIndex = 1;

    for (const msg of messages) {
      const data = msg.message;

      const value = Number(data.value);
      if (
        !data.sensorId ||
        !data.type ||
        !Number.isFinite(value) ||
        !data.timestamp ||
        Number.isNaN(Date.parse(data.timestamp))
      ) {
        console.error(`Discarding malformed Redis stream entry ${msg.id}`);
        invalidMessageIds.push(msg.id);
        continue;
      }

      messageIds.push(msg.id);
      values.push(data.sensorId, data.type, value, new Date(data.timestamp).toISOString());
      placeholders.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
    }

    if (invalidMessageIds.length > 0) {
      await redisClient.xDel(STREAM_KEY, invalidMessageIds);
    }

    if (messageIds.length === 0) return 0;

    const query = `
      INSERT INTO sensor_readings (sensor_id, metric_type, value, time)
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (time, sensor_id, metric_type) DO NOTHING
    `;

    // 3. Execute the single bulk insert
    await databasePool.query(query, values);

    // 4. Clear the processed messages from Redis so they aren't read again
    await redisClient.xDel(STREAM_KEY, messageIds);

    return messageIds.length;
  } catch (error) {
    console.error('Error processing batch:', error);
    return 0;
  }
}

async function startWorker() {
  await connectRedis();
  console.log('👷 Background worker started. Polling Redis...');

  // The infinite event loop
  while (true) {
    const processedCount = await processBatch();
    
    // If queue is empty, sleep for 100ms to prevent CPU thrashing
    if (processedCount === 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    } else {
      console.log(`✅ Processed and inserted batch of ${processedCount} readings.`);
    }
  }
}

startWorker();

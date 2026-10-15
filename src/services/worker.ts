import { redisClient, connectRedis } from '../config/redis.js';
import { databasePool } from '../config/database.js';
import type { PoolClient } from 'pg';
import type { SensorReading } from '../types/sensor.js';
import { matchesThreshold, type RuleOperator } from '../domain/alert-rules.js';

const STREAM_KEY = 'sensor_stream';
const BATCH_SIZE = 1000;
const IDLE_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 30_000;

interface AlertRuleRow {
  id: string;
  sensor_type: string;
  operator: RuleOperator;
  threshold: number;
  window_minutes: number;
}

interface WindowAggregateRow {
  sensor_id: string;
  avg_val: string | number | null;
}

interface InsertedReadingRow {
  sensor_id: string;
  metric_type: string;
  value: number;
  time: Date | string;
}

interface PendingAlert {
  ruleId: string;
  sensorId: string;
  value: number;
  message: string;
  dedupeKey: string;
}

function parseStreamReading(data: Record<string, string>): SensorReading | null {
  const sensorId = data.sensorId?.trim();
  const type = data.type?.trim();
  const value = Number(data.value);
  const timestamp = data.timestamp;

  if (!sensorId || !type || !Number.isFinite(value) || !timestamp || Number.isNaN(Date.parse(timestamp))) {
    return null;
  }

  return { sensorId, type, value, timestamp: new Date(timestamp).toISOString() };
}

function alertDedupeKey(ruleId: string, sensorId: string): string {
  return `${ruleId}:${sensorId}`;
}

async function evaluateRules(client: PoolClient, readings: SensorReading[]): Promise<void> {
  if (readings.length === 0) return;

  const rulesResult = await client.query<AlertRuleRow>(
    'SELECT id, sensor_type, operator, threshold, window_minutes FROM alert_rules WHERE enabled = true'
  );
  const rules = rulesResult.rows;
  const pendingByKey = new Map<string, PendingAlert>();

  for (const reading of readings) {
    for (const rule of rules) {
      if (rule.window_minutes !== 0 || rule.sensor_type !== reading.type) continue;
      if (!matchesThreshold(reading.value, rule.operator, rule.threshold)) continue;

      const dedupeKey = alertDedupeKey(rule.id, reading.sensorId);
      pendingByKey.set(dedupeKey, {
        ruleId: rule.id,
        sensorId: reading.sensorId,
        value: reading.value,
        message: `Single reading ${reading.value} ${rule.operator} ${rule.threshold}`,
        dedupeKey
      });
    }
  }

  const sensorsByType = new Map<string, Set<string>>();
  for (const reading of readings) {
    const sensors = sensorsByType.get(reading.type) ?? new Set<string>();
    sensors.add(reading.sensorId);
    sensorsByType.set(reading.type, sensors);
  }

  for (const rule of rules) {
    if (rule.window_minutes <= 0) continue;
    const sensors = [...(sensorsByType.get(rule.sensor_type) ?? [])];
    if (sensors.length === 0) continue;

    const windowResult = await client.query<WindowAggregateRow>(
      `SELECT sensor_id, AVG(value) AS avg_val
       FROM sensor_readings
       WHERE metric_type = $1
         AND sensor_id = ANY($3::text[])
         AND time >= NOW() - ($2::integer * INTERVAL '1 minute')
       GROUP BY sensor_id`,
      [rule.sensor_type, rule.window_minutes, sensors]
    );

    for (const row of windowResult.rows) {
      if (row.avg_val === null) continue;
      const average = Number(row.avg_val);
      if (!matchesThreshold(average, rule.operator, rule.threshold)) continue;

      const dedupeKey = alertDedupeKey(rule.id, row.sensor_id);
      pendingByKey.set(dedupeKey, {
        ruleId: rule.id,
        sensorId: row.sensor_id,
        value: average,
        message: `Rolling average ${average.toFixed(2)} ${rule.operator} ${rule.threshold} over ${rule.window_minutes}m`,
        dedupeKey
      });
    }
  }

  const alerts = [...pendingByKey.values()];
  if (alerts.length === 0) return;

  const values: (string | number)[] = [];
  const placeholders = alerts.map((alert, index) => {
    const start = index * 5;
    values.push(alert.ruleId, alert.sensorId, alert.value, alert.message, alert.dedupeKey);
    return `($${start + 1}, $${start + 2}, $${start + 3}, $${start + 4}, $${start + 5})`;
  });

  await client.query(
    `INSERT INTO alerts (rule_id, sensor_id, current_value, message, dedupe_key)
     VALUES ${placeholders.join(', ')}
     ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING`,
    values
  );
}

async function persistBatch(readings: SensorReading[]): Promise<void> {
  if (readings.length === 0) return;

  const client = await databasePool.connect();
  try {
    await client.query('BEGIN');

    const values: (string | number)[] = [];
    const placeholders = readings.map((reading, index) => {
      const start = index * 4;
      values.push(reading.sensorId, reading.type, reading.value, reading.timestamp);
      return `($${start + 1}, $${start + 2}, $${start + 3}, $${start + 4})`;
    });

    const result = await client.query<InsertedReadingRow>(
      `INSERT INTO sensor_readings (sensor_id, metric_type, value, time)
       VALUES ${placeholders.join(', ')}
       ON CONFLICT (time, sensor_id, metric_type) DO NOTHING
       RETURNING sensor_id, metric_type, value, time`,
      values
    );

    // Only new rows can trigger reading-based work. A retry after a committed
    // insert therefore cannot emit the same single-reading alert again.
    const newlyInserted = result.rows.map(row => ({
      sensorId: row.sensor_id,
      type: row.metric_type,
      value: Number(row.value),
      timestamp: new Date(row.time).toISOString()
    }));
    await evaluateRules(client, newlyInserted);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

async function processBatch(): Promise<number> {
  const messages = (await redisClient.xRange(STREAM_KEY, '-', '+', { COUNT: BATCH_SIZE })) ?? [];
  if (messages.length === 0) return 0;

  const readings: SensorReading[] = [];
  const validMessageIds: string[] = [];
  const invalidMessageIds: string[] = [];

  for (const message of messages) {
    const reading = parseStreamReading(message.message);
    if (!reading) {
      console.error(`Discarding malformed Redis stream entry ${message.id}`);
      invalidMessageIds.push(message.id);
      continue;
    }
    readings.push(reading);
    validMessageIds.push(message.id);
  }

  if (invalidMessageIds.length > 0) await redisClient.xDel(STREAM_KEY, invalidMessageIds);
  if (validMessageIds.length === 0) return invalidMessageIds.length;

  await persistBatch(readings);
  // A failure here is safe: persisted readings are idempotent and rule
  // evaluation only runs for newly inserted rows on a retry.
  await redisClient.xDel(STREAM_KEY, validMessageIds);
  return messages.length;
}

async function startWorker(): Promise<void> {
  await connectRedis();
  console.log('Background worker started; polling the Redis sensor stream.');
  let consecutiveFailures = 0;

  while (true) {
    try {
      const processedCount = await processBatch();
      consecutiveFailures = 0;
      if (processedCount === 0) {
        await new Promise(resolve => setTimeout(resolve, IDLE_DELAY_MS));
      } else {
        console.log(`Processed ${processedCount} stream entries.`);
      }
    } catch (error) {
      console.error('Error processing Redis stream batch:', error);
      consecutiveFailures += 1;
      const retryDelay = Math.min(1000 * 2 ** Math.min(consecutiveFailures - 1, 5), MAX_RETRY_DELAY_MS);
      await new Promise(resolve => setTimeout(resolve, retryDelay));
    }
  }
}

startWorker().catch(error => {
  console.error('Worker failed to start:', error);
  process.exitCode = 1;
});

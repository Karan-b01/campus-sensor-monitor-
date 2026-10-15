import { redisClient, connectRedis } from '../config/redis.js';
import { databasePool } from '../config/database.js';
import type { SensorReading } from '../types/sensor.js';
import { matchesThreshold, type RuleOperator } from '../domain/alert-rules.js';

const STREAM_KEY = 'sensor_stream';
const BATCH_SIZE = 1000;

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

interface PendingAlert {
  ruleId: string;
  sensorId: string;
  value: number;
  message: string;
}

async function processBatch() {
  try {
    const messages = (await redisClient.xRange(STREAM_KEY, '-', '+', { COUNT: BATCH_SIZE })) ?? [];
    if (messages.length === 0) return 0;

    // 1. Prepare data for bulk insert
    const values: (string | number)[] = [];
    const placeholders: string[] = [];
    const messageIds: string[] = [];
    const invalidMessageIds: string[] = [];
    const parsedReadings: SensorReading[] = [];
    let paramIndex = 1;

    for (const msg of messages) {
      const data = msg.message;
      const sensorId = data.sensorId;
      const type = data.type;
      const value = Number(data.value);
      const timestamp = data.timestamp;
      if (!sensorId || !type || !Number.isFinite(value) || !timestamp || Number.isNaN(Date.parse(timestamp))) {
        console.error(`Discarding malformed Redis stream entry ${msg.id}`);
        invalidMessageIds.push(msg.id);
        continue;
      }

      messageIds.push(msg.id);
      parsedReadings.push({
        sensorId,
        type,
        value,
        timestamp: new Date(timestamp).toISOString()
      });
      values.push(sensorId, type, value, timestamp);
      placeholders.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
    }

    if (invalidMessageIds.length > 0) {
      await redisClient.xDel(STREAM_KEY, invalidMessageIds);
    }
    if (messageIds.length === 0) return invalidMessageIds.length;

    // 2. Bulk Insert into TimescaleDB
    const insertQuery = `
      INSERT INTO sensor_readings (sensor_id, metric_type, value, time)
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (time, sensor_id, metric_type) DO NOTHING
    `;
    await databasePool.query(insertQuery, values);

    // 3. Fetch Active Rules
    const rulesRes = await databasePool.query<AlertRuleRow>(
      'SELECT id, sensor_type, operator, threshold, window_minutes FROM alert_rules WHERE enabled = true'
    );
    const rules = rulesRes.rows;
    const singleRules = rules.filter((r: { window_minutes: number }) => r.window_minutes === 0);
    const windowRules = rules.filter((r: { window_minutes: number }) => r.window_minutes > 0);

    const alertInserts: PendingAlert[] = [];

    // 4. Evaluate Single-Reading Rules (In-Memory)
    for (const reading of parsedReadings) {
      const val = reading.value;
      for (const rule of singleRules) {
        if (rule.sensor_type === reading.type && matchesThreshold(val, rule.operator, rule.threshold)) {
          alertInserts.push({
            ruleId: rule.id,
            sensorId: reading.sensorId,
            value: val,
            message: `Single reading ${val} ${rule.operator} ${rule.threshold}`
          });
        }
      }
    }

    // 5. Evaluate Time-Window Rules (Database-Level)
    for (const rule of windowRules) {
      // Calculate the rolling aggregate for the specific window
      const windowQuery = `
        SELECT sensor_id, AVG(value) as avg_val
        FROM sensor_readings
        WHERE metric_type = $1 AND time >= NOW() - ($2::integer * INTERVAL '1 minute')
        GROUP BY sensor_id
      `;
      const windowRes = await databasePool.query<WindowAggregateRow>(windowQuery, [rule.sensor_type, rule.window_minutes]);
      for (const row of windowRes.rows) {
        if (row.avg_val === null) continue;
        const avgVal = Number.parseFloat(String(row.avg_val));
        if (matchesThreshold(avgVal, rule.operator, rule.threshold)) {
          alertInserts.push({
            ruleId: rule.id,
            sensorId: row.sensor_id,
            value: avgVal,
            message: `Rolling average ${avgVal.toFixed(2)} ${rule.operator} ${rule.threshold} over ${rule.window_minutes}m`
          });
        }
      }
    }

    // 6. Bulk Insert Alerts (if any were triggered)
    if (alertInserts.length > 0) {
      const alertValues: (string | number)[] = [];
      const alertPlaceholders: string[] = [];
      let alertParam = 1;

      alertInserts.forEach(alert => {
        alertValues.push(alert.ruleId, alert.sensorId, alert.value, alert.message);
        alertPlaceholders.push(`($${alertParam++}, $${alertParam++}, $${alertParam++}, $${alertParam++})`);
      });

      await databasePool.query(`
        INSERT INTO alerts (rule_id, sensor_id, current_value, message)
        VALUES ${alertPlaceholders.join(', ')}
      `, alertValues);
    }

    // 7. Clear Processed Messages from Redis
    await redisClient.xDel(STREAM_KEY, messageIds);

    return messages.length;
  } catch (error) {
    console.error('Error processing batch:', error);
    return 0;
  }
}

async function startWorker() {
  await connectRedis();
  console.log('👷 Background worker started. Polling Redis & Evaluating Rules...');

  while (true) {
    const processedCount = await processBatch();
    if (processedCount === 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    } else {
      console.log(`✅ Processed ${processedCount} readings and evaluated alerts.`);
    }
  }
}

startWorker();

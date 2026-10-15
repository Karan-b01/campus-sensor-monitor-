import type { SensorReading } from '../types/sensor.js';

export const MAX_INGEST_BATCH_SIZE = 10_000;

export type ReadingBatchResult =
  | { ok: true; readings: SensorReading[] }
  | { ok: false; error: string };

export function parseSensorReading(value: unknown): SensorReading | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;

  const reading = value as Record<string, unknown>;
  if (
    typeof reading.sensorId !== 'string' || !reading.sensorId.trim() ||
    typeof reading.type !== 'string' || !reading.type.trim() ||
    typeof reading.value !== 'number' || !Number.isFinite(reading.value) ||
    typeof reading.timestamp !== 'string' || Number.isNaN(Date.parse(reading.timestamp))
  ) {
    return null;
  }

  return {
    sensorId: reading.sensorId.trim(),
    type: reading.type.trim(),
    value: reading.value,
    timestamp: new Date(reading.timestamp).toISOString()
  };
}

export function parseReadingBatch(payload: unknown): ReadingBatchResult {
  const entries = Array.isArray(payload) ? payload : [payload];
  if (entries.length === 0) return { ok: false, error: 'At least one reading is required' };
  if (entries.length > MAX_INGEST_BATCH_SIZE) {
    return { ok: false, error: `A batch may contain at most ${MAX_INGEST_BATCH_SIZE} readings` };
  }

  const readings: SensorReading[] = [];
  for (const [index, entry] of entries.entries()) {
    const reading = parseSensorReading(entry);
    if (!reading) return { ok: false, error: `Invalid sensor reading at index ${index}` };
    readings.push(reading);
  }

  return { ok: true, readings };
}

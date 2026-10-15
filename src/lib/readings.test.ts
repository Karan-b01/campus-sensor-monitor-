import assert from 'node:assert/strict';
import test from 'node:test';
import { MAX_INGEST_BATCH_SIZE, parseReadingBatch, parseSensorReading } from './readings.js';

const validReading = {
  sensorId: 'library-floor-2',
  type: 'temperature',
  value: 34.5,
  timestamp: '2026-10-12T10:30:00Z'
};

test('parses a valid reading and normalizes its timestamp', () => {
  const reading = parseSensorReading(validReading);
  assert.deepEqual(reading, { ...validReading, timestamp: '2026-10-12T10:30:00.000Z' });
});

test('rejects readings with missing fields, non-finite values, or invalid timestamps', () => {
  assert.equal(parseSensorReading({ ...validReading, sensorId: '  ' }), null);
  assert.equal(parseSensorReading({ ...validReading, value: Number.NaN }), null);
  assert.equal(parseSensorReading({ ...validReading, timestamp: 'not-a-date' }), null);
});

test('accepts one reading or a batch within the configured limit', () => {
  assert.deepEqual(parseReadingBatch(validReading), { ok: true, readings: [parseSensorReading(validReading)] });
  const fullBatch = parseReadingBatch(Array.from({ length: MAX_INGEST_BATCH_SIZE }, () => validReading));
  assert.equal(fullBatch.ok, true);
  if (fullBatch.ok) assert.equal(fullBatch.readings.length, MAX_INGEST_BATCH_SIZE);
});

test('rejects empty, oversized, and partially invalid batches', () => {
  assert.equal(parseReadingBatch([]).ok, false);
  assert.equal(parseReadingBatch(Array.from({ length: MAX_INGEST_BATCH_SIZE + 1 }, () => validReading)).ok, false);
  assert.deepEqual(parseReadingBatch([validReading, { ...validReading, value: 'hot' }]), {
    ok: false,
    error: 'Invalid sensor reading at index 1'
  });
});

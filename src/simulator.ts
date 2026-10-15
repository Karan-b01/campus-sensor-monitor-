interface SensorReading {
  sensorId: string;
  type: string;
  value: number;
  timestamp: string;
}

const API_URL = 'http://localhost:3000/ingest';
const SENSORS = ['library-floor-1', 'library-floor-2', 'cafeteria-main', 'cs-lab-401', 'dorm-building-a'];
const TYPES = ['temperature', 'air_quality', 'occupancy', 'humidity'];

// Helper to generate a random number within a range
const randomInRange = (min: number, max: number) => parseFloat((Math.random() * (max - min) + min).toFixed(2));

function generateBurstData(count: number): SensorReading[] {
  const payload: SensorReading[] = [];
  const now = new Date();

  for (let i = 0; i < count; i++) {
    // Generate organic-looking data
    const type: string = TYPES[Math.floor(Math.random() * TYPES.length)] ?? 'temperature';
    const sensorId: string = SENSORS[Math.floor(Math.random() * SENSORS.length)] ?? 'library-floor-1';
    let value = 0;
    
    if (type === 'temperature') value = randomInRange(20, 45); // Mix of normal and spike temps
    if (type === 'air_quality') value = randomInRange(30, 150); // AQI
    if (type === 'occupancy') value = Math.floor(randomInRange(0, 500));
    if (type === 'humidity') value = randomInRange(30, 80);

    payload.push({
      sensorId,
      type,
      value,
      timestamp: new Date(now.getTime() - i).toISOString() // Keep readings unique at millisecond precision
    });
  }
  
  return payload;
}

async function runLoadTest() {
  const TARGET_READINGS = 10000;
  console.log(`🧪 Generating ${TARGET_READINGS} sensor readings...`);
  
  const payload = generateBurstData(TARGET_READINGS);
  
  console.log(`🚀 Firing burst of ${TARGET_READINGS} readings at the API...`);
  
  const startTime = performance.now();
  
  try {
    const response = await fetch(API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const endTime = performance.now();
    const durationMs = endTime - startTime;
    
    const result = await response.json();
    
    console.log(`\n📊 --- LOAD TEST REPORT ---`);
    console.log(`Status Code: ${response.status} ${response.statusText}`);
    console.log(`Response Data:`, result);
    console.log(`Total Time to Accept 10,000 readings: ${durationMs.toFixed(2)} ms`);

    if (!response.ok) {
      console.error('❌ API rejected the load test request.');
      process.exitCode = 1;
      return;
    }
    
    if (durationMs < 1000) {
      console.log(`✅ SUCCESS: API accepted burst in under 1 second.`);
    } else {
      console.log(`⚠️ WARNING: System took longer than 1 second.`);
    }

  } catch (error) {
    console.error('❌ Load test failed:', error);
  }
}

runLoadTest();

import { type FastifyInstance } from 'fastify';
import { databasePool } from '../config/database.js';

type RuleOperator = '>' | '>=' | '<' | '<=' | '=' | '!=';

interface CreateRuleBody {
  name: string;
  sensorType: string;
  operator: RuleOperator;
  threshold: number;
  windowMinutes?: number;
}

export default async function ruleRoutes(app: FastifyInstance) {
  
  // 1. Create a new alert rule
  app.post<{ Body: CreateRuleBody }>('/rules', async (request, reply) => {
    const { name, sensorType, operator, threshold, windowMinutes = 0 } = request.body;

    if (
      !name?.trim() ||
      !sensorType?.trim() ||
      !['>', '>=', '<', '<=', '=', '!='].includes(operator) ||
      !Number.isFinite(threshold) ||
      !Number.isInteger(windowMinutes) ||
      windowMinutes < 0
    ) {
      return reply.code(400).send({ error: 'Invalid alert rule fields' });
    }
    
    try {
      const result = await databasePool.query(
        `INSERT INTO alert_rules (name, sensor_type, operator, threshold, window_minutes) 
         VALUES ($1, $2, $3, $4, $5) RETURNING *`,
        [name.trim(), sensorType.trim(), operator, threshold, windowMinutes]
      );
      reply.code(201).send(result.rows[0]);
    } catch (error) {
      request.log.error(error);
      reply.code(500).send({ error: 'Failed to create rule' });
    }
  });

  // 2. View all rules
  app.get('/rules', async (request, reply) => {
    try {
      const result = await databasePool.query('SELECT * FROM alert_rules ORDER BY created_at DESC');
      reply.send(result.rows);
    } catch (error) {
      reply.code(500).send({ error: 'Failed to fetch rules' });
    }
  });

  // 3. Enable or Disable a rule
  app.patch('/rules/:id/toggle', async (request, reply) => {
    const { id } = request.params as { id: string };
    const { enabled } = request.body as { enabled: boolean };
    
    try {
      const result = await databasePool.query(
        'UPDATE alert_rules SET enabled = $1 WHERE id = $2 RETURNING *',
        [enabled, id]
      );
      if (result.rowCount === 0) return reply.code(404).send({ error: 'Rule not found' });
      reply.send(result.rows[0]);
    } catch (error) {
      reply.code(500).send({ error: 'Failed to update rule' });
    }
  });
}

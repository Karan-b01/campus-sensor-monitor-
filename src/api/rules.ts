import { type FastifyInstance } from 'fastify';
import { databasePool } from '../config/database.js';
import { isAlertRuleInput, isAlertRulePatch } from '../domain/alert-rules.js';

interface RuleParams {
  id: string;
}

interface RuleRow {
  id: string;
  name: string;
  sensor_type: string;
  operator: string;
  threshold: number;
  window_minutes: number;
}

function validId(id: string): boolean {
  return /^[1-9]\d*$/.test(id);
}

export default async function ruleRoutes(app: FastifyInstance) {
  app.post<{ Body: unknown }>('/rules', async (request, reply) => {
    if (!isAlertRuleInput(request.body)) {
      return reply.code(400).send({ error: 'Invalid alert rule fields' });
    }

    const { name, sensorType, operator, threshold, windowMinutes = 0 } = request.body;
    try {
      const result = await databasePool.query(
        `INSERT INTO alert_rules (name, sensor_type, operator, threshold, window_minutes)
         VALUES ($1, $2, $3, $4, $5) RETURNING *`,
        [name.trim(), sensorType.trim(), operator, threshold, windowMinutes]
      );
      return reply.code(201).send(result.rows[0]);
    } catch (error) {
      request.log.error(error);
      return reply.code(500).send({ error: 'Failed to create rule' });
    }
  });

  app.get('/rules', async (request, reply) => {
    try {
      const result = await databasePool.query('SELECT * FROM alert_rules ORDER BY created_at DESC');
      return reply.send(result.rows);
    } catch (error) {
      request.log.error(error);
      return reply.code(500).send({ error: 'Failed to fetch rules' });
    }
  });

  app.patch<{ Params: RuleParams; Body: unknown }>('/rules/:id', async (request, reply) => {
    const { id } = request.params;
    if (!validId(id)) return reply.code(400).send({ error: 'Invalid rule ID' });
    if (!isAlertRulePatch(request.body)) {
      return reply.code(400).send({ error: 'Invalid alert rule fields' });
    }

    try {
      const currentResult = await databasePool.query<RuleRow>(
        'SELECT id, name, sensor_type, operator, threshold, window_minutes FROM alert_rules WHERE id = $1',
        [id]
      );
      const current = currentResult.rows[0];
      if (!current) return reply.code(404).send({ error: 'Rule not found' });

      const patch = request.body;
      const name = patch.name?.trim() ?? current.name;
      const sensorType = patch.sensorType?.trim() ?? current.sensor_type;
      const operator = patch.operator ?? current.operator;
      const threshold = patch.threshold ?? current.threshold;
      const windowMinutes = patch.windowMinutes ?? current.window_minutes;

      const result = await databasePool.query(
        `UPDATE alert_rules
         SET name = $1, sensor_type = $2, operator = $3, threshold = $4, window_minutes = $5
         WHERE id = $6 RETURNING *`,
        [name, sensorType, operator, threshold, windowMinutes, id]
      );
      return reply.send(result.rows[0]);
    } catch (error) {
      request.log.error(error);
      return reply.code(500).send({ error: 'Failed to update rule' });
    }
  });

  app.patch<{ Params: RuleParams; Body: unknown }>('/rules/:id/toggle', async (request, reply) => {
    const { id } = request.params;
    if (!validId(id)) return reply.code(400).send({ error: 'Invalid rule ID' });

    const body = request.body;
    if (typeof body !== 'object' || body === null || !('enabled' in body) || typeof body.enabled !== 'boolean') {
      return reply.code(400).send({ error: 'enabled must be a boolean' });
    }

    try {
      const result = await databasePool.query(
        'UPDATE alert_rules SET enabled = $1 WHERE id = $2 RETURNING *',
        [body.enabled, id]
      );
      if (result.rowCount === 0) return reply.code(404).send({ error: 'Rule not found' });
      return reply.send(result.rows[0]);
    } catch (error) {
      request.log.error(error);
      return reply.code(500).send({ error: 'Failed to update rule' });
    }
  });
}

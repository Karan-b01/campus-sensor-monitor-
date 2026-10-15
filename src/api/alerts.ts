import { type FastifyInstance } from 'fastify';
import { databasePool } from '../config/database.js';

export default async function alertRoutes(app: FastifyInstance) {

  // 1. View all active alerts
  app.get('/alerts', async (request, reply) => {
    try {
      const result = await databasePool.query(`
        SELECT a.*, r.name as rule_name, r.operator, r.threshold 
        FROM alerts a
        JOIN alert_rules r ON a.rule_id = r.id
        ORDER BY a.triggered_at DESC
      `);
      reply.send(result.rows);
    } catch (error) {
      reply.code(500).send({ error: 'Failed to fetch alerts' });
    }
  });

  // 2. Acknowledge an alert
  app.patch('/alerts/:id/acknowledge', async (request, reply) => {
    const { id } = request.params as { id: string };
    try {
      const result = await databasePool.query(
        `UPDATE alerts SET status = 'ACKNOWLEDGED', acknowledged_at = NOW() 
         WHERE id = $1 AND status = 'OPEN' RETURNING *`,
        [id]
      );
      if (result.rowCount === 0) return reply.code(404).send({ error: 'Alert not found or already acknowledged' });
      reply.send(result.rows[0]);
    } catch (error) {
      reply.code(500).send({ error: 'Failed to acknowledge alert' });
    }
  });

  // 3. Resolve an alert
  app.patch('/alerts/:id/resolve', async (request, reply) => {
    const { id } = request.params as { id: string };
    try {
      const result = await databasePool.query(
        `UPDATE alerts SET status = 'RESOLVED', resolved_at = NOW() 
         WHERE id = $1 RETURNING *`,
        [id]
      );
      if (result.rowCount === 0) return reply.code(404).send({ error: 'Alert not found' });
      reply.send(result.rows[0]);
    } catch (error) {
      reply.code(500).send({ error: 'Failed to resolve alert' });
    }
  });
}

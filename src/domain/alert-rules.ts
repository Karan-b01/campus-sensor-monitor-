export const RULE_OPERATORS = ['>', '>=', '<', '<=', '=', '!='] as const;

export type RuleOperator = (typeof RULE_OPERATORS)[number];

export interface AlertRuleInput {
  name: string;
  sensorType: string;
  operator: RuleOperator;
  threshold: number;
  windowMinutes?: number;
}

const RULE_FIELDS = new Set(['name', 'sensorType', 'operator', 'threshold', 'windowMinutes']);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isRuleOperator(value: unknown): value is RuleOperator {
  return typeof value === 'string' && RULE_OPERATORS.includes(value as RuleOperator);
}

export function isAlertRulePatch(value: unknown): value is Partial<AlertRuleInput> {
  if (!isRecord(value)) return false;
  const keys = Object.keys(value);
  if (keys.length === 0 || keys.some(key => !RULE_FIELDS.has(key))) return false;

  if ('name' in value && (typeof value.name !== 'string' || !value.name.trim())) return false;
  if ('sensorType' in value && (typeof value.sensorType !== 'string' || !value.sensorType.trim())) return false;
  if ('operator' in value && !isRuleOperator(value.operator)) return false;
  if ('threshold' in value && (typeof value.threshold !== 'number' || !Number.isFinite(value.threshold))) return false;
  if ('windowMinutes' in value && (!Number.isInteger(value.windowMinutes) || (value.windowMinutes as number) < 0)) return false;

  return true;
}

export function isAlertRuleInput(value: unknown): value is AlertRuleInput {
  if (!isAlertRulePatch(value) || !isRecord(value)) return false;
  return 'name' in value && 'sensorType' in value && 'operator' in value && 'threshold' in value;
}

export function matchesThreshold(value: number, operator: RuleOperator, threshold: number): boolean {
  switch (operator) {
    case '>': return value > threshold;
    case '>=': return value >= threshold;
    case '<': return value < threshold;
    case '<=': return value <= threshold;
    case '=': return value === threshold;
    case '!=': return value !== threshold;
  }
}

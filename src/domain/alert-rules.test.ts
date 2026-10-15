import assert from 'node:assert/strict';
import test from 'node:test';
import { isAlertRuleInput, isAlertRulePatch, matchesThreshold, type RuleOperator } from './alert-rules.js';

test('evaluates every supported comparison operator', () => {
  const cases: Array<[number, RuleOperator, number, boolean]> = [
    [5, '>', 4, true],
    [5, '>=', 5, true],
    [4, '<', 5, true],
    [5, '<=', 5, true],
    [5, '=', 5, true],
    [5, '!=', 4, true],
    [5, '!=', 5, false]
  ];

  for (const [value, operator, threshold, expected] of cases) {
    assert.equal(matchesThreshold(value, operator, threshold), expected, `${value} ${operator} ${threshold}`);
  }
});

test('validates complete create bodies and partial updates', () => {
  assert.equal(isAlertRuleInput({ name: 'Heat', sensorType: 'temperature', operator: '>', threshold: 40 }), true);
  assert.equal(isAlertRuleInput({ name: 'Heat', sensorType: 'temperature', operator: '>', threshold: 40, windowMinutes: -1 }), false);
  assert.equal(isAlertRulePatch({ threshold: 42 }), true);
  assert.equal(isAlertRulePatch({}), false);
  assert.equal(isAlertRulePatch({ unexpected: true }), false);
});

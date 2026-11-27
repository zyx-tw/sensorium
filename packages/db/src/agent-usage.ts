import type { Pool } from "pg";

/** Agent usage from spans that follow the OpenTelemetry GenAI semantic conventions (gen_ai.*). */

export interface AgentUsageParams {
  project: string;
  since: Date;
  until?: Date;
}

export interface ModelUsage {
  provider: string | null;
  model: string | null;
  operation: string | null;
  calls: number;
  errors: number;
  inputTokens: number;
  outputTokens: number;
  avgDurationMs: number | null;
}

export interface ToolUsage {
  tool: string;
  calls: number;
  errors: number;
  avgDurationMs: number | null;
  maxDurationMs: number | null;
}

export interface AgentUsage {
  models: ModelUsage[];
  tools: ToolUsage[];
  /** The most common error.type values on GenAI spans. */
  errorTypes: { type: string; count: number }[];
}

/** A jsonb attribute as a number when it holds one (a number or a numeric string), else null. */
function numericAttr(key: string): string {
  return `case when jsonb_typeof(attributes -> '${key}') = 'number' then (attributes ->> '${key}')::numeric
               when attributes ->> '${key}' ~ '^[0-9]+(\\.[0-9]+)?$' then (attributes ->> '${key}')::numeric end`;
}

const GEN_AI_SPAN_SQL = `(
    attributes ? 'gen_ai.operation.name'
    or attributes ? 'gen_ai.request.model'
    or attributes ? 'gen_ai.provider.name'
    or attributes ? 'gen_ai.system'
  )`;

const GEN_AI_ERROR_SQL = `(status_code = 'error' or attributes ? 'error.type')`;

/**
 * Token use, calls and errors per model and per tool. Reads the current names
 * first and the older ones as a fallback: gen_ai.provider.name / gen_ai.system,
 * gen_ai.usage.input_tokens / prompt_tokens, output_tokens / completion_tokens.
 */
export async function agentUsage(pool: Pool, params: AgentUsageParams): Promise<AgentUsage> {
  const conditions = ["project = $1", "start_ts >= $2", GEN_AI_SPAN_SQL];
  const values: unknown[] = [params.project, params.since];
  if (params.until) {
    values.push(params.until);
    conditions.push(`start_ts <= $${values.length}`);
  }
  const where = conditions.join(" and ");
  const [models, tools, errorTypes] = await Promise.all([
    pool.query<{
      provider: string | null;
      model: string | null;
      operation: string | null;
      calls: string;
      errors: string;
      input_tokens: string | null;
      output_tokens: string | null;
      avg_ms: number | null;
    }>(
      `select coalesce(attributes ->> 'gen_ai.provider.name', attributes ->> 'gen_ai.system') as provider,
              coalesce(attributes ->> 'gen_ai.response.model', attributes ->> 'gen_ai.request.model') as model,
              attributes ->> 'gen_ai.operation.name' as operation,
              count(*) as calls,
              count(*) filter (where ${GEN_AI_ERROR_SQL}) as errors,
              sum(coalesce(${numericAttr("gen_ai.usage.input_tokens")}, ${numericAttr("gen_ai.usage.prompt_tokens")})) as input_tokens,
              sum(coalesce(${numericAttr("gen_ai.usage.output_tokens")}, ${numericAttr("gen_ai.usage.completion_tokens")})) as output_tokens,
              avg(duration_ms) as avg_ms
         from spans
        where ${where} and coalesce(attributes ->> 'gen_ai.operation.name', '') <> 'execute_tool'
        group by 1, 2, 3
        order by calls desc
        limit 100`,
      values,
    ),
    pool.query<{ tool: string; calls: string; errors: string; avg_ms: number | null; max_ms: number | null }>(
      `select attributes ->> 'gen_ai.tool.name' as tool,
              count(*) as calls,
              count(*) filter (where ${GEN_AI_ERROR_SQL}) as errors,
              avg(duration_ms) as avg_ms,
              max(duration_ms) as max_ms
         from spans
        where ${where} and attributes ->> 'gen_ai.tool.name' is not null
        group by 1
        order by calls desc
        limit 100`,
      values,
    ),
    pool.query<{ type: string; count: string }>(
      `select coalesce(attributes ->> 'error.type', 'error') as type, count(*) as count
         from spans
        where ${where} and ${GEN_AI_ERROR_SQL}
        group by 1
        order by count desc
        limit 20`,
      values,
    ),
  ]);
  const num = (v: string | number | null) => (v === null ? 0 : Number(v));
  const ms = (v: number | string | null) => (v === null ? null : Number(v));
  return {
    models: models.rows.map((r) => ({
      provider: r.provider,
      model: r.model,
      operation: r.operation,
      calls: num(r.calls),
      errors: num(r.errors),
      inputTokens: num(r.input_tokens),
      outputTokens: num(r.output_tokens),
      avgDurationMs: ms(r.avg_ms),
    })),
    tools: tools.rows.map((r) => ({
      tool: r.tool,
      calls: num(r.calls),
      errors: num(r.errors),
      avgDurationMs: ms(r.avg_ms),
      maxDurationMs: ms(r.max_ms),
    })),
    errorTypes: errorTypes.rows.map((r) => ({ type: r.type, count: num(r.count) })),
  };
}

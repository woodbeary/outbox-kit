export type Incoming<T> = { [K in keyof T]?: T[K] | null | undefined };

export function mergeKnown<T extends Record<string, unknown>>(
  existing: T,
  incoming: Incoming<T>,
  options: { clear?: readonly (keyof T)[] } = {},
): T {
  const clear = new Set<keyof T>(options.clear ?? []);
  const merged: T = { ...existing };
  for (const key of Object.keys(incoming) as (keyof T)[]) {
    const value = incoming[key];
    if (value === null || value === undefined) {
      if (clear.has(key)) merged[key] = null as T[keyof T];
      continue;
    }
    merged[key] = value as T[keyof T];
  }
  return merged;
}

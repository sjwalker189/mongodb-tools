export type ToArray<T> = T extends Array<any> ? T : Array<T>;

export type SortDirection = "asc" | "desc";

export type RangeFilter<T> =
  | T
  | { between: [T, T] }
  | { after: T }
  | { before: T };

export const arrayOperators = ["in", "nin", "or"] as const;

export const logicalOperators = ["eq", "ne", "gt", "gte", "lt", "lte"] as const;

export const queryOperators = new Set([...logicalOperators, ...arrayOperators]);

export type LogicalOperator = (typeof logicalOperators)[number];
export type ArrayOperator = (typeof arrayOperators)[number];
export type Operator = LogicalOperator | ArrayOperator;

export type LogicalFilter<T> = [LogicalOperator, T];
export type ArrayFilter<T extends Array<any>> = [ArrayOperator, T];
export type Filter<T> = T | LogicalFilter<T> | ArrayFilter<ToArray<T>>;

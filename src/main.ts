import { Db, Collection } from "mongodb";
import { Filter, Operator, queryOperators } from "./types";

function identityTransformer<V>(value: V) {
  return value;
}

export function match<
  Name extends string,
  MatchValue,
  DatabaseValue extends any,
>({
  fieldName,
  filter,
  transformer,
}: {
  fieldName: Name;
  filter: Filter<MatchValue>;
  transformer?: (value: MatchValue) => DatabaseValue;
}) {
  let operator: Operator | undefined;
  let value: any = filter;
  let transformFilterValue = transformer ?? identityTransformer;

  // Check if the provided filter is a valid query builder tuple
  if (Array.isArray(filter) && filter.length === 2) {
    const [op, match] = filter;
    if (queryOperators.has(op)) {
      operator = op;
      value = match;
    }
  }

  // Apply any user defined transformations to the match value.
  value = Array.isArray(value)
    ? value.map((val) => transformFilterValue(val))
    : transformFilterValue(value);

  // Determine the appropriate operator when not explicitly provided.
  if (!operator) {
    operator = Array.isArray(value) ? "in" : "eq";
  }

  // When performing an `in` filter operation with an empty list of
  // items, no results will be returned.
  // Instead we treat this case as "match all"
  if (operator === "in" && value.length === 0) {
    return {};
  }

  // Transform the given options into a mongodb compatible
  // match statement.
  // e.g. { foo: { $eq: "bar" }  }
  const matchStatement = {
    [fieldName]: {
      [`$${operator}`]: value,
    },
  };

  return matchStatement as Record<Name, QuerySelector<DatabaseValue>>;
}

/**
 * Get all collection names from the database
 */
export async function getCollectionNames(db: Db) {
  const collections: Array<{ name: string }> = await db
    .listCollections()
    .toArray();
  return collections.map((collection) => collection.name);
}

/**
 * Drop a set of collections from the database
 */
export async function dropCollections(db: Db, collections: Array<string>) {
  const collectionNames = new Set<string>(await getCollectionNames(db));

  for (const collection of collections) {
    if (collectionNames.has(collection)) {
      try {
        await db.dropCollection(collection);
      } catch (e) {
        console.error("Failed to drop collection " + collection);
        console.error(e);
      }
    }
  }
}

/**
 * Drop all collections form a database
 */
export async function dropAllCollections(db: Db) {
  for (const collection of await getCollectionNames(db)) {
    try {
      await db.dropCollection(collection);
    } catch (e) {
      console.error("Failed to drop collection " + collection);
      console.error(e);
    }
  }
}

/**
 * Clear all records from a collection
 */
export async function truncate(db: Db, collection: string) {
  const collectionNames = new Set<string>(await getCollectionNames(db));
  if (collectionNames.has(collection)) {
    await prune(db.collection(collection));
  }
}

/**
 * Clear all records from a collection matching a given filter
 */
export async function prune(collection: Collection<any>, filter: object = {}) {
  const op = collection.initializeUnorderedBulkOp();
  op.find(filter).delete();
  await op.execute();
}

/**
 * Clear all records from all collections
 */
export async function truncateAllCollections(db: Db) {
  for (const collection of await getCollectionNames(db)) {
    await prune(db.collection(collection));
  }
}

/**
 * Get a mongodb time range expression from a date range.
 */
export function matchTimeRange(
  fieldName: string,
  { start, end }: DateRange,
  exclusive: boolean = false,
) {
  const startTime = start.valueOf();
  const endTime = end.valueOf();

  if (startTime > endTime) {
    throw new Error(
      "Invalid time range. `start` date must be before `end` date.",
    );
  }

  // Perform an equality check when the same time is given.
  if (startTime === endTime) {
    let operation = exclusive ? "$ne" : "$eq";
    return {
      [fieldName]: {
        [operation]: startTime,
      },
    };
  }

  // Invert the search boundaries when excluding the date range.
  let startOperation = exclusive ? "$lt" : "$gte";
  let endOperation = exclusive ? "$gt" : "$lte";

  return {
    $and: [
      {
        [fieldName]: { [startOperation]: startTime },
      },
      {
        [fieldName]: { [endOperation]: endTime },
      },
    ],
  };
}

/**
 * Get a mongodb aggregation expression from a metric aggregation strategy.
 */
export function getAggregationStrategyExpression(
  aggregation: string,
  fieldReference: string,
): Record<string, any> {
  const strategy = match(aggregation, {
    sum: { $sum: fieldReference },
    min: { $min: fieldReference },
    max: { $max: fieldReference },
    // Mean & Avg are equivalent
    avg: { $avg: fieldReference },
    mean: { $avg: fieldReference },
    // Not implemented yet
    mode: undefined,
    median: undefined,
    range: undefined,
  });

  if (strategy) {
    return strategy;
  }

  throw new Error(
    `Missing aggregation strategy. "${aggregation}" is not implemented yet.`,
  );
}

export async function pipelineToCount(db: Db, cursor: Cursor): Promise<number> {
  // Extract details from the pending cursor
  // @ts-ignore: undocumented api
  const collectionName = cursor.operation.target;
  // @ts-ignore: undocumented api
  const pipeline = cursor.operation.pipeline;

  const [result] = await db
    .collection(collectionName)
    .aggregate([...pipeline, { $count: "total" }], { allowDiskUse: true })
    .toArray();

  const total = result?.total ?? 0;
  return total;
}

export function createBucketingStages({
  dateField,
  valueField,
  duration,
  aggregation,
}: {
  dateField: string;
  valueField: string;
  duration: Duration;
  aggregation: string;
}): Array<any> {
  return [
    {
      $addFields: {
        groupDate: {
          $multiply: [
            {
              $trunc: {
                $divide: [`$${dateField}`, duration.milliseconds()],
              },
            },
            duration.milliseconds(),
          ],
        },
      },
    },
    {
      $group: {
        _id: "$groupDate",
        values: { $push: `$${valueField}` },
      },
    },
    {
      $sort: {
        _id: 1,
      },
    },
    {
      $project: {
        _id: 0,
        [`${dateField}`]: "$_id",
        [`${valueField}`]: getAggregationStrategyExpression(
          aggregation,
          "$values",
        ),
      },
    },
  ];
}

/**
 * Add a custom ranking field based on a provided ranking order. This can be used
 * to implement custom sorting behaviour.
 *
 * @example
 * const stage = createSortRankFieldStage({
 *    field: "status",
 *    rankPriority: {
 *      enabled: 1,
 *      disabled: 2,
 *      banned: 3,
 *    },
 *    rankField: "statusRank",
 * })
 */
export function createSortRankFieldStage({
  field,
  rankPriority = {},
  rankField = "rank",
}: {
  /** The field name to rank */
  field: string;
  /** The mapping of field values to sort ranking */
  rankPriority: Record<any, number>;
  /** The name of the rank field to add, default = 'rank' */
  rankField?: string;
}) {
  // Create a list of mongodb switch branches to match each field value
  // undeclared values will have the lowest priority
  const fieldRef = "$" + field;
  const branches = Object.entries(rankPriority).map(([matchValue, weight]) => {
    return {
      case: { $eq: [fieldRef, matchValue] as const },
      then: weight,
    };
  });

  return [
    {
      $addFields: {
        [rankField]: {
          $switch: {
            branches,
            default: branches.length + 1,
          },
        },
      },
    },
  ] as const;
}

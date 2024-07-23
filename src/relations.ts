import type { FilterQuery } from "mongodb";

type Document = Record<string, any>;

type HasOneRelation<RelatedDocument extends Document = {}> = {
  /** The field to join from on the current collection */
  localField: string;
  /** The field to join to on the remote collection */
  foreignField: string;
  /** The name of the remote collection*/
  from: string;
  /** Optional additional match constraints to apply when finding relations */
  match?: FilterQuery<RelatedDocument>;
};

type HasManyRelation<RelatedDocument extends Document = {}> = {
  /** The field to join from on the current collection */
  localField: string;
  /** The field to join to on the remote collection */
  foreignField: string;
  /** The name of the remote collection*/
  from: string;
  /** Optional additional match constraints to apply when finding relations */
  match?: FilterQuery<RelatedDocument>;
};

/**
 * Create a pipeline stage to add a "has one" relation to a mongodb query
 *
 * @example
 * db.collection("blog_posts").aggregate([
 *   ...hasOneRelation("author", {
 *    localField: "userId",
 *    foreignField: "id",
 *    from: "users",
 *   })
 * ])
 */
export function hasOneRelation<T extends Document = {}>(
  name: string,
  relationship: HasOneRelation<T>,
) {
  // TODO: Hypothesis. This doesn't support instances where the local or foreign fields are arrays
  return [
    ...hasManyRelation(name, relationship),
    {
      $addFields: {
        [name]: {
          $arrayElemAt: ["$" + name, 0],
        },
      },
    },
  ];
}

/**
 * Create a pipeline stage to add a "has many" relation to a mongodb query
 *
 * @example
 * db.collection("blog_posts").aggregate([
 *   ...hasManyRelation("comments", {
 *    localField: "id",
 *    foreignField: "postId",
 *    from: "comments",
 *    // Optional, limit comments to those created within the last 7 days
 *    match: {
 *       createdAt: {
 *           $gt: subDays(new Date(), 7)
 *       }
 *    }
 *   })
 * ])
 */
export function hasManyRelation<T extends Document = {}>(
  name: string,
  relationship: HasManyRelation<T>,
): object[] {
  const { localField, foreignField, from, match } = relationship;

  if (typeof match !== "undefined") {
    const pkRef = "$$pk"; // The double $ is important
    const fkRef = "$" + foreignField;
    return [
      {
        $lookup: {
          as: name,
          from,
          // Add the primary key to the pipeline scope
          let: { pk: "$" + localField },
          pipeline: [
            {
              $match: {
                // Only match documents where the foreign key matches
                $expr: { $eq: [fkRef, pkRef] },
                ...match,
              },
            },
          ],
        },
      },
    ];
  }

  // Use standard lookup when no additional match constraints are given
  return [
    {
      $lookup: {
        as: name,
        from,
        localField,
        foreignField,
      },
    },
  ];
}

// TODO: Add function for the remaining relationship types
// - belongsTo
// - belongsToMany

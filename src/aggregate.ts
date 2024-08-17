import {
  AggregationCursor as BaseAggregationCursor,
  Collection,
  Db,
} from "mongodb";
import { Document, HasManyRelation, HasOneRelation } from "./types";
import { AggregateError } from "./errors";

type Default = any;

// TODO:
// 1/ Instead of extending aggregation cursor extend AbstractCursor and
// re-implement the standard methods.
//
// 2/ Use a pipeline builder to produce the underlying query pipeline
//
export class AggregationCursor<T = Default> extends BaseAggregationCursor<T> {
  static collection = "";

  static query<T extends Default, B extends typeof AggregationCursor<T>>(
    this: B,
    db: Db,
  ): InstanceType<B> {
    if (this.collection === "") {
      throw new AggregateError("Must define collection name");
    }
    return this.make(db.collection(this.collection));
  }
  /**
   * Create a new instance of an AggregationCursor
   */
  public static make<
    T extends Default,
    Base extends typeof AggregationCursor<T>,
  >(this: Base, collection: Collection): InstanceType<Base> {
    // @ts-ignore
    return new this(
      // @ts-ignore
      collection.client,
      // @ts-ignore
      collection.s.namespace,
      [],
      // @ts-ignore
      // resolveOptions(collection, {}),
    );
  }

  withOne<T extends Document = {}>(
    name: string,
    relationship: HasOneRelation<T>,
  ) {
    const { localField, foreignField, from } = relationship;

    // We always need to use the let + pipeline approach because we always want the limit
    const pkRef = "$$pk";
    const fkRef = "$" + foreignField;

    return this.lookup({
      as: name,
      from,
      // Add the primary key to the pipeline scope
      let: { pk: "$" + localField },
      pipeline: [
        {
          $match: {
            // Only match documents where the foreign key matches
            $expr: { $eq: [fkRef, pkRef] },
          },
        },
        {
          $limit: 1,
        },
      ],
    }).unwind({ path: `$${name}`, preserveNullAndEmptyArrays: true });
  }

  withMany<T extends Document = {}>(
    name: string,
    relationship: HasManyRelation<T>,
  ) {
    const { localField, foreignField, from } = relationship;
    return this.lookup({
      as: name,
      from,
      localField,
      foreignField,
    });
  }

  async findOne() {
    const [result] = await this.limit(1).toArray();
    return result;
  }

  async findOneOrFail() {
    const result = await this.findOne();
    if (!result) {
      throw new AggregateError(`Document not found`);
    }
    return result;
  }

  async findMany() {
    return await this.toArray();
  }
}

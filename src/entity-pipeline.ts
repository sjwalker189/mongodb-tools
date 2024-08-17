import {
  AbstractCursor,
  AggregateOptions,
  AggregationCursor,
  Collection,
  Db,
  Filter,
  MongoClient,
  MongoDBNamespace,
  TopologyDescriptionChangedEvent,
} from "mongodb";
import { AggregateError } from "./errors";

type AnySchema = Record<string, any>;

export class EntityPipeline<TSchema extends AnySchema> {
  static create<
    T extends Record<string, any>,
    B extends typeof EntityPipeline<T>,
  >(this: B): InstanceType<B> {
    // @ts-ignore
    return this();
  }

  protected pipeline: object[] = [];

  protected addStage(doc: object) {
    this.pipeline.push(doc);
    return this;
  }

  toArray() {
    return this.pipeline;
  }

  match($match: Filter<TSchema>) {
    return this.addStage({ $match });
  }

  limit($limit: number) {
    return this.addStage({ $limit });
  }

  lookup<
    RSchema extends Record<string, any>,
    RKey extends keyof RSchema,
  >($lookup: {
    from: string;
    as: string;
    localField: keyof TSchema;
    foreignField: RKey;
    pipeline?: EntityPipeline<RSchema>;
  }): EntityPipeline<TSchema & Record<RKey, Array<RSchema>>> {
    // @ts-ignore
    return this.addStage({ $lookup });
  }

  hasOne<P extends EntityPipeline<AnySchema>>(
    relation: string,
    opts: {
      from: string;
      localField: string;
      foreignField: string;
      pipeline?: P;
    },
  ) {
    const lookupPipeline = opts.pipeline?.toArray() ?? [];
    return this.addStage({
      $lookup: {
        as: relation,
        from: opts.from,
        let: { pk: `$${opts.localField}` },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ["$$pk", `$${opts.foreignField}`],
              },
            },
          },
          ...lookupPipeline,
        ],
      },
    }).addStage({
      $setFields: {
        [relation]: { $arrayElemAt: [`$${relation}`, 0] },
      },
    });
  }
}

export class EntityCursor<TSchema extends AnySchema> extends AggregationCursor {
  static collectionName = "";
  static query<T extends AnySchema, B extends typeof EntityCursor<T>>(
    this: B,
    db: Db,
  ): InstanceType<B> {
    if (this.collectionName === "") {
      throw new AggregateError("Must define collection name");
    }
    return this.make(db.collection(this.collectionName));
  }
  /**
   * Create a new instance of an AggregationCursor
   */
  public static make<T extends AnySchema, Base extends typeof EntityCursor<T>>(
    this: Base,
    collection: Collection,
  ): InstanceType<Base> {
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

  declare match: ($match: Filter<TSchema>) => this;

  limit($limit: number) {
    return this.addStage({ $limit });
  }

  hasOne<P extends EntityPipeline<AnySchema>>(
    relation: string,
    opts: {
      from: string;
      localField: string;
      foreignField: string;
      pipeline?: P;
    },
  ) {
    const lookupPipeline = opts.pipeline?.toArray() ?? [];
    return this.addStage({
      $lookup: {
        as: relation,
        from: opts.from,
        let: { pk: `$${opts.localField}` },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ["$$pk", `$${opts.foreignField}`],
              },
            },
          },
          ...lookupPipeline,
          { $limit: 1 },
        ],
      },
    }).addStage({
      $addFields: {
        [relation]: { $arrayElemAt: [`$${relation}`, 0] },
      },
    });
  }

  hasMany<P extends EntityPipeline<AnySchema>>(
    relation: string,
    opts: {
      from: string;
      localField: string;
      foreignField: string;
      pipeline?: P;
    },
  ) {
    const lookupPipeline = opts.pipeline?.toArray() ?? [];
    return this.addStage({
      $lookup: {
        as: relation,
        from: opts.from,
        let: { pk: `$${opts.localField}` },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ["$$pk", `$${opts.foreignField}`],
              },
            },
          },
          ...lookupPipeline,
        ],
      },
    });
  }

  async findOne(): Promise<undefined | TSchema> {
    this.limit(1);
    const results = await this.toArray();
    return results[0];
  }
}

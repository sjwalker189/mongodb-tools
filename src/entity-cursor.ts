import {
  AbstractCursor,
  AggregateOptions,
  Filter,
  MongoClient,
  MongoCursorInUseError,
  MongoDBNamespace,
} from "mongodb";

export class EntityCursor<
  TSchema extends Record<string, any> = {},
> extends AbstractCursor<TSchema> {
  public readonly pipeline: Document[];

  /** @internal */
  private aggregateOptions: AggregateOptions;

  initialized = false;

  /** @internal */
  constructor(
    client: MongoClient,
    namespace: MongoDBNamespace,
    pipeline: Document[] = [],
    options: AggregateOptions = {},
  ) {
    // @ts-ignore
    super(client, namespace, options);

    this.pipeline = pipeline;
    this.aggregateOptions = options;
  }

  /** @internal */
  protected throwIfInitialized() {
    if (this.initialized) throw new MongoCursorInUseError();
  }

  addStage(stage: any): this {
    this.throwIfInitialized();
    this.pipeline.push(stage);
    return this;
  }

  match($match: Filter<TSchema>) {
    return this.addStage({ $match });
  }

  // @ts-ignore
  clone() {
    return undefined;
  }
}

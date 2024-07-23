import path from "node:path";
import fs from "node:fs";
import { MongoMemoryReplSet, MongoMemoryServer } from "mongodb-memory-server";
import { MongoClient, Db, MongoClientOptions } from "mongodb";

function onTerminate() {
  const dataPath = path.resolve(__dirname, "mongodb/data");
  if (fs.pathExistsSync(dataPath)) {
    fs.removeSync(dataPath);
  }
}

// Ensure that we clean up after ourselves
process.on("exit", onTerminate);
process.on("uncaughtException", onTerminate);

const dbName = "testing";
const databaseOptions: MongoClientOptions = {
  poolSize: 100,
  socketTimeoutMS: 60000,
  useUnifiedTopology: true,
};

export interface DatabaseContext {
  id: string;
  changeStreams: boolean;
  server: MongoMemoryServer | MongoMemoryReplSet;
  client: MongoClient;
  db: Db;
  collection: CollectionGetter;
  locked: boolean;
  isLocked(): boolean;
  refresh(): Promise<void>;
  lock(): Promise<void>;
  unlock(): Promise<void>;
}

// This is to share databaseContexts when working in watch mode
// @ts-ignore
const databaseContexts =
  process.databaseContexts ?? new Map<string, DatabaseContext>();
// @ts-ignore
process.databaseContexts = databaseContexts;

function getFirstFreeId(id: number) {
  while (true) {
    const databaseContextId = `${process.pid}-${id}`;
    if (databaseContexts.has(databaseContextId)) {
      id++;
      continue;
    }
    return databaseContextId;
  }
}

/**
 * Handles instance timeout not working until this lands:
 * https://github.com/nodkz/mongodb-memory-server/pull/716
 */
class CustomMongoMemoryReplSet extends MongoMemoryReplSet {
  // @ts-ignore
  getInstanceOpts(baseOpts = {}, keyFileLocation) {
    const opts = super.getInstanceOpts(baseOpts, keyFileLocation);
    // @ts-ignore
    if (baseOpts.launchTimeout) {
      // @ts-ignore
      opts.launchTimeout = baseOpts.launchTimeout;
    }
    return opts;
  }
}

async function createDatabaseContext(
  changeStreams: boolean = false,
): Promise<DatabaseContext> {
  const id = getFirstFreeId(0);
  const dbPath = path.resolve(__dirname, "mongodb/data", id);
  await fs.ensureDir(dbPath);
  const instance = {
    dbName,
    dbPath,
    launchTimeout: 10_000_000,
  };
  const binary = {
    version: "4.2.1",
  };
  const server = changeStreams
    ? await CustomMongoMemoryReplSet.create({
        instanceOpts: [instance],
        replSet: {
          count: 1,
          storageEngine: "wiredTiger",
        },
        binary,
      })
    : await MongoMemoryServer.create({
        instance,
        binary,
      });

  const client = new MongoClient(server.getUri(), databaseOptions);
  await client.connect();
  const db = client.db(dbName);

  // @ts-ignore: polyfill method which exists on later versions of mongodb
  db.getClient = () => client;

  const item: DatabaseContext = {
    id,
    changeStreams,
    server,
    client,
    db,
    locked: true,
    isLocked() {
      return item.locked;
    },
    async lock() {
      if (item.isLocked()) {
        throw new Error(`DatabaseContext of id '${item.id}' is already locked`);
      }
      item.locked = true;
    },
    async unlock() {
      if (!item.isLocked()) {
        throw new Error(
          `DatabaseContext of id '${item.id}' is already unlocked`,
        );
      }
      item.locked = false;
    },
    async refresh() {
      await dropAllCollections(item.db);
    },
  };

  return item;
}

export async function getDatabaseContext(
  changeStreams: boolean = false,
): Promise<DatabaseContext> {
  for (const databaseContext of databaseContexts.values()) {
    if (
      changeStreams === databaseContext.changeStreams &&
      !databaseContext.isLocked()
    ) {
      databaseContext.lock();
      return databaseContext;
    }
  }

  const item = await createDatabaseContext(changeStreams);
  databaseContexts.set(item.id, item);

  return item;
}

export type DatabaseContextRef = { value: DatabaseContext };

const HOOK_TIMEOUT = 60_000;

export function useDatabase(
  changeStreams: boolean = false,
): DatabaseContextRef {
  const ref: DatabaseContextRef = {} as any;

  before(async function () {
    this.timeout(HOOK_TIMEOUT);
    ref.value = await getDatabaseContext(changeStreams);
  });

  after(async function () {
    this.timeout(HOOK_TIMEOUT);
    if (ref.value) {
      ref.value.unlock();
    }
  });

  afterEach(async function () {
    this.timeout(HOOK_TIMEOUT);
    if (ref.value) {
      await ref.value.refresh();
    }
  });

  return ref;
}

export async function sleep(waitTime: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, waitTime));
}

export async function toArray<T>(
  generator: AsyncGenerator<T>,
): Promise<Array<T>> {
  const values: Array<T> = [];
  for await (const value of generator) {
    values.push(value);
  }
  return values;
}

export async function changeStreamClose({
  stream,
}: {
  stream: AutoResumingChangeStream<any>;
}): Promise<void> {
  return stream.close();
}

export async function changeStreamsClose(
  streams: Array<{ stream: AutoResumingChangeStream<any> }>,
): Promise<void[]> {
  return Promise.all(streams.map(changeStreamClose));
}

export async function changeStreamReady({
  stream,
}: {
  stream: AutoResumingChangeStream<any>;
}): Promise<void> {
  return new Promise((resolve) => {
    const intervalId = setInterval(checkStreamReady, 1);

    function checkStreamReady() {
      // @ts-ignore
      if (stream.changeStream.cursor.cursorState.cursorId) {
        clearInterval(intervalId);
        return resolve(undefined);
      }
    }
  });
}

export async function changeStreamsReady(
  streams: Array<{ stream: AutoResumingChangeStream<any> }>,
): Promise<void[]> {
  return Promise.all(streams.map(changeStreamReady));
}

export function getStreamChangePromise(
  streams: Array<MaterializedViewChangeStream>,
  operationType: OperationType,
  collection?: string,
): Promise<void> {
  return (
    streams
      .find(({ collection: coll, operationType: opType }) => {
        return (
          opType === operationType &&
          (collection === undefined || coll === collection)
        );
      })
      ?.createOnChange() ?? Promise.resolve(undefined)
  );
}

export function getStreamInsertPromise(
  streams: Array<MaterializedViewChangeStream>,
  collection?: string,
): Promise<void> {
  return getStreamChangePromise(streams, "insert", collection);
}

export function getStreamReplacePromise(
  streams: Array<MaterializedViewChangeStream>,
  collection?: string,
): Promise<void> {
  return getStreamChangePromise(streams, "replace", collection);
}

export function getStreamUpdatePromise(
  streams: Array<MaterializedViewChangeStream>,
  collection?: string,
): Promise<void> {
  return getStreamChangePromise(streams, "update", collection);
}

export function getStreamDeletePromise(
  streams: Array<MaterializedViewChangeStream>,
  collection?: string,
): Promise<void> {
  return getStreamChangePromise(streams, "delete", collection);
}

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

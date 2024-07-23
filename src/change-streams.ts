import { ChangeStream, ResumeToken } from "mongodb";
import { EventEmitter } from "node:events";

const RETRY_DELAY = 1_000;

export type AutoResumingChangeStreamCreator<T extends { [key: string]: any }> =
  (resumeToken?: ResumeToken) => ChangeStream<T>;

export type AutoResumingChangeStreamOptions = {
  resumeToken?: ResumeToken;
};

export default class AutoResumingChangeStream<
  T extends { [key: string]: any },
> extends EventEmitter {
  creator: AutoResumingChangeStreamCreator<T>;
  changeStream?: ChangeStream<T>;
  resumeToken?: ResumeToken;

  constructor(
    creator: AutoResumingChangeStreamCreator<T>,
    options?: AutoResumingChangeStreamOptions,
  ) {
    super();
    this.creator = creator;
    this.resumeToken = options?.resumeToken;
    this.setMaxListeners(0);
  }

  /**
   * Create a new change stream instance
   *
   * New change streams will be automatically created on error while listeners are still
   * registered against the current event emitter instance.
   */
  private _createChangeStream(resumeToken: ResumeToken | undefined) {
    let changeStream: ChangeStream<T> | undefined = this.creator(resumeToken);

    changeStream.on("change", (...args: any[]) => {
      this.emit("change", ...args);
    });

    changeStream.on(
      "error",
      // This must only run once as we'll be re-creating change stream instances
      // on each error received.
      once(async () => {
        if (changeStream && !changeStream.isClosed()) {
          await changeStream.close();
        }

        await sleep(RETRY_DELAY);

        if (this._hasListeners()) {
          try {
            this._createChangeStream(changeStream?.resumeToken);
          } catch (e) {
            this._createChangeStream(undefined);
          }
        }

        changeStream = undefined;
      }),
    );

    this.changeStream = changeStream;
  }

  on(event: "change", listener: (...args: any[]) => void): this {
    super.on(event, listener);

    if (!this.changeStream) {
      this._createChangeStream(this.resumeToken);
      // Set to undefined because we if we close due to there being no more change listeners
      // we shouldn't resume from the original resume token if another listener is added again
      // in the future
      this.resumeToken = undefined;
    }

    return this;
  }

  off(event: "change", listener: (...args: any[]) => void): this {
    super.off(event, listener);

    if (!this._hasListeners()) {
      this.close();
    }

    return this;
  }

  async close() {
    if (this.changeStream && !this.changeStream.isClosed()) {
      await this.changeStream.close();
    }
    this.changeStream = undefined;
  }

  private _hasListeners(): boolean {
    return this.listenerCount("change") > 0;
  }
}

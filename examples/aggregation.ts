import { MongoClient } from "mongodb";
import { EntityCursor } from "../src/entity-pipeline";

const client = new MongoClient("mongodb://localhost:27017/acmanage", {
  connectTimeoutMS: 1000,
  tls: false,
});
await client.connect();
const db = client.db("acmanage");

type User = {
  name: string;
  email: string;
  password: string;
  accountId: string;
  enabled: boolean;
};

type Team = {
  name: string;
  code: string;
  userId: string;
};

class UserCursor extends EntityCursor<User> {
  static collectionName = "users";

  enabled(isEnabled = true) {
    return this.match({ enabled: isEnabled });
  }

  withAccount() {
    return this.hasOne("account", {
      from: "accounts",
      localField: "accountId",
      foreignField: "userId",
    });
  }

  withRoles() {
    return this.hasMany("roles", {
      from: "user_roles",
      localField: "_id",
      foreignField: "userId",
    });
  }
}

// await db.collection("users").insertOne({
//   enabled: true,
//   email: "sam.walker@gmail.com",
//   name: "Sam",
//   password: "123456789",
// });

const user = await UserCursor.query(db)
  .enabled()
  .withAccount()
  .withRoles()
  .findOne();

console.log(user);
process.exit(0);

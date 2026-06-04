const replicaSet = {
  _id: "rs0",
  members: [{ _id: 0, host: "mongo:27017" }],
};

let ready = false;

try {
  const status = rs.status();
  ready = status.ok === 1;
} catch (error) {
  ready = false;
}

if (!ready) {
  rs.initiate(replicaSet);
}

for (let attempt = 0; attempt < 30; attempt += 1) {
  if (db.hello().isWritablePrimary) {
    ready = true;
    break;
  }
  sleep(1000);
}

if (!ready) {
  throw new Error("MongoDB replica set did not become primary");
}

const checkout = db.getSiblingDB("checkout");
checkout.orders.createIndex({ checkout_id: 1 }, { unique: true });
checkout.outbox_events.createIndex({ aggregateid: 1 });
checkout.outbox_events.createIndex({ aggregatetype: 1 });

print("MongoDB replica set and checkout indexes are ready");

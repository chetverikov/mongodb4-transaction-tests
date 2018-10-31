const assert = require('assert');
const { MongoClient } = require('mongodb');
const transfer = require('./transfer');

const uri = 'mongodb://localhost:27017,localhost:27018,localhost:27019/txn';
const options = { useNewUrlParser: true, replicaSet: 'rs' };

describe('Mongodb transaction', () => {
  let client = null;
  let db = null;

  before(async () => {
    client = await MongoClient.connect(uri, options);
    db = client.db();
  });

  beforeEach(() => db.dropDatabase());
  after(() => client.close());

  it('should transfer from A to B and C to D in parallel', async () => {
    await db.collection('Account').insertMany([
      { name: 'A', balance: 5 },
      { name: 'B', balance: 10 },
      { name: 'C', balance: 5 },
      { name: 'D', balance: 10 }
    ]);

    const session_first = client.startSession();
    await session_first.startTransaction();

    const session_second = client.startSession();
    await session_second.startTransaction();

    await transfer(db, session_first, 'A', 'B', 2);
    await transfer(db, session_second, 'C', 'D', 5);

    await session_first.commitTransaction();
    await session_second.commitTransaction();

    session_first.endSession();
    session_second.endSession();

    const accounts = await db.collection('Account').find({}).toArray();

    const expected = [
      ['A', 3],
      ['B', 12],
      ['C', 0],
      ['D', 15],
    ];

    for (const [index, val] of expected.entries()) {
      assert.equal(accounts[index].name, val[0]);
      assert.equal(accounts[index].balance, val[1]);
    }
  });

  it('should throw a write conflict exception when transfer from A to B and B to A in parallel', async () => {
    await db.collection('Account').insertMany([
      { name: 'A', balance: 5 },
      { name: 'B', balance: 10 }
    ]);

    const session_first = client.startSession();
    await session_first.startTransaction();

    const session_second = client.startSession();
    await session_second.startTransaction();

    try {
      await transfer(db, session_first, 'B', 'A', 2);
      await transfer(db, session_second, 'A', 'B', 5);

      await session_first.commitTransaction();
      await session_second.commitTransaction();

      throw new Error('WHY!?');
    } catch (err) {
      assert.equal(err.name, 'MongoError');
      assert.equal(err.errmsg, 'WriteConflict');
    } finally {
      session_first.endSession();
      session_second.endSession();
    }
  });

  it('should throw a write conflict exception when transfer from A to B and C to A in parallel', async () => {
    await db.collection('Account').insertMany([
      { name: 'A', balance: 5 },
      { name: 'B', balance: 10 },
      { name: 'C', balance: 8 },
    ]);

    const session_first = client.startSession();
    await session_first.startTransaction();

    const session_second = client.startSession();
    await session_second.startTransaction();

    try {
      await transfer(db, session_first, 'A', 'B', 2);
      await transfer(db, session_second, 'C', 'A', 7);

      await session_first.commitTransaction();
      await session_second.commitTransaction();

      throw new Error('WHY!?');
    } catch (err) {
      assert.equal(err.name, 'MongoError');
      assert.equal(err.errmsg, 'WriteConflict');
    } finally {
      session_first.endSession();
      session_second.endSession();
    }
  });

  it('should throw exception when transfer balance from A to B and from reserve C to A in parallel', async () => {
    await db.collection('Account').insertMany([
      { name: 'A', balance: 5, reserve: 10 },
      { name: 'B', balance: 10, reserve: 5 },
      { name: 'C', balance: 8, reserve: 9 },
    ]);

    const session_first = client.startSession();
    await session_first.startTransaction();

    const session_second = client.startSession();
    await session_second.startTransaction();

    try {
      await transfer(db, session_first, 'A', 'B', 2);
      await transfer(db, session_second, 'C', 'A', 7, 'reserve');

      await session_first.commitTransaction();
      await session_second.commitTransaction();

      throw new Error('WHY!?');
    } catch (err) {
      assert.equal(err.name, 'MongoError');
      assert.equal(err.errmsg, 'WriteConflict');
    } finally {
      session_first.endSession();
      session_second.endSession();
    }
  });

  it('should throw exception when transfer from A to B and C to A in parallel use different connections', async () => {
    const clientFirst = await MongoClient.connect(uri, options);
    const dbFirst = client.db();

    const clientSecond = await MongoClient.connect(uri, options);
    const dbSecond = client.db();

    await dbFirst.collection('Account').insertMany([
      { name: 'A', balance: 5 },
      { name: 'B', balance: 10 },
      { name: 'C', balance: 8 },
    ]);

    const session_first = clientFirst.startSession();
    await session_first.startTransaction();

    const session_second = clientSecond.startSession();
    await session_second.startTransaction();

    try {
      await transfer(dbFirst, session_first, 'A', 'B', 2);
      await transfer(dbSecond, session_second, 'C', 'A', 7);

      await session_first.commitTransaction();
      await session_second.commitTransaction();

      throw new Error('WHY!?');
    } catch (err) {
      assert.equal(err.name, 'MongoError');
      assert.equal(err.errmsg, 'WriteConflict');
    } finally {
      session_first.endSession();
      session_second.endSession();

      await clientFirst.close();
      await clientSecond.close();
    }
  })
});
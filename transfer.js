
module.exports = async function transfer(db, session, from, to, amount, field = 'balance') {
  const opts = { session, returnOriginal: false };
  const {value: A} = await db.collection('Account')
    .findOneAndUpdate({ name: from }, { $inc: { [field]: -amount } }, opts);

  if (A[field] < 0) {
    throw new Error('Insufficient funds: ' + (A[field] + amount));
  }

  const {value: B} = await db.collection('Account')
    .findOneAndUpdate({ name: to }, { $inc: { [field]: amount } }, opts);

  return { from: A, to: B };
};

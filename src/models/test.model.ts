import { PG_DATA_TYPE, DBModel, aggregateFn, fieldFn } from '../PgQueryBuilder';

export class BasketA extends DBModel {}

export class BasketB extends DBModel {}
export class BasketC extends DBModel {}
export class BasketD extends DBModel {}
export class BasketE extends DBModel {}

BasketA.init(
  {
    a: { type: PG_DATA_TYPE.int, isPrimary: true },
    fruit_a: { type: PG_DATA_TYPE.string(100), notNull: true },
  },
  { tableName: 'basket_a' },
);

BasketB.init(
  {
    b: { type: PG_DATA_TYPE.int, isPrimary: true },
    fruit_b: { type: PG_DATA_TYPE.string(100), notNull: true },
  },
  { tableName: 'basket_b' },
);

BasketC.init(
  {
    c: { type: PG_DATA_TYPE.int, isPrimary: true },
    fruit_c: { type: PG_DATA_TYPE.string(100), notNull: true },
  },
  { tableName: 'basket_c' },
);

BasketD.init(
  {
    d: { type: PG_DATA_TYPE.int, isPrimary: true },
    fruit_d: { type: PG_DATA_TYPE.string(100), notNull: true },
  },
  { tableName: 'basket_d' },
);

BasketE.init(
  {
    d: { type: PG_DATA_TYPE.int, isPrimary: true },
    e: { type: PG_DATA_TYPE.int, isPrimary: true },
    fruit_d: { type: PG_DATA_TYPE.string(100), notNull: true },
    fruit_e: { type: PG_DATA_TYPE.string(100), notNull: true },
  },
  { tableName: 'basket_e' },
);

// BasketA.createBulk(
//   ['a', 'fruit_a'],
//   [
//     [1, 'Apple'],
//     [2, 'Orange'],
//     [3, 'Banana'],
//     [4, 'Cucumber'],
//   ],
// );

// BasketA.create({ a: 6, fruit_a: 'Banana' }, { a: 'b' });

// BasketB.createBulk(
//   ['b', 'fruit_b'],
//   [
//     [1, 'Orange'],
//     [2, 'Apple'],
//     [3, 'Watermelon'],
//     [4, 'Pear'],
//   ],
// );

// BasketE.createBulk(
//   ['d', 'e', 'fruit_d', 'fruit_e'],
//   [
//     [1, 4, 'Apple', 'Orange'],
//     [2, 5, 'Orange', 'Watermelon'],
//     [3, 9, 'Watermelon', 'Banana'],
//     [4, 0, 'Pear', 'Cucumber'],
//   ],
// );

// BasketD.createBulk(
//   ['d', 'fruit_d'],
//   [
//     [1, 'Apple'],
//     [2, 'Orange'],
//     [3, 'Watermelon'],
//     [4, 'Pear'],
//   ],
// );

BasketA.findAll({
  // columns: ['a'],
  // columns: [[aggregateFn.COUNT('a'), 'b']],
  columns: [
    // 'a',
    fieldFn.abs(fieldFn.sub(aggregateFn.avg('a'), 5)),
    // [fieldFn.sub('a', { model: BasketB, column: aggregateFn.avg('b') }), 'av'],
    // fieldFn.power(fieldFn.val(5), fieldFn.col('a')),
    // [
    //   fieldFn.sub(
    //     { model: BasketB, column: aggregateFn.avg('b') },
    //     fieldFn.col('a'),
    //   ),
    //   'sub',
    // ],
    // fieldFn.abs(
    //   fieldFn.sub('a', {
    //     model: BasketB,
    //     column: aggregateFn.avg('b'),
    //   }),
    // ),
    // 'fruit_a',
    // [
    //   fieldFn.sqrt({
    //     model: BasketB,
    //     column: 'b',
    //     where: { b: { eq: 3 } },
    //   }),
    //   'sum',
    // ],
    // [
    //   fieldFn.add('a', {
    //     model: BasketB,
    //     column: 'b',
    //     where: { b: { eq: 3 } },
    //   }),
    //   'sum',
    // ],
  ],
  // columns: { a: 'b' },
  // where: {
  //   fruit_a: { iStartsWith: 'a' },
  // },
  // alias: {
  //   as: 'ac',
  //   query: {
  //     alias: { query: { model: BasketC }, as: 'fine' },
  //     where: { 'fine.c': { gt: 2 } },
  //   },
  // },
  where: {
    // fruit_a: 'a OR 1=1',
    // fruit_a: { notMatch: 5 },
    // 1: '1',
    // a: { between: [1, 3], gte: 1 },
    // where: { a: { gt: 1 } },
    // b: { gt: 1 },
    // a: {
    //   eq: {
    //     ANY: { model: BasketB, column: 'b' },
    //   },
    // },
    // in: { model: BasketB, column: 'b' },
    // },
    // a:{in:{}}
    // fruit_a: 'Apple',
    // $and: [
    //   {
    //     $exists: {
    //       model: BasketB,
    //       alias: 'b',
    //       where: { 'b.fruit_b': { iStartsWith: 'a' } },
    //     },
    //   },
    //   {
    //     $exists: {
    //       model: BasketB,
    //       alias: 'b',
    //       where: { 'b.fruit_b': { iStartsWith: 'o' } },
    //     },
    //   },
    // ],
    // a: {},
    // $or: [
    //   { fruit_a: { iStartsWith: 'c', iEndsWith: 'r' } },
    //   { fruit_a: { iStartsWith: 'a' } },
    // ],
    // fruit_a: { iStartsWith: 'a' },
    // $exists: {
    //   model: BasketB,
    //   alias: 'b',
    //   where: { 'b.fruit_b': { iStartsWith: 'x' } },
    // },
    // $exist:{tableName:'sf',where:{a:'5'}}
    // fruit_a: 'Apple',
    // a: 1,
  },
  //   where: {
  //     'basket_b.fruit_b': 'Orange',
  //   },
  // alias: 'fruit',
  // join: [
  //   {
  //     type: 'LEFT',
  //     model: BasketB,
  //     alias: 'basket_b',
  //     on: { fruit_a: 'basket_b.fruit_b', a: 'basket_b.b' },
  //     //   { model: BasketC, on: { 'basket_a.fruit_a': 'basket_c.fruit_c' } },
  //     // alias: 'basket_c',
  //     // // on: { 'basket_a.fruit_a': 'basket_c.fruit_a' },
  //   },
  //   {
  //     type: 'LEFT',
  //     model: BasketC,
  //     alias: 'basket_c',
  //     on: { fruit_a: 'basket_c.fruit_c' },
  //   },
  // ],
  // orderBy: {
  //   a: 'DESC',
  //   // a: { order: 'DESC' },

  //   // fruit_a: 'DESC',
  // },
  // groupBy: ['fruit_a', 'a'],
  // groupBy: ['a'],
  // having: {
  //   [aggregateFn.COUNT('a')]: { gt: 2 },
  // },
  // limit: 1,
  // offset: 1,
  // set: {
  //   type: 'EXCEPT',
  //   model: BasketB,
  //   // columns: { b: null },
  //   // where: { b: 1 },

  //   set: {
  //     type: 'UNION',
  //     model: BasketC,
  //     where: { c: 1 },
  //     // columns: { c: null },

  //     set: {
  //       type: 'UNION_ALL',
  //       model: BasketD,
  //       // where: { d: 1 },
  //       // columns: { d: null },
  //     },
  //   },
  // },
}).then((res) => {
  console.log(res);
});

BasketE.queryRawSql({
  columns: ['SIGN(d)'],
  // where: ['a & 1'],
}).then((res) => {
  console.log('raw Query Result->', res);
});

BasketA.queryRawSql(
  'SELECT AVg(a),ABS(Avg(a) -5) AS deviation FROM basket_a;',

  // 'SELECT a, ABS(a - t.avg_a) AS deviation FROM basket_a CROSS JOIN (SELECT AVG(a) AS avg_a FROM basket_a) As t;',
  // 'SELECT a,ABS(a-AVG(a))  FROM basket_a',
  // 'SELECT a,ABS(a - (SELECT AVG(ABS(b - AVG(b) OVER ())) FROM basket_b)) AS deviation FROM basket_a;',
  // 'SELECT a, ABS(a - (SELECT AVG(b) FROM basket_b)) AS deviation FROM basket_a;',
  // 'SELECT (SELECT c FROM basket_c where c=3 ) + (SELECT b FROM basket_b where b=2 ) AS sum FROM basket_a',
).then((res) => {
  console.log('raw Query Result->', res);
});

export function run() {
  console.log('test model running');
}

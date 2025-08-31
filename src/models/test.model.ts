import {
  PG_DATA_TYPE,
  DBModel,
  aggrFn,
  fieldFn,
  col,
  castFn,
} from '../PgQueryBuilder';

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
  // columns: [[aggrFn.COUNT('a'), 'b']],
  columns: [
    // aggrFn.count(col('*')),
    // fieldFn.lPad(castFn.text(col('fruit_a')), 10, '0')
    // fieldFn.abs(col('a')),
    fieldFn.now(),
    // fieldFn.age(fieldFn.now(), castFn.timestamp('2023-12-25')),
    fieldFn.datePart('month', fieldFn.now()),
    fieldFn.toNumber('123.454', '999.99'),
    fieldFn.currentTime(),
    fieldFn.typeOf(col('a')),

    fieldFn.least(col('a'), 0),
    // fieldFn.substring(col('fruit_a'), castFn.int(1), castFn.int(1)),
    // fieldFn.trim('A', col('fruit_a')),
    // fieldFn.position(col('fruit_a'), col('fruit_a')),
    // fieldFn.extractYear(castFn.timestamp('2023-12-25')),
    // castFn.timestamp(fieldFn.clockTimestamp(), { precision: 2 }),
    // fieldFn.sub(fieldFn.now(), castFn.timestamp('2023-12-25')),
    // fieldFn.concat(
    //   castFn.text('Mr'),
    //   col('fruit_a'),
    //   castFn.text(':'),
    //   col('fruit_a'),
    // ),
    // 'a',
    // aggrFn.avg(fieldFn.abs(fieldFn.sub(col('a'), 8))),
    // aggrFn.boolOr('a'),
    // aggrFn.avg('a', {
    //   isDistinct: true,
    // }),
    // 'a',
    // 'a',
    // 'fruit_a',
    // fieldFn.abs(castFn.int('2')),
    // [aggrFn.avg(fieldFn.power('a', 2)), 'd'],
    // [fieldFn.abs(fieldFn.sub('a', fieldFn.col('t.avg_a'))), 'deviation'],
    // fieldFn.abs(fieldFn.sub(5, aggrFn.avg('a'))),
    // fieldFn.abs(fieldFn.sub(5, fieldFn.col('a'))),
    // [fieldFn.sub('a', { model: BasketB, column: aggrFn.avg('b') }), 'av'],
    // fieldFn.power(fieldFn.val(5), fieldFn.col('a')),
    // [
    //   fieldFn.sub(
    //     { model: BasketB, column: aggrFn.avg('b') },
    //     fieldFn.col('a'),
    //   ),
    //   'sub',
    // ],
    // fieldFn.abs(
    //   fieldFn.sub('a', {
    //     model: BasketB,
    //     column: aggrFn.avg('b'),
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
    // a: { arrayContains: { model: BasketB, column: aggrFn.arrayAgg('b') } },
    // $matches: [[fieldFn.sub(col('a'), 2), { gt: 2 }]],
    // a: { arrayOverlap: [1, 2] },
    // fruit_a: { iLike: { ALL: ['a%', 'o%'] } },
    // fruit_a:{startsWith:}
    // a: { isTrue: null },
    // a: {
    //   between: [
    //     1,
    //     { model: BasketB, column: fieldFn.add(aggrFn.avg('b'), 0) },
    //   ],
    // },
    // $matches: [
    //   [
    //     fieldFn.abs(fieldFn.sub('a', fieldFn.col('t.avg_a'))),
    //     { gt: 2 },
    //     // { gt: { model } },
    //   ],
    //   // [fieldFn.upper(fieldFn.col('fruit_a')), { startsWith: 'O' }],
    // ],
    // a: { gte: 2 },
    // a: { gt: { model: BasketB, column: fieldFn.add(aggrFn.avg('b'), 0) } },
    // a: 2,
    // fruit_a: { startsWith: 'A' },
    // $or: [{ a: { gt: 2 } }, { '1': '1' }],
    // deviation: { gt: 2 },
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
    //   where: { 'b.fruit_b': { iStartsWith: 'a' } },
    // },
    // $exist:{tableName:'sf',where:{a:'5'}}
    // fruit_a: 'Apple',
    // a: 1,
  },
  // crossJoin: {
  //   model: BasketA,
  //   alias: 't',
  //   columns: [[aggrFn.avg('a'), 'avg_a']],
  // },
  // leftJoin: [
  //   {
  //     model: BasketB,
  //     alias: 'basket_b',
  //     on: { fruit_a: 'basket_b.fruit_b' },
  //   },
  //   {
  //     model: BasketC,
  //     alias: 'basket_c',
  //     on: { fruit_a: 'basket_c.fruit_c' },
  //   },
  // ],
  //   where: {
  //     'basket_b.fruit_b': 'Orange',
  //   },
  // alias: 'fruit',
  // orderBy: [
  //   aggrFn.avg('a'),
  //   // ['a', 'DESC'],
  //   // ['fruit_a', 'ASC'],
  // ],
  // groupBy: ['fruit_a', 'a'],
  // groupBy: ['a'],
  // having: {
  //   $matches: [
  //     [
  //       aggrFn.count(fieldFn.abs(fieldFn.sub(fieldFn.col('a'), 5))),
  //       { gt: 2 },
  //     ],
  //   ],
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
  console.dir(res, { depth: null });
});

// BasketE.queryRawSql({
//   columns: ['SIGN(d)'],
//   // where: ['a & 1'],
// }).then((res) => {
//   console.log('raw Query Result->', res);
// });

BasketA.queryRawSql(
  // "SELECT (NOW() - '2023-12-25'::TIMESTAMP) - INTERVAL '30 days'  AS deviation FROM basket_a;",
  // "SELECT * FROM basket_a WHERE fruit_a ILIKE ANY (ARRAY['a%','O%']::TEXT[])",
  'SELECT AVg(a),ABS(Avg(a) -5) AS deviation FROM basket_a;',
  // 'SELECT NOW() - (INTERVAL $1::DATE) FROM basket_a',
  // 'SELECT a, ABS(a - t.avg_a) AS deviation FROM basket_a CROSS JOIN (SELECT AVG(a) AS avg_a FROM basket_a) As t WHERE (ABS(a - t.avg_a)  > 2 );',
  // 'SELECT a,ABS(a-AVG(a))  FROM basket_a',
  // 'SELECT a,ABS(a - (SELECT AVG(ABS(b - AVG(b) OVER ())) FROM basket_b)) AS deviation FROM basket_a;',
  // 'SELECT a, ABS(a - (SELECT AVG(b) FROM basket_b)) AS deviation FROM basket_a;',
  // 'SELECT (SELECT c FROM basket_c where c=3 ) + (SELECT b FROM basket_b where b=2 ) AS sum FROM basket_a',
  // ['30 days'],
).then((res) => {
  console.dir({ 'raw Query Result->': res }, { depth: null });
});

export function run() {
  console.log('test model running');
}

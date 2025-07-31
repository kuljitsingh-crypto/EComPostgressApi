import { aggregateFn, DataTypes, DBModel } from './model.helpers';

export class BasketA extends DBModel {}

export class BasketB extends DBModel {}
export class BasketC extends DBModel {}
export class BasketD extends DBModel {}

BasketA.init(
  {
    a: { type: DataTypes.int, isPrimary: true },
    fruit_a: { type: DataTypes.string(100), notNull: true },
  },
  { tableName: 'basket_a' },
);

BasketB.init(
  {
    b: { type: DataTypes.int, isPrimary: true },
    fruit_b: { type: DataTypes.string(100), notNull: true },
  },
  { tableName: 'basket_b' },
);

BasketC.init(
  {
    c: { type: DataTypes.int, isPrimary: true },
    fruit_c: { type: DataTypes.string(100), notNull: true },
  },
  { tableName: 'basket_c' },
);

BasketD.init(
  {
    d: { type: DataTypes.int, isPrimary: true },
    fruit_d: { type: DataTypes.string(100), notNull: true },
  },
  { tableName: 'basket_d' },
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
  // columns: { a: null },
  // where: {
  //   fruit_a: { iStartsWith: 'a' },
  // },
  alias: { as: 'ac', model: BasketB },
  where: {
    // a: { between: [1, 3], gte: 1 },
    // where: { a: { gt: 1 } },
    b: { gt: 1 },
    // a: {
    // eq: {
    //   ANY: { model: BasketB, column: 'b' },
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
  //     type: 'INNER',
  //     model: BasketB,
  //     alias: 'basket_b',
  //     on: { 'fruit.fruit_a': 'basket_b.fruit_b', 'fruit.a': 'basket_b.b' },
  //     //   { model: BasketC, on: { 'basket_a.fruit_a': 'basket_c.fruit_c' } },
  //     // alias: 'basket_c',
  //     // // on: { 'basket_a.fruit_a': 'basket_c.fruit_a' },
  //   },
  //   {
  //     type: 'LEFT',
  //     model: BasketC,
  //     alias: 'basket_c',
  //     on: { 'fruit.fruit_a': 'basket_c.fruit_c' },
  //   },
  // ],
  // orderBy: {
  //   b: 'ASC',
  //   // a: { order: 'DESC' },

  //   // fruit_a: 'DESC',
  // },
  // groupBy: ['fruit_a', 'a'],
  // groupBy: ['a'],
  // having: {
  //   [aggregateFn.COUNT('a')]: { gt: 5 },
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

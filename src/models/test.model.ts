import { DataTypes, DBModel } from './model.helpers';

export class BasketA extends DBModel {}

export class BasketB extends DBModel {}
export class BasketC extends DBModel {}

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

// BasketA.createBulk(
//   ['a', 'fruit_a'],
//   [
//     [1, 'Apple'],
//     [2, 'Orange'],
//     [3, 'Banana'],
//     [4, 'Cucumber'],
//   ],
// );

// BasketB.createBulk(
//   ['b', 'fruit_b'],
//   [
//     [1, 'Orange'],
//     [2, 'Apple'],
//     [3, 'Watermelon'],
//     [4, 'Pear'],
//   ],
// );

// BasketC.createBulk(
//   ['c', 'fruit_c'],
//   [
//     [1, 'Apple'],
//     [2, 'Orange'],
//     [3, 'Watermelon'],
//     [4, 'Pear'],
//   ],
// );

BasketA.findAll({
  attributes: { a: { fn: 'count', as: 'b' } },
  //   where: {
  //     a: { between: [1, 3], gte: 1 },
  //     $or: [
  //       { fruit_a: { iStartsWith: 'c', iEndsWith: 'r' } },
  //       { fruit_a: { iStartsWith: 'a' } },
  //     ],
  //     fruit_a: { iStartsWith: 'a' },
  //     // fruit_a: 'Apple',
  //     // a: 1,
  //   },
  //   where: {
  //     'basket_b.fruit_b': 'Orange',
  //   },
  //   alias: 'fruit',
  //   include: {
  //     type: 'innerJoin',
  //     models: [
  //       {
  //         model: BasketB,
  //         alias: 'basket_b',
  //         on: { 'fruit.fruit_a': 'basket_b.fruit_b' },
  //       },
  //       //   { model: BasketC, on: { 'basket_a.fruit_a': 'basket_c.fruit_c' } },
  //     ],
  //     // alias: 'basket_c',
  //     // // on: { 'basket_a.fruit_a': 'basket_c.fruit_a' },
  //   },
  orderBy: {
    b: 'ASC',
    // a: { order: 'DESC' },

    // fruit_a: 'DESC',
  },
  groupBy: ['fruit_a', 'a'],
}).then((res) => {
  console.log(res);
});

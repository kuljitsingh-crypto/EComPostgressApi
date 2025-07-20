import { DataTypes, DBModel } from './model.helpers';

export class BasketA extends DBModel {}

export class BasketB extends DBModel {}

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

BasketA.findAll({
  attributes: ['a', 'fruit_a', 'b', 'fruit_b'],
  include: {
    type: 'innerJoin',
    models: [
      {
        model: BasketB,
        on: [
          { baseColumn: 'basket_a.fruit_a', joinColumn: 'basket_b.fruit_b' },
        ],
      },
    ],
  },
}).then((res) => {
  console.log(res);
});

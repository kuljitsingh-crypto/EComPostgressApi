import { PgDataType, DBModel, fn, RawQuery } from '../PgQueryBuilder';

export class BasketA extends DBModel {}
export class BasketB extends DBModel {}
export class BasketC extends DBModel {}
export class BasketD extends DBModel {}
export class BasketE extends DBModel {}
export class Company extends DBModel {}
export class Examples extends DBModel {}
export class Employees extends DBModel {}
export class Order extends DBModel {}

BasketA.init(
  {
    a: { type: PgDataType.int, isPrimary: true },
    fruit_a: { type: PgDataType.string(100), notNull: true },
  },
  { tableName: 'basket_a' },
);

BasketB.init(
  {
    b: { type: PgDataType.int, isPrimary: true },
    fruit_b: { type: PgDataType.string(100), notNull: true },
  },
  { tableName: 'basket_b' },
);

BasketC.init(
  {
    c: { type: PgDataType.int, isPrimary: true },
    fruit_c: { type: PgDataType.string(100), notNull: true },
  },
  { tableName: 'basket_c' },
);

BasketD.init(
  {
    d: { type: PgDataType.int, isPrimary: true },
    fruit_d: { type: PgDataType.string(100), notNull: true },
  },
  { tableName: 'basket_d' },
);

BasketE.init(
  {
    d: { type: PgDataType.int, isPrimary: true },
    e: { type: PgDataType.int, isPrimary: true },
    fruit_d: { type: PgDataType.string(100), notNull: true },
    fruit_e: { type: PgDataType.string(100), notNull: true },
  },
  { tableName: 'basket_e' },
);

Company.init(
  {
    id: { type: PgDataType.serial, isPrimary: true },
    name: { type: PgDataType.text, notNull: true },
    founded: { type: PgDataType.date },
    is_public: { type: PgDataType.boolean },
    metadata: { type: PgDataType.jsonb },
    employees: { type: PgDataType.jsonb },
    departments: { type: PgDataType.jsonb },
    finances: { type: PgDataType.jsonb },
  },
  { tableName: 'companies' },
);

Examples.init(
  {
    id: { type: PgDataType.serial, isPrimary: true },
    data: { type: PgDataType.jsonb },
  },
  { tableName: 'examples' },
);

Employees.init(
  {
    id: { type: PgDataType.serial, isPrimary: true },
    data: { type: PgDataType.jsonb },
  },
  { tableName: 'employees' },
);

Employees.init(
  {
    id: { type: PgDataType.serial, isPrimary: true },
    data: { type: PgDataType.jsonb },
  },
  { tableName: 'employees' },
);
Order.init(
  {
    id: { type: PgDataType.serial, isPrimary: true },
    info: { type: PgDataType.jsonb },
  },
  { tableName: 'orders' },
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

// Company.createBulk(
//   [
//     'name',
//     'founded',
//     'is_public',
//     'metadata',
//     'employees',
//     'departments',
//     'finances',
//   ],
//   [
//     [
//       'TechCorp',
//       '2010-05-12',
//       true,
//       `{
//     "hq": "Delhi",
//     "ceo": "Alice CEO",
//     "tags": ["IT","Analytics","AI"],
//     "ratings": { "glassdoor": 4.2, "indeed": 4.0 }
//   }`,
//       `[
//     {
//       "id": 101,
//       "name": "Alice",
//       "role": "Manager",
//       "salary": 95000,
//       "skills": ["Postgres","SQL","Leadership"],
//       "projects": [
//         {"name":"Ecommerce API","status":"completed","budget":50000},
//         {"name":"Analytics Dashboard","status":"ongoing","budget":75000}
//       ]
//     },
//     {
//       "id": 102,
//       "name": "Bob",
//       "role": "Developer",
//       "salary": null,
//       "skills": ["JavaScript","React","Node.js"],
//       "projects": [
//         {"name":"Mobile App","status":"ongoing","budget":40000},
//         {"name":"AI Chatbot","status":"planned","budget":60000}
//       ]
//     }
//   ]`,
//       `{
//     "IT": { "head": "Alice", "budget": 200000 },
//     "HR": { "head": "Carol", "budget": 80000 }
//   }`,
//       `{
//     "2023": { "revenue": 2000000, "profit": 450000 },
//     "2024": { "revenue": 3000000, "profit": 600000 }
//   }`,
//     ],
//     [
//       'InnoSoft',
//       '2015-07-20',
//       false,
//       `{
//     "hq": "Bangalore",
//     "ceo": "Ethan CEO",
//     "tags": ["Cloud","SaaS","DevOps"],
//     "ratings": { "glassdoor": 4.5, "indeed": 4.3 }
//   }`,
//       `[
//     {
//       "id": 201,
//       "name": "Carol",
//       "role": "Team Lead",
//       "salary": 85000,
//       "skills": ["Kubernetes","Docker","AWS"],
//       "projects": [
//         {"name":"Cloud Migration","status":"completed","budget":90000},
//         {"name":"CI/CD Pipeline","status":"ongoing","budget":30000}
//       ]
//     },
//     {
//       "id": 202,
//       "name": "David",
//       "role": "DevOps Engineer",
//       "salary": 70000,
//       "skills": ["Terraform","Linux","Monitoring"],
//       "projects": [
//         {"name":"Logging System","status":"ongoing","budget":20000}
//       ]
//     }
//   ]`,
//       `{
//     "Engineering": { "head": "Carol", "budget": 150000 },
//     "Support": { "head": "Ethan", "budget": 50000 }
//   }`,
//       `{
//     "2023": { "revenue": 1200000, "profit": 250000 },
//     "2024": { "revenue": 1800000, "profit": 400000 }
//   }`,
//     ],
//     [
//       'DataWorks',
//       '2018-03-15',
//       true,
//       `{
//     "hq": "Hyderabad",
//     "ceo": "Sophia CEO",
//     "tags": ["BigData","ETL","ML"],
//     "ratings": { "glassdoor": 4.1, "indeed": 3.9 }
//   }`,
//       `[
//     {
//       "id": 301,
//       "name": "Emma",
//       "role": "Data Scientist",
//       "salary": 120000,
//       "skills": ["Python","TensorFlow","ML"],
//       "projects": [
//         {"name":"Recommendation Engine","status":"completed","budget":100000},
//         {"name":"Fraud Detection","status":"ongoing","budget":150000}
//       ]
//     },
//     {
//       "id": 302,
//       "name": "Frank",
//       "role": "ETL Engineer",
//       "salary": 65000,
//       "skills": ["Spark","Hadoop","SQL"],
//       "projects": [
//         {"name":"Data Lake","status":"planned","budget":80000}
//       ]
//     }
//   ]`,
//       `{
//     "DataScience": { "head": "Emma", "budget": 180000 },
//     "ETL": { "head": "Frank", "budget": 100000 }
//   }`,
//       `{
//     "2023": { "revenue": 2500000, "profit": 500000 },
//     "2024": { "revenue": 4000000, "profit": 900000 }
//   }`,
//     ],
//   ],
// );

// Examples.createBulk(
//   ['data'],
//   [
//     [
//       fn.toJsonStr({
//         name: 'Alice',
//         age: 30,
//         skills: ['Python', 'SQL'],
//         address: { city: 'NYC', zip: '10001' },
//       }),
//     ],
//     [
//       fn.toJsonStr({
//         name: 'Bob',
//         age: 25,
//         hobbies: ['reading', 'gaming'],
//         active: true,
//       }),
//     ],
//     [
//       fn.toJsonStr({
//         products: [
//           { id: 1, name: 'Laptop' },
//           { id: 2, name: 'Mouse' },
//         ],
//       }),
//     ],
//   ],
// );

// BasketA.select({
//   // columns: ['a'],
//   // columns: [[fn.COUNT('a'), 'b']],
//   columns: [
//     fn.array([{ model: BasketB, column: fn.avg(fn.col('b')) }, 1, 2], {
//       type: PgDataType.double,
//     }),
//     // 'a',
//     // [
//     //   fn.window.rowNumber({
//     //     partitionBy: 'a',
//     //     // orderBy: ['a'],
//     //     frameOption: fn.rows('UNBOUNDED', 'UNBOUNDED'),
//     //   }),
//     //   'b',
//     // ],
//     // fn.count(fn.col('*')),
//     // fn.lPad(fn.cast.text(fn.col('fruit_a')), 10, '0'),
//     // fn.abs(fn.col('a')),
//     // fn.now(),
//     // fn.abs(
//     //   fn.avg(
//     //     fn.case(
//     //       {
//     //         when: { a: { gte: 1, lte: 2 } },
//     //         then: { model: BasketB, column: fn.avg(fn.col('b')) },
//     //       },
//     //       // { when: { a: 3 }, then: 0 },
//     //       { else: fn.multiple(fn.col('a'), -1) },
//     //     ),
//     //   ),
//     // ),
//     // fn.case(
//     //   {
//     //     when: { a: { gte: 1, lte: 2 } },
//     //     then: { model: BasketB, column: fn.avg(fn.col('b')) },
//     //   },
//     //   // { when: { a: 3 }, then: 0 },
//     //   { else: fn.multiple(fn.col('a'), -1) },
//     // ),
//     // 'fruit_a',
//     // fn.corr(fn.col('a'), {
//     //   model: BasketB,
//     //   column: 'b',
//     //   where: { 't.b': { eq: fn.col('a') } },
//     //   alias: 't',
//     // }),
//     // fn.sum(fn.cast.int(fn.case({ when: { a: 3 }, then: 1 }, { else: 0 }))),
//     // fn.age(fn.now(), fn.cast.timestamp('2023-12-25')),
//     // fn.datePart('month', fn.now()),
//     // fn.toNumber('123.454', '999.99'),
//     // fn.currentTime(),
//     // fn.typeOf(fn.col('a')),
//     // fn.case({when:6,then:5},{else:4})
//     // fn.least(fn.col('a'), 0),
//     // fn.substring(fn.col('fruit_a'), fn.cast.int(1), fn.cast.int(1)),
//     // fn.trim('A', fn.col('fruit_a')),
//     // fn.position(fn.col('fruit_a'), fn.col('fruit_a')),
//     // fn.extractYear(fn.cast.timestamp('2023-12-25')),
//     // fn.cast.timestamp(fn.clockTimestamp(), { precision: 2 }),
//     // fn.sub(fn.now(), fn.cast.timestamp('2023-12-25')),
//     // fn.concat(
//     //   fn.cast.text('Mr'),
//     //   fn.col('fruit_a'),
//     //   fn.cast.text(':'),
//     //   fn.col('fruit_a'),
//     // ),
//     // 'a',
//     // fn.avg(fn.abs(fn.sub(fn.col('a'), 8))),
//     // fn.boolOr('a'),
//     // fn.avg('a', {
//     //   isDistinct: true,
//     // }),
//     // 'a',
//     // 'a',
//     // 'fruit_a',
//     // fn.abs(fn.cast.int('2')),
//     // [fn.avg(fn.power('a', 2)), 'd'],
//     // [fn.abs(fn.sub('a', fn.col('t.avg_a'))), 'deviation'],
//     // fn.abs(fn.sub(5, fn.avg('a'))),
//     // fn.abs(fn.sub(5, fn.col('a'))),
//     // [fn.sub('a', { model: BasketB, column: fn.avg('b') }), 'av'],
//     // fn.power(fn.val(5), fn.col('a')),
//     // [
//     //   fn.sub(
//     //     { model: BasketB, column: fn.avg('b') },
//     //     fn.col('a'),
//     //   ),
//     //   'sub',
//     // ],
//     // fn.abs(
//     //   fn.sub('a', {
//     //     model: BasketB,
//     //     column: fn.avg('b'),
//     //   }),
//     // ),
//     // 'fruit_a',
//     // [
//     //   fn.sqrt({
//     //     model: BasketB,
//     //     column: 'b',
//     //     where: { b: { eq: 3 } },
//     //   }),
//     //   'sum',
//     // ],
//     // [
//     //   fn.add('a', {
//     //     model: BasketB,
//     //     column: 'b',
//     //     where: { b: { eq: 3 } },
//     //   }),
//     //   'sum',
//     // ],
//   ],
//   // columns: { a: 'b' },
//   // where: {
//   //   fruit_a: { iStartsWith: 'a' },
//   // },
//   // alias: {
//   //   as: 'ac',
//   //   query: {
//   //     alias: { query: { model: BasketC }, as: 'fine' },
//   //     where: { 'fine.c': { gt: 2 } },
//   //   },
//   // },
//   where: {
//     // $matches: [
//     //   fn.case(
//     //     {
//     //       when: { a: { gte: 1, lte: 2 } },
//     //       then: fn.cast.boolean(true),
//     //     },
//     //     // { when: { a: 3 }, then: 0 },
//     //     // { else: fn.multiple(fn.col('a'), -1) },
//     //   ),
//     // ],
//     // a: { gt: fn.sub(fn.cast.int(4), 2) },
//     // a: { jsonbContains: { model: BasketB, column: fn.arrayAgg('b') } },
//     // $matches: [[fn.sub(col('a'), 2), { gt: 2 }]],
//     // a: { arrayOverlap: [1, 2] },
//     // fruit_a: { iLike: { ALL: ['a%', 'o%'] } },
//     // fruit_a:{startsWith:}
//     // a: { isTrue: null },
//     // a: {
//     //   between: [
//     //     1,
//     //     { model: BasketB, column: fn.add(fn.avg('b'), 0) },
//     //   ],
//     // },
//     // $matches: [
//     //   [
//     //     fn.abs(fn.sub('a', fn.col('t.avg_a'))),
//     //     { gt: 2 },
//     //     // { gt: { model } },
//     //   ],
//     //   // [fn.upper(fn.col('fruit_a')), { startsWith: 'O' }],
//     // ],
//     // a: { gte: 2 },
//     // a: { gt: { model: BasketB, column: fn.add(fn.avg('b'), 0) } },
//     // a: 2,
//     // fruit_a: { startsWith: 'A' },
//     // $or: [{ a: { gt: 2 } }, { '1': '1' }],
//     // deviation: { gt: 2 },
//     // fruit_a: 'a OR 1=1',
//     // fruit_a: { notMatch: 5 },
//     // 1: '1',
//     // a: { between: [1, 3], gte: 1 },
//     // where: { a: { gt: 1 } },
//     // b: { gt: 1 },
//     // a: {
//     //   eq: {
//     //     ANY: { model: BasketB, column: 'b' },
//     //   },
//     // },
//     // in: { model: BasketB, column: 'b' },
//     // },
//     // a:{in:{}}
//     // fruit_a: 'Apple',
//     // $and: [
//     //   {
//     //     $exists: {
//     //       model: BasketB,
//     //       alias: 'b',
//     //       where: { 'b.fruit_b': { iStartsWith: 'a' } },
//     //     },
//     //   },
//     //   {
//     //     $exists: {
//     //       model: BasketB,
//     //       alias: 'b',
//     //       where: { 'b.fruit_b': { iStartsWith: 'o' } },
//     //     },
//     //   },
//     // ],
//     // a: {},
//     // $or: [
//     //   { fruit_a: { iStartsWith: 'c', iEndsWith: 'r' } },
//     //   { fruit_a: { iStartsWith: 'a' } },
//     // ],
//     // fruit_a: { iStartsWith: 'a' },
//     // $exists: {
//     //   model: BasketB,
//     //   alias: 'b',
//     //   where: { 'b.fruit_b': { iStartsWith: 'a' } },
//     // },
//     // $exist:{tableName:'sf',where:{a:'5'}}
//     // fruit_a: 'Apple',
//     // 't.a': 1,
//     // $exists: { subquery: { model: BasketB }, where: { b: fn.col('t.a') } },
//     // $exists: {
//     //   model: BasketB,
//     //   // model: {
//     //   //   model: BasketB,
//     //   //   union: { model: BasketC },
//     //   //   intersect: { model: BasketD },
//     //   // },
//     //   // alias: 'y',
//     //   where: { b: fn.col('a') },
//     //   union: { model: BasketC },
//     //   // intersect: { model: BasketD },
//     // },
//   },
//   alias: 't',
//   // crossJoin
//   // crossJoin: {
//   //   model: { model: BasketB, alias: 'y' },
//   //   alias: 't',
//   //   columns: [[fn.avg(fn.cast.int(fn.col('y.b'))), 'avg_a']],
//   //   modelAlias: 'y',
//   // },
//   // innerJoin: [
//   //   {
//   //     model: { model: BasketB, alias: 't', where: { b: { gt: 2 } } },
//   //     on: { a: 'b' },
//   //   },
//   //   {
//   //     model: { model: BasketC, alias: 't', where: { c: { gt: 1 } } },
//   //     on: { c: 'a' },
//   //   },
//   // ],
//   // leftJoin: [
//   //   {
//   //     model: BasketB,
//   //     alias: 'basket_b',
//   //     on: { fruit_a: 'basket_b.fruit_b' },
//   //   },
//   //   {
//   //     model: BasketC,
//   //     alias: 'basket_c',
//   //     on: { fruit_a: 'basket_c.fruit_c' },
//   //   },
//   // ],
//   //   where: {
//   //     'basket_b.fruit_b': 'Orange',
//   //   },
//   // alias: { query: { model: BasketB }, as: 't' },
//   // alias: 'fruit',
//   // orderBy: [
//   //   fn.avg('a'),
//   //   // ['a', 'DESC'],
//   //   // ['fruit_a', 'ASC'],
//   // ],
//   // orderBy: [['a', 'ASC']],
//   // orderBy: [
//   //   [
//   //     fn.case(
//   //       { when: { fruit_a: { iLike: ['A%', 'o%'] } }, then: 1 },
//   //       { else: 0 },
//   //     ),
//   //     'ASC',
//   //   ],
//   // ],
//   // groupBy: ['fruit_a', 'a'],
//   // groupBy: ['a'],
//   // groupBy: ['fruit_a'],
//   // having: {
//   //   $matches: [
//   //     [
//   //       fn.count(fn.abs(fn.sub(fn.col('a'), 5))),
//   //       { gt: 2 },
//   //     ],
//   //   ],
//   // },
//   // limit: 1,
//   // offset: 1,
//   // derivedModel: {
//   //   model: BasketA,
//   //   union: { model: BasketB },
//   //   unionAll: { model: BasketC },
//   // },
//   // union: {
//   //   model: BasketB,
//   //   intersect: { model: BasketD },
//   //   // where: { b: { gt: 1 } },
//   // },
//   // union: { model: BasketB },

//   // unionAll: { model: BasketC, where: { c: { gt: 2 } } },
//   // intersect: { model: BasketD },
// }).then((res) => {
//   console.dir(res, { depth: null });
// });

// Company.select({
//   // columns: [fn.col('x.metadata.tags', { asJson: true })],
//   columns: [
//     // fn.jsonbTypeOf(fn.col('metadata.ratings.indeed', { asJson: true })),
//     // fn.jsonbKeys(fn.col('metadata')),
//     // fn.jsonbEntries(fn.col('metadata')),
//     // fn.jsonbEntiresText(fn.col('metadata')),
//     // fn.jsonbArrayElements(fn.col('metadata.tags', { asJson: true })),
//     // fn.jsonbArrayElementsText(fn.col('metadata.tags', { asJson: true })),
//     // fn.jsonbArrayLength(fn.col('metadata.tags', { asJson: true })),
//     fn.jsonbBuildArray(
//       fn.col('metadata.tags', { asJson: true }),
//       fn.cast.int(1),
//       fn.cast.int(2),
//       fn.cast.text('f'),
//       fn.cast.text(null),
//       fn.cast.boolean(false),
//     ),
//     // fn.jsonBuildObject(fn.cast.text('name'), fn.cast.text('kuljit')),
//     // fn.jsonbTypeOf(fn.col('metadata.ratings.indeed', { asJson: true })),
//     // fn.jsonbAgg(fn.col('metadata.hq')),
//     // fn.jsonbObjectAgg(fn.col('metadata.hq'), fn.col('metadata.ceo')),
//     // fn.jPath(['metadata', 'tags']),
//     fn.jsonObject(
//       fn.jPath(['metadata', 'tags']),
//       fn.jPath(['metadata', 4]),
//     ),
//   ],
//   where: {
//     // 'metadata.ratings.indeed': { gt: 4 },
//     // $or: [
//     //   { [fn.cast.numeric(fn.col('x.metadata.ratings.indeed'))]: { gt: 4 } },
//     //   {
//     //     [fn.col('x.metadata.ratings.glassdoor')]: { gt: 4.5 },
//     //   },
//     // ],
//     // [fn.cast.text(fn.col('metadata.tags', { asJson: true }))]: {
//     //   in: ['IT'],
//     // },
//     // [fn.col('metadata.tags', { asJson: true })]: { jsonbContains: ['AI'] },
//     // metadata: { jsonbContains: { tags: ['AI', 'ML'] } },
//     // metadata: { jsonbHasAny: ['hq', 'ceo'] },
//   },
//   alias: 'x',
// }).then((res) => {
//   console.dir(res, { depth: null });
// });
// console.log(fn.cast.numeric('fdd'));
// BasketE.queryRawSql({
//   columns: ['SIGN(d)'],
//   // where: ['a & 1'],
// }).then((res) => {
//   console.log('raw Query Result->', res);
// });

// Examples.select({
//   columns: [
//     // fn.jsonbSet(fn.col('data'), fn.jPath(['age']), 35),
//     // fn.jsonbSet(
//     //   fn.col('data'),
//     //   fn.jPath(['address', 'country']),
//     //   fn.toJsonStr('india'),
//     //   true,
//     // ),
//     // fn.arrayToJson(
//     //   fn.array([fn.add(fn.cast.int(1), 32), 2, 3], { type: PgDataType.int }),
//     // ),
//     // fn.jsonConcat(fn.col('data'), fn.toJsonStr({ country: 'USA' })),
//     // fn.jsonbPathQuery(
//     //   fn.col('data'),
//     //   fn
//     //     .jQuery()
//     //     .start()
//     //     .key('skills')
//     //     .at(0)
//     //     .end()
//     //     .and()
//     //     .start()
//     //     .key('age')
//     //     .end()
//     //     .build(),
//     // ),
//   ],
//   // where: { id: 1 },
// }).then((res) => {
//   console.dir({ ' Query Result->': res }, { depth: null });
// });

Company.select({
  columns: [
    // fn.jsonbSet(fn.col('data'), fn.jPath(['age']), 35),
    // fn.jsonbSet(
    //   fn.col('data'),
    //   fn.jPath(['address', 'country']),
    //   fn.toJsonStr('india'),
    //   true,
    // ),
    // fn.arrayToJson(
    //   fn.array([fn.add(fn.cast.int(1), 32), 2, 3], { type: PgDataType.int }),
    // ),
    // fn.jsonConcat(fn.col('data'), fn.toJsonStr({ country: 'USA' })),
    // fn.jsonbPathQuery(
    //   fn.col('data'),
    //   fn
    //     .jQuery()
    //     .start()
    //     .key('skills')
    //     .at(0)
    //     .end()
    //     .and()
    //     .start()
    //     .key('age')
    //     .end()
    //     .build(),
    // ),
    // fn.jsonbPathQuery(fn.col('info'), fn.jQuery().s.build()),
    // fn.jsonbPathQueryArray(
    //   fn.col('data'),
    //   fn
    //     .jQuery()
    //     // .key('department')
    //     // .eq('IT')
    //     // .and()
    //     // .key('salary')
    //     // .gte(75000)
    //     // .grpStart(null)
    //     // .key('age')
    //     // .gt(30)
    //     // .grpEnd()
    //     // .or()
    //     // .grpStart(null)
    //     // .key('salary')
    //     // .lt(50000)
    //     // .grpEnd()
    //     .ctxStart('skills', '*')
    //     .likeRegex('^J')
    //     .ctxEnd()
    //     // .keyvalue()
    //     // .asKey()
    //     // .ctxStart('$')
    //     // .grpStart()
    //     // .key('salary')
    //     // .gt(65000)
    //     // .and()
    //     // .key('salary')
    //     // .lte(75000)
    //     // .grpEnd()
    //     // .or()
    //     // .key('age')
    //     // .eq(28)
    //     // .grpEnd()
    //     // .ctxEnd()
    //     // .or()
    //     // .ctxStart('age')
    //     // .eq(28)
    //     // .ctxEnd()
    //     // .grpEnd()
    //     // .not()
    //     // .grpStart(null)
    //     // .grpStart('salary')
    //     // .lt(75000)
    //     // .and()
    //     // .key('salary')
    //     // .gt(60000)
    //     // .grpEnd()
    //     // .or()
    //     // .grpStart('age')
    //     // .gt(20)
    //     // .grpEnd()
    //     // .grpEnd()
    //     .build(),
    // ),
    // fn.custom({ name: 'va', isCallableOp: false }, 2, 3),
    // fn.custom({ name: 'va', isCallableOp: false }, 2, 3),
    // fn.add(fn.cast.int(1), 2),
    // fn.custom({ name: 'NOT', callable: false }, fn.cast.boolean(true)),
    // fn.not(false),
    // fn.slice([1, 2, 3], 2, 3),
  ],
  // where: { id: 1 },
  where: {
    // data: {
    //   jsonbMatch: fn
    //     .jQuery()
    //     .not()
    //     .start()
    //     .key('age')
    //     .gt(25)
    //     .and()
    //     .key('age')
    //     .lt(35)
    //     .end()
    //     .build(),
    // jsonbMatch: fn
    //   .jQuery()
    //   .key('salary')
    //   .filterStart()
    //   .in([65000, 75000])
    //   .filterEnd()
    //   .build(),
    // jsonbMatch: fn.jQuery().key('skills').at(0).neq('Python').build(),
    // },
  },
}).then((res) => {
  console.dir({ ' Query Result->': res }, { depth: null });
});

RawQuery.query(
  'SELECT * FROM basket_a AS t WHERE EXISTS (SELECT 1 FROM ((SELECT * FROM basket_b UNION SELECT * FROM basket_c) INTERSECT SELECT * FROM basket_d) AS y WHERE (b = t.a))',
  // 'SELECT * FROM basket_a AS t WHERE EXISTS ((SELECT 1 FROM basket_b WHERE (b = t.a) UNION SELECT 1 FROM basket_c) INTERSECT SELECT 1 FROM basket_d)',
  // 'SELECT * FROM ((SELECT * FROM basket_a UNION SELECT * FROM basket_b) UNION ALL SELECT * FROM basket_c) AS t',
  // 'SELECT * FROM basket_a AS t WHERE EXISTS (SELECT 1 FROM basket_b WHERE (b = t.a) UNION SELECT 1 FROM basket_c)',
  // 'SELECT a,t.avg_a FROM basket_a CROSS JOIN (SELECT AVG(y.b::INTEGER) AS avg_a FROM (Select * from basket_b) as y) AS t',
  // "SELECT (NOW() - '2023-12-25'::TIMESTAMP) - INTERVAL '30 days'  AS deviation FROM basket_a;",
  // "SELECT * FROM basket_a WHERE fruit_a ILIKE ANY (ARRAY['a%','O%']::TEXT[])",
  // 'SELECT AVg(a),ABS(Avg(a) -5) AS deviation FROM basket_a;',
  // 'SELECT ABS((a>1) -5) AS deviation FROM basket_a;',
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

// SELECT final_sq.department_id, final_sq.avg_salary
// FROM (
//     SELECT mid_sq.department_id, AVG(mid_sq.salary) AS avg_salary
//     FROM (
//         SELECT e.department_id, e.salary
//         FROM employees e
//         WHERE e.salary > (
//             SELECT AVG(salary)
//             FROM employees
//         )
//     ) AS mid_sq
//     GROUP BY mid_sq.department_id
// ) AS final_sq
// WHERE final_sq.avg_salary > 50000;

//SELECT * FROM (((SELECT * FROM basket_a AS t UNION ALL (SELECT * FROM basket_c)) INTERSECT (SELECT * FROM basket_d)INTERSECT (SELECT * FROM basket_e))) AS results WHERE (a = $1)

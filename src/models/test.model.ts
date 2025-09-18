import {
  PgDataType,
  DBModel,
  aggrFn,
  fieldFn,
  col,
  castFn,
  windowFn,
  frameFn,
} from '../PgQueryBuilder';

export class BasketA extends DBModel {}
export class BasketB extends DBModel {}
export class BasketC extends DBModel {}
export class BasketD extends DBModel {}
export class BasketE extends DBModel {}
export class Company extends DBModel {}

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

// BasketA.findAll({
//   // columns: ['a'],
//   // columns: [[aggrFn.COUNT('a'), 'b']],
//   columns: [
//     // 'a',
//     // [
//     //   windowFn.rowNumber({
//     //     partitionBy: 'a',
//     //     // orderBy: ['a'],
//     //     // frameOption: frameFn.rows('UNBOUNDED', 'UNBOUNDED'),
//     //   }),
//     //   'b',
//     // ],
//     // aggrFn.count(col('*')),
//     // fieldFn.lPad(castFn.text(col('fruit_a')), 10, '0'),
//     // fieldFn.abs(col('a')),
//     // fieldFn.now(),
//     // fieldFn.abs(
//     //   aggrFn.avg(
//     //     fieldFn.case(
//     //       {
//     //         when: { a: { gte: 1, lte: 2 } },
//     //         then: { model: BasketB, column: aggrFn.avg(col('b')) },
//     //       },
//     //       // { when: { a: 3 }, then: 0 },
//     //       { else: fieldFn.multiple(col('a'), -1) },
//     //     ),
//     //   ),
//     // ),
//     // fieldFn.case(
//     //   {
//     //     when: { a: { gte: 1, lte: 2 } },
//     //     then: { model: BasketB, column: aggrFn.avg(col('b')) },
//     //   },
//     //   // { when: { a: 3 }, then: 0 },
//     //   { else: fieldFn.multiple(col('a'), -1) },
//     // ),
//     // 'fruit_a',
//     // aggrFn.corr(col('a'), {
//     //   model: BasketB,
//     //   column: 'b',
//     //   where: { 't.b': { eq: col('a') } },
//     //   alias: 't',
//     // }),
//     // aggrFn.sum(
//     //   castFn.int(
//     //     fieldFn.case(
//     //       { when: { fruit_a: { iLike: ['A%', 'o%'] } }, then: 1 },
//     //       { else: 0 },
//     //     ),
//     //   ),
//     // ),
//     // fieldFn.age(fieldFn.now(), castFn.timestamp('2023-12-25')),
//     // fieldFn.datePart('month', fieldFn.now()),
//     // fieldFn.toNumber('123.454', '999.99'),
//     // fieldFn.currentTime(),
//     // fieldFn.typeOf(col('a')),
//     // fieldFn.case({when:6,then:5},{else:4})
//     // fieldFn.least(col('a'), 0),
//     // fieldFn.substring(col('fruit_a'), castFn.int(1), castFn.int(1)),
//     // fieldFn.trim('A', col('fruit_a')),
//     // fieldFn.position(col('fruit_a'), col('fruit_a')),
//     // fieldFn.extractYear(castFn.timestamp('2023-12-25')),
//     // castFn.timestamp(fieldFn.clockTimestamp(), { precision: 2 }),
//     // fieldFn.sub(fieldFn.now(), castFn.timestamp('2023-12-25')),
//     // fieldFn.concat(
//     //   castFn.text('Mr'),
//     //   col('fruit_a'),
//     //   castFn.text(':'),
//     //   col('fruit_a'),
//     // ),
//     // 'a',
//     // aggrFn.avg(fieldFn.abs(fieldFn.sub(col('a'), 8))),
//     // aggrFn.boolOr('a'),
//     // aggrFn.avg('a', {
//     //   isDistinct: true,
//     // }),
//     // 'a',
//     // 'a',
//     // 'fruit_a',
//     // fieldFn.abs(castFn.int('2')),
//     // [aggrFn.avg(fieldFn.power('a', 2)), 'd'],
//     // [fieldFn.abs(fieldFn.sub('a', fieldFn.col('t.avg_a'))), 'deviation'],
//     // fieldFn.abs(fieldFn.sub(5, aggrFn.avg('a'))),
//     // fieldFn.abs(fieldFn.sub(5, fieldFn.col('a'))),
//     // [fieldFn.sub('a', { model: BasketB, column: aggrFn.avg('b') }), 'av'],
//     // fieldFn.power(fieldFn.val(5), fieldFn.col('a')),
//     // [
//     //   fieldFn.sub(
//     //     { model: BasketB, column: aggrFn.avg('b') },
//     //     fieldFn.col('a'),
//     //   ),
//     //   'sub',
//     // ],
//     // fieldFn.abs(
//     //   fieldFn.sub('a', {
//     //     model: BasketB,
//     //     column: aggrFn.avg('b'),
//     //   }),
//     // ),
//     // 'fruit_a',
//     // [
//     //   fieldFn.sqrt({
//     //     model: BasketB,
//     //     column: 'b',
//     //     where: { b: { eq: 3 } },
//     //   }),
//     //   'sum',
//     // ],
//     // [
//     //   fieldFn.add('a', {
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
//     //   fieldFn.case(
//     //     {
//     //       when: { a: { gte: 1, lte: 2 } },
//     //       then: castFn.boolean(true),
//     //     },
//     //     // { when: { a: 3 }, then: 0 },
//     //     // { else: fieldFn.multiple(col('a'), -1) },
//     //   ),
//     // ],
//     // a: { gt: fieldFn.sub(castFn.int(4), 2) },
//     // a: { arrayContains: { model: BasketB, column: aggrFn.arrayAgg('b') } },
//     // $matches: [[fieldFn.sub(col('a'), 2), { gt: 2 }]],
//     // a: { arrayOverlap: [1, 2] },
//     // fruit_a: { iLike: { ALL: ['a%', 'o%'] } },
//     // fruit_a:{startsWith:}
//     // a: { isTrue: null },
//     // a: {
//     //   between: [
//     //     1,
//     //     { model: BasketB, column: fieldFn.add(aggrFn.avg('b'), 0) },
//     //   ],
//     // },
//     // $matches: [
//     //   [
//     //     fieldFn.abs(fieldFn.sub('a', fieldFn.col('t.avg_a'))),
//     //     { gt: 2 },
//     //     // { gt: { model } },
//     //   ],
//     //   // [fieldFn.upper(fieldFn.col('fruit_a')), { startsWith: 'O' }],
//     // ],
//     // a: { gte: 2 },
//     // a: { gt: { model: BasketB, column: fieldFn.add(aggrFn.avg('b'), 0) } },
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
//     't.a': 1,
//     // $exists: { subquery: { model: BasketB }, where: { b: col('t.a') } },
//     // $exists: {
//     //   model: BasketB,
//     //   // model: {
//     //   //   model: BasketB,
//     //   //   union: { model: BasketC },
//     //   //   intersect: { model: BasketD },
//     //   // },
//     //   // alias: 'y',
//     //   where: { b: col('a') },
//     //   union: { model: BasketC },
//     //   // intersect: { model: BasketD },
//     // },
//   },
//   alias: 't',
//   // crossJoin
//   // crossJoin: {
//   //   model: { model: BasketB, alias: 'y' },
//   //   alias: 't',
//   //   columns: [[aggrFn.avg(castFn.int(col('y.b'))), 'avg_a']],
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
//   //   aggrFn.avg('a'),
//   //   // ['a', 'DESC'],
//   //   // ['fruit_a', 'ASC'],
//   // ],
//   // orderBy: [['a', 'ASC']],
//   // orderBy: [
//   //   [
//   //     fieldFn.case(
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
//   //       aggrFn.count(fieldFn.abs(fieldFn.sub(fieldFn.col('a'), 5))),
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

Company.findAll({
  // columns: [col('x.metadata.tags', { asJson: true })],
  columns: [
    fieldFn.jsonbTypeOf(col('metadata.ratings.indeed', { asJson: true })),
    fieldFn.jsonbKeys(col('metadata')),
    fieldFn.jsonbEntries(col('metadata')),
    fieldFn.jsonbEntiresText(col('metadata')),
    fieldFn.jsonbArrayElements(col('metadata.tags', { asJson: true })),
    fieldFn.jsonbArrayElementsText(col('metadata.tags', { asJson: true })),
    fieldFn.jsonbArrayLength(col('metadata.tags', { asJson: true })),
    fieldFn.jsonbBuildArray(
      col('metadata.tags', { asJson: true }),
      castFn.int(1),
      castFn.int(2),
      castFn.text('f'),
      castFn.text(null),
      castFn.boolean(false),
    ),
    fieldFn.jsonBuildObject(castFn.text('name'), castFn.text('kuljit')),
    // fieldFn.jsonbTypeOf(col('metadata.ratings.indeed', { asJson: true })),
  ],
  where: {
    // 'metadata.ratings.indeed': { gt: 4 },
    // $or: [
    //   { [castFn.numeric(col('x.metadata.ratings.indeed'))]: { gt: 4 } },
    //   {
    //     [col('x.metadata.ratings.glassdoor')]: { gt: 4.5 },
    //   },
    // ],
    // [castFn.text(col('metadata.tags', { asJson: true }))]: {
    //   in: ['IT'],
    // },
  },
  alias: 'x',
}).then((res) => {
  console.dir(res, { depth: null });
});
// console.log(castFn.numeric('fdd'));
// BasketE.queryRawSql({
//   columns: ['SIGN(d)'],
//   // where: ['a & 1'],
// }).then((res) => {
//   console.log('raw Query Result->', res);
// });

BasketA.queryRawSql(
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

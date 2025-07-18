import { DataTypes, dbDefaultValue, DBModel } from './model.helpers';
import { modelTypes, roleType } from './modelConstants';

class Role extends DBModel {}
Role.init(
  {
    id: {
      type: DataTypes.uuid,
      defaultValue: dbDefaultValue.uuidV4,
      isPrimary: true,
    },
    type: {
      type: DataTypes.string(255),
      customDefaultValue: modelTypes.role,
    },
    userRole: {
      type: DataTypes.enum(Object.values(roleType)),
    },
    roleId: {
      type: DataTypes.uuid,
    },
  },
  {
    modelName: 'role',
    references: [
      {
        parentColumn: 'id',
        parentModel: 'current_user',
        column: 'roleId',
        constraintName: 'fk_role',
        onDelete: 'CASCADE',
        onUpdate: 'CASCADE',
      },
    ],
  },
);
export default Role;

Role.findAll({
  // // attributes: ['userRole', { column: 'roleId', alias: 'rId' }],
  // filters: [
  //   { column: 'roleId', op: 'isNull' },
  //   // { op: 'between', column: 'userRole', value: ['admin', 'seller'] },
  //   {
  //     column: 'id',
  //     op: 'in',
  //     value: [
  //       '83fdcb3b-df81-4ffc-b98c-8d57679ada00',
  //       '645d1e52-ec00-44b0-837c-89317886e42d',
  //     ],
  //   },
  //   { column: 'type', op: 'eq', value: 'role' },
  //   {
  //     value1: { column: 'userRole', op: 'eq', value: 'admin' },
  //     op: 'or',
  //     value2: { column: 'userRole', op: 'eq', value: 'seller' },
  //   },
  // ],
}).then((res) => {
  console.log(res);
});

// Role.create(
//   {
//     userRole: roleType.admin,
//   },
//   ['userRole', { column: 'roleId', alias: 'rId' }],
// ).then((res) => {
//   console.log(res);
// });

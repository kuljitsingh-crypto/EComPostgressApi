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

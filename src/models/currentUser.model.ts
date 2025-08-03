import {
  DataTypes,
  dbDefaultValue,
  DBModel,
  DbTable,
} from '../PgQueryBuilder/model.helpers';
import { modelTypes } from './modelConstants';

class CurrentUser extends DBModel {}
CurrentUser.init(
  {
    id: {
      type: DataTypes.uuid,
      isPrimary: true,
      defaultValue: dbDefaultValue.uuidV4,
      notNull: true,
    },
    imageId: {
      type: DataTypes.uuid,
    },
    type: {
      type: DataTypes.string(255),
      customDefaultValue: modelTypes.currentUser,
    },
    firstName: { type: DataTypes.string(255) },
    lastName: { type: DataTypes.string(255) },
    displayName: { type: DataTypes.string(255) },
    password: { type: DataTypes.text },
    email: { type: DataTypes.string(500), notNull: true, unique: true },
    emailVerified: { type: DataTypes.boolean },
    banned: { type: DataTypes.boolean },
    bio: { type: DataTypes.text },
    createdAt: {
      type: DataTypes.timestamptz,
      defaultValue: dbDefaultValue.currentTimestamp,
    },
    metadata: { type: DataTypes.json },
  },
  {
    tableName: 'current_user',
    references: [
      {
        parentTable: 'image',
        parentColumn: 'id',
        column: 'imageId',
        onDelete: 'SET NULL',
      },
    ],
  },
);

export default CurrentUser;

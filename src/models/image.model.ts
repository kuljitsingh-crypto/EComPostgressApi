import { DataTypes, dbDefaultValue, DBModel } from './model.helpers';
import { modelTypes } from './modelConstants';

class Image extends DBModel {}

Image.init(
  {
    id: {
      type: DataTypes.uuid,
      defaultValue: dbDefaultValue.uuidV4,
      notNull: true,
      isPrimary: true,
    },
    type: {
      type: DataTypes.string(255),
      customDefaultValue: modelTypes.image,
    },
    url: {
      type: DataTypes.text,
      notNull: true,
    },
    alt: {
      type: DataTypes.string(100),
    },
  },
  { tableName: 'image' },
);

export default Image;

export type Primitive = string | number | boolean | null;

export type Table = {
  [key in string]: {
    type: string;
    isPrimary?: boolean;
    defaultValue?: string;
    unique?: boolean;
    notNull?: boolean;
    customDefaultValue?: string;
    check?: string;
  };
};

export type TableValues = Table[keyof Table];

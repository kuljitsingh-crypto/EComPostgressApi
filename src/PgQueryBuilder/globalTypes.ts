export type Primitive = string | number | boolean | null;

export type Table<T extends string = string> = {
  [key in T]: {
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

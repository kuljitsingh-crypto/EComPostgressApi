import { throwError } from './errorHelper';
import { isValidObject } from './helperFunction';

export function objToJson(obj: Record<string, unknown>): any {
  if (!isValidObject(obj)) {
    return throwError.invalidObjectOPType('objToJson');
  }
  return JSON.stringify(obj);
}

export function jsonToObj(jsonStr: string): any {
  try {
    return JSON.parse(jsonStr);
  } catch (err) {
    return throwError.invalidJsonStrType('jsonToObj');
  }
}

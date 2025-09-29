import { throwError } from './errorHelper';

export function toJsonStr(val: unknown): string {
  try {
    return JSON.stringify(val);
  } catch (_) {
    return throwError.invalidJsonType('fromJsonStr');
  }
}

export function fromJsonStr(jsonStr: string): any {
  try {
    return JSON.parse(jsonStr);
  } catch (err) {
    return throwError.invalidJsonType('fromJsonStr');
  }
}

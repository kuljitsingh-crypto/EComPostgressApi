import { CallableField } from '../internalTypes';

class SymbolFuncRegistry {
  static #instance: SymbolFuncRegistry | null = null;
  #symbolFuncRegister = new Map<Symbol, CallableField>();
  constructor() {
    if (SymbolFuncRegistry.#instance === null) {
      SymbolFuncRegistry.#instance = this;
    }
    return SymbolFuncRegistry.#instance;
  }
  get(symbol: Symbol) {
    return this.#symbolFuncRegister.get(symbol);
  }

  add(symbol: Symbol, method: CallableField) {
    this.#symbolFuncRegister.set(symbol, method);
  }
  delete(symbol: Symbol) {
    this.#symbolFuncRegister.delete(symbol);
  }
  has(symbol: Symbol) {
    return this.#symbolFuncRegister.has(symbol);
  }
}

export const symbolFuncRegister = new SymbolFuncRegistry();

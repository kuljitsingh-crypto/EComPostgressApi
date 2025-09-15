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
  getFrmRegistry(symbol: Symbol) {
    return this.#symbolFuncRegister.get(symbol);
  }

  addToRegistry(symbol: Symbol, method: CallableField) {
    this.#symbolFuncRegister.set(symbol, method);
  }
  deleteFrmRegistry(symbol: Symbol) {
    this.#symbolFuncRegister.delete(symbol);
  }
}

export const symbolFuncRegister = new SymbolFuncRegistry();

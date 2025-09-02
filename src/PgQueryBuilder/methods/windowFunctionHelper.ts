class FrameFunction {
  static #instance: FrameFunction | null = null;
  constructor() {
    if (FrameFunction.#instance === null) {
      FrameFunction.#instance = this;
      // Initialize other properties or methods here if needed
    }
    return FrameFunction.#instance;
  }
}

class WindowFunction {
  static #instance: WindowFunction | null = null;

  constructor() {
    if (WindowFunction.#instance === null) {
      WindowFunction.#instance = this;
      // Initialize other properties or methods here if needed
    }
    return WindowFunction.#instance;
  }
}

export const windowFn = new WindowFunction();
export const frameFn = new FrameFunction();

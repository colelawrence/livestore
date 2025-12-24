// Related https://github.com/facebook/hermes/issues/612#issuecomment-2549404649
const REACT_NATIVE_BAD_FUNCTION_STRING = 'function() { [bytecode] }'

export const isValidFunctionString = (
  fnStr: string,
): { _tag: 'valid' } | { _tag: 'invalid'; reason: 'react-native' } => {
  if (fnStr === REACT_NATIVE_BAD_FUNCTION_STRING) {
    return { _tag: 'invalid', reason: 'react-native' }
  }

  return { _tag: 'valid' }
}

/**
 * WeakMap-based function identity tracking.
 *
 * When `fn.toString()` returns useless bytecode (Hermes/React Native) and no explicit `deps`
 * are provided, we use object identity to deduplicate. The same function reference always
 * gets the same ID; different function references get different IDs.
 *
 * This means parameterized query factories will NOT deduplicate across calls unless explicit
 * `deps` are provided—each call creates a new function, so each gets a unique ID.
 */
const functionIdentityMap = new WeakMap<WeakKey, string>()
let functionIdentityCounter = 0

/**
 * Returns a stable unique identifier for a function based on object identity.
 * The same function reference always returns the same ID.
 */
export const getFunctionIdentity = (fn: (...args: any[]) => any): string => {
  let id = functionIdentityMap.get(fn)
  if (id === undefined) {
    id = `fn-id-${++functionIdentityCounter}`
    functionIdentityMap.set(fn, id)
  }
  return id
}

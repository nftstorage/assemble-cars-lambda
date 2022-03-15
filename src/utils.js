'use strict'

/**
 * @template T
 * @param {AsyncIterable<T>} iterable
 * @returns {Promise<Array<T>>}
 */
async function collect(iterable) {
  const chunks = []
  for await (const chunk of iterable) {
    chunks.push(chunk)
  }
  return chunks
}

/**
 * @param {AsyncIterable<Uint8Array>} iterable
 * @returns {Promise<Uint8Array>}
 */
async function collectBytes(iterable) {
  const chunks = await collect(iterable)
  return new Uint8Array([].concat(...chunks.map((c) => Array.from(c))))
}

module.exports = {
  collectBytes,
}

'use strict'

const { CarBlockIterator } = require('@ipld/car')
const { Block } = require('multiformats/block')
const raw = require('multiformats/codecs/raw')
const cbor = require('@ipld/dag-cbor')
const pb = require('@ipld/dag-pb')

const MAX_BLOCK_SIZE = 1 << 20 // Maximum permitted block size in bytes (1MiB).
const decoders = [pb, raw, cbor]

/**
 * Get CAR basic properties.
 *
 * @param {Uint8Array} carBytes
 */
async function carStat(carBytes) {
  const blocksIterator = await CarBlockIterator.fromBytes(carBytes)
  const roots = await blocksIterator.getRoots()

  if (roots.length === 0) {
    throw new Error('missing roots')
  }
  if (roots.length > 1) {
    throw new Error('too many roots')
  }

  const rootCid = roots[0]
  return { rootCid, blocksIterator }
}

/**
 * Inspect CAR to find out if it is valid and what is its structure.
 */
async function inspectCarBlocks(rootCid, blocksIterator) {
  let rawRootBlock
  let blocks = []
  for await (const block of blocksIterator) {
    const blockSize = block.bytes.byteLength
    if (blockSize > MAX_BLOCK_SIZE) {
      throw new Error(`block too big: ${blockSize} > ${MAX_BLOCK_SIZE}`)
    }
    if (!rawRootBlock && block.cid.equals(rootCid)) {
      rawRootBlock = block
    }
    blocks.push(block)
  }

  if (!blocks.length) {
    throw new Error('empty CAR')
  }
  if (!rawRootBlock) {
    throw new Error('missing root block')
  }

  const decoder = decoders.find((d) => d.code === rootCid.code)
  let structure, size
  if (decoder) {
    const rootBlock = new Block({
      cid: rawRootBlock.cid,
      bytes: rawRootBlock.bytes,
      value: decoder.decode(rawRootBlock.bytes),
    })

    // get the size of the full dag for this root, even if we only have a partial CAR.
    if (rootBlock.cid.code === pb.code) {
      size = cumulativeSize(rootBlock.bytes, rootBlock.value)
    }

    // if there's only 1 block (the root block) and it's a raw node, we know it is complete
    if (blocks.length === 1 && rootCid.code === raw.code) {
      structure = 'Complete'
    } else {
      // Iterate Dag links
      const isComplete = iterateDag(rawRootBlock, blocks)

      if (isComplete) {
        structure = 'Complete'
      } else {
        structure = 'Partial'
      }
    }
  }
  return { rootCid, structure, size }
}

/**
 * Iterate over a dag starting on the raw block and using the CAR blocks.
 * Returns whether the DAG is complete.
 */
function iterateDag(rawBlock, blocks) {
  const decoder = decoders.find((d) => d.code === rawBlock.cid.code)
  const rBlock = new Block({
    cid: rawBlock.cid,
    bytes: rawBlock.bytes,
    value: decoder.decode(rawBlock.bytes),
  })
  const bLinks = Array.from(rBlock.links())

  for (const link of bLinks) {
    const existingBlock = blocks.find((b) => b.cid.equals(link[1]))

    // Incomplete Dag
    if (!existingBlock) {
      return false
    }

    // Incomplete Dag
    if (!iterateDag(existingBlock, blocks)) {
      return false
    }
  }

  // Complete Dag
  return true
}

/**
 * The sum of the node size and size of each link
 * @param {Uint8Array} pbNodeBytes
 * @param {import('@ipld/dag-pb/src/interface').PBNode} pbNode
 * @returns {number} the size of the DAG in bytes
 */
function cumulativeSize(pbNodeBytes, pbNode) {
  // NOTE: Tsize is optional, but all ipfs implementations we know of set it.
  // It's metadata, that could be missing or deliberately set to an incorrect value.
  // This logic is the same as used by go/js-ipfs to display the cumulative size of a dag-pb dag.
  return pbNodeBytes.byteLength + pbNode.Links.reduce((acc, curr) => acc + (curr.Tsize || 0), 0)
}

module.exports = {
  carStat,
  inspectCarBlocks
}

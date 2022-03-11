const { toString } = require('uint8arrays')

const { Blob } = require('@web-std/blob')
const { sha256 } = require('multiformats/hashes/sha2')
const { TreewalkCarSplitter, TreewalkCarJoiner } = require('carbites')
const { CarReader } = require('@ipld/car')
const { pack } = require('ipfs-car/pack')
const { packToBlob } = require('ipfs-car/pack/blob')

const { collectBytes } = require('../../src/utils')

/**
 * @param {number} length
 * @param {Object} [packOptions]
 */
async function generateCar (length, packOptions = {}) {
  const { root, car } = await packToBlob({
    input: [{
      path: 'file.txt',
      content: generateUint8Array(length),
    }],
    wrapWithDirectory: false,
    ...packOptions
  })

  const carBuffer = new Uint8Array(await car.arrayBuffer())
  const multihash = await sha256.digest(carBuffer)

  return {
    root,
    car: carBuffer,
    key: `raw/${root.toString()}/${toString(multihash.bytes, 'base32')}.car`
  }
}

/**
 * @param {number} length
 * @param {number} targetSize
 * @param {Object} [packOptions]
 */
async function generateSplittedCar (length, targetSize, packOptions = {}) {
  const { root, out } = await pack({
    input: [{
      path: 'file.txt',
      content: generateUint8Array(length),
    }],
    wrapWithDirectory: true,
    ...packOptions
  })
  const car = await CarReader.fromIterable(out)
  const splitter = new TreewalkCarSplitter(car, targetSize)
  const carParts = []

  for await (const car of splitter.cars()) {
    const part = []
    for await (const block of car) {
      part.push(block)
    }
    carParts.push(part)
  }

  // Create Car files
  const carFiles = await Promise.all(carParts.map(async part => {
    const blob = new Blob(part, { type: 'application/car' })
    const car = new Uint8Array(await blob.arrayBuffer())
    const multihash = await sha256.digest(car)
    return {
      car,
      key: `raw/${root.toString()}/${toString(multihash.bytes, 'base32')}.car`,
      size: car.byteLength
    }
  }))

  // Create Join Car
  const carReaders = await Promise.all(carFiles.map(cf =>
    CarReader.fromBytes(cf.car)))
  const joiner = new TreewalkCarJoiner(carReaders)
  const directoryCar = await collectBytes(joiner.car())

  return {
    root,
    carFiles,
    directoryCar
  }
}

function generateUint8Array (length) {
  const data = Array.from({ length }, () => Math.floor(Math.random() * (255 - 0 + 1) + 0))

  return new Uint8Array(data)
}

module.exports = {
  generateCar,
  generateSplittedCar
}

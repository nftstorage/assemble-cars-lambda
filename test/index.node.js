const { serial: test } = require('ava')

const aws = require('aws-sdk-mock')
const { TreewalkCarSplitter } = require('carbites')
const { Blob } = require('@web-std/blob')
const { CarReader } = require('@ipld/car')
const { pack } = require('ipfs-car/pack')
const { sha256 } = require('multiformats/hashes/sha2')
const pDefer = require('p-defer')
const pMap = require('p-map')
const { toString } = require('uint8arrays')

const { handler } = require('../src/index') // lambda function
const { generateCar, generateSplittedCar } = require('./utils/cars')
const { largerContent } = require('./fixtures/larger-content')

const createS3PutEvent = ({
  bucketName = 'test-bucket',
  objectKey = 'raw/bafybeif3hyrtm2skjmldufjid3myfp37sxyqbtz7xe3f2fqyd7ugi33b2a/102702852462097292/ciqg66cgf5pib7qhdczhhzlpb3z2dk5xeo4l6zlel7snh3j4v3g7l5i.car'
} = {}) => ({
  Records: [{
    s3: {
      bucket: {
        name: bucketName
      },
      object: {
        key: objectKey
      }
    }
  }]
})

test.afterEach(t => {
  aws.restore('S3')
})

test('should fail if triggered not by a raw car', async t => {
  const error = await t.throwsAsync(() => handler(
    createS3PutEvent({
      objectKey: 'complete/bafybeif3hyrtm2skjmldufjid3myfp37sxyqbtz7xe3f2fqyd7ugi33b2a.car'
    })
  ))
  t.is(error.message, 'lambda should only triggered with raw namespace: CAR with complete/bafybeif3hyrtm2skjmldufjid3myfp37sxyqbtz7xe3f2fqyd7ugi33b2a.car from bucket test-bucket')
})

test('should put complete CAR file when raw file has complete structure metadata', async t => {
  const deferredWrite = pDefer()
  const { car, key, root } = await generateCar(2)

  // Mock S3
  aws.mock('S3', 'getObject', (params) => {
    if (params.Key === key) {
      return Promise.resolve({
        Body: car, Metadata: {
          structure: 'Complete'
        }
      })
    }
    return Promise.reject('invalid s3 object requestes')
  })
  aws.mock('S3', 'putObject', (params, cb) => {
    t.deepEqual(params.Key, `complete/${root.toString()}.car`)
    t.deepEqual(params.Body, car)

    deferredWrite.resolve()
    cb(undefined, putObjectOutput)
  })

  const { rootCid, structure } = await handler(createS3PutEvent({ objectKey: key }))
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Complete')

  // Guarantee write happened
  await deferredWrite.promise
})

test('should put complete raw file without metadata structure by its code and single block', async t => {
  const deferredWrite = pDefer()
  const { car, key, root } = await generateCar(2)

  // Mock S3
  aws.mock('S3', 'getObject', (params) => {
    if (params.Key === key) {
      return Promise.resolve({
        Body: car, Metadata: {}
      })
    }
    return Promise.reject('invalid s3 object requestes')
  })
  aws.mock('S3', 'putObject', (params, cb) => {
    t.deepEqual(params.Key, `complete/${root.toString()}.car`)
    t.deepEqual(params.Body, car)

    deferredWrite.resolve()
    cb(undefined, putObjectOutput)
  })

  const { rootCid, structure } = await handler(createS3PutEvent({ objectKey: key }))
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Complete')

  // Guarantee write happened
  await deferredWrite.promise
})

test('should put dag pb complete CAR file without metadata structure by traversing dag', async t => {
  const deferredWrite = pDefer()
  const { car, key, root } = await generateCar(2, { wrapWithDirectory: true })

  // Mock S3
  aws.mock('S3', 'getObject', (params) => {
    if (params.Key === key) {
      return Promise.resolve({
        Body: car, Metadata: {}
      })
    }
    return Promise.reject('invalid s3 object requestes')
  })
  aws.mock('S3', 'putObject', (params, cb) => {
    t.deepEqual(params.Key, `complete/${root.toString()}.car`)
    t.deepEqual(params.Body, car)

    deferredWrite.resolve()
    cb(undefined, putObjectOutput)
  })

  const { rootCid, structure } = await handler(createS3PutEvent({ objectKey: key }))
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Complete')

  // Guarantee write happened
  await deferredWrite.promise
})

test.skip('should not write dag cbor CAR file to complete as we wont know its size', async () => {
  
})

test('should not write complete CAR when dag pb incomplete CAR file written and not all CAR files already stored', async t => {
  const { carFiles, root } = await generateSplittedCar(36, 90, { wrapWithDirectory: true })

  // Body with only first car
  aws.mock('S3', 'getObject', (params) => {
    if (params.Key === carFiles[0].key) {
      return Promise.resolve({
        Body: carFiles[0].car, Metadata: {}
      })
    }
    return Promise.reject('invalid s3 object requestes')
  })
  aws.mock('S3', 'listObjectsV2', {
    Contents: [carFiles.map(carFile => ({
      Key: carFile.key,
      LastModified: new Date().toISOString(),
      ETag: '60faf8595327ae9c8a7f7ab099a5c9b0',
      Size: 49,
      StorageClass: 'STANDARD'
    })).shift()]
  })
  aws.mock('S3', 'putObject', (params, cb) => {
    throw new Error('should not write complete CAR when dag pb incomplete CAR file written and not all CAR files already stored')
  })

  const { rootCid, structure } = await handler(createS3PutEvent({ objectKey: carFiles[0].key }))
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Partial')
})

test.only('dag pb incomplete CAR file with all CAR files already stored triggers gateway request', async t => {
  const { carFiles, root } = await generateSplittedCar(36, 90, { wrapWithDirectory: true })

  console.log('car files', carFiles)
  // Write only second CAR
  aws.mock('S3', 'getObject', (params) => {
    console.log('params', params)
    if (params.Key === carFiles[0].key) {
      return Promise.resolve({
        Body: carFiles[0].car, Metadata: {}
      })
    }
    return Promise.reject('invalid s3 object requestes')
  })

  aws.mock('S3', 'listObjectsV2', {
    Contents: carFiles.map(carFile => ({
      Key: carFile.key,
      LastModified: new Date().toISOString(),
      ETag: '60faf8595327ae9c8a7f7ab099a5c9b0',
      Size: 49,
      StorageClass: 'STANDARD'
    }))
  })

  const { rootCid, structure, directoryStructure } = await handler(createS3PutEvent({ objectKey: carFiles[0].key }))
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Partial')
  console.log('directoryStructure', directoryStructure)
})

const putObjectOutput = {
  ETag: '6805f2cfc46c0f04559748bb039d69ae',
  VersionId: 'psM2sYY4.o1501dSx8wMvnkOzSBB.V4a'
}

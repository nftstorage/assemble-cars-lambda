const { serial: test } = require('ava')

const aws = require('aws-sdk-mock')
const pDefer = require('p-defer')

const { handler } = require('../src/index') // lambda function
const { generateCar, generateSplittedCar } = require('./utils/cars')
const { createS3PutEvent } = require('./utils/s3')

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

test('should write to /complete when triggering file has complete structure metadata', async t => {
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

test('should write to /complete when triggering file has no metadata structure and has a single raw block', async t => {
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

test('should write to /complete when triggering file has no metadata structure, but has DagPB root and is complete', async t => {
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

test('should not write to /complete when triggering file has DagPB root but is incomplete and directory does not contain all DAG chunks', async t => {
  const { carFiles, root } = await generateSplittedCar(150, 130, {
    wrapWithDirectory: true,
    maxChunkSize: 100,
  })

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

  const { rootCid, structure, directoryStructure } = await handler(createS3PutEvent({ objectKey: carFiles[0].key }))
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Partial')
  t.deepEqual(directoryStructure, 'Partial')
})

test('should not write to /complete when triggering file has DagPB root and is incomplete, directory does not contain all DAG chunks but size is already bigger', async t => {
  const { carFiles, root } = await generateSplittedCar(150, 130, {
    wrapWithDirectory: true,
    maxChunkSize: 100,
  })

  aws.mock('S3', 'getObject', (params) => {
    const getObjectCar = carFiles.find((carFile) => carFile.key === params.Key)
    if (getObjectCar) {
      return Promise.resolve({
        Body: getObjectCar.car, Metadata: {}
      })
    }
    return Promise.reject('invalid s3 object requestes')
  })
  aws.mock('S3', 'listObjectsV2', {
    Contents: carFiles
      // Remove last CAR
      .filter(cf => cf.key !== carFiles[carFiles.length - 1].key)
      .map(carFile => ({
        Key: carFile.key,
        LastModified: new Date().toISOString(),
        ETag: '60faf8595327ae9c8a7f7ab099a5c9b0',
        Size: carFile.size,
        StorageClass: 'STANDARD'
      }))
  })
  aws.mock('S3', 'putObject', (params, cb) => {
    throw new Error('should not write complete CAR when dag pb incomplete CAR file written and not all CAR files already stored')
  })

  const { rootCid, structure, directoryStructure } = await handler(createS3PutEvent({ objectKey: carFiles[0].key }))
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Partial')
  t.deepEqual(directoryStructure, 'Partial')
})

test('should write to /complete when triggering file has DagPB root and is incomplete, but directory contains all DAG chunks', async t => {
  const deferredWrite = pDefer()
  const { carFiles, root, directoryCar } = await generateSplittedCar(150, 130, {
    wrapWithDirectory: true,
    maxChunkSize: 100,
  })

  aws.mock('S3', 'getObject', (params) => {
    const getObjectCar = carFiles.find((carFile) => carFile.key === params.Key)
    if (getObjectCar) {
      return Promise.resolve({
        Body: getObjectCar.car, Metadata: {}
      })
    }
    return Promise.reject('invalid s3 object requestes')
  })
  aws.mock('S3', 'listObjectsV2', {
    Contents: carFiles.map(carFile => ({
      Key: carFile.key,
      LastModified: new Date().toISOString(),
      ETag: '60faf8595327ae9c8a7f7ab099a5c9b0',
      Size: carFile.size,
      StorageClass: 'STANDARD'
    }))
  })
  aws.mock('S3', 'putObject', (params, cb) => {
    t.deepEqual(params.Key, `complete/${root.toString()}.car`)
    t.deepEqual(params.Body, directoryCar)

    deferredWrite.resolve()
    cb(undefined, putObjectOutput)
  })

  // Write only last CAR
  const { rootCid, structure, directoryStructure } = await handler(createS3PutEvent({ objectKey: carFiles[carFiles.length - 1].key }))
  t.deepEqual(root.toString(), rootCid.toString())
  t.deepEqual(structure, 'Partial')
  t.deepEqual(directoryStructure, 'Complete')
  
  // Guarantee write happened
  await deferredWrite.promise
})

const putObjectOutput = {
  ETag: '6805f2cfc46c0f04559748bb039d69ae',
  VersionId: 'psM2sYY4.o1501dSx8wMvnkOzSBB.V4a'
}

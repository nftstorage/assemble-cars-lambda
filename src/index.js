'use strict'

const aws = require('aws-sdk')

const { S3Client } = require ('./s3-client.js')
const { Logger } = require('./logging')
const { carStat, inspectCarBlocks } = require('./car')

// TODO: Refine MAX_SIZE_TO_ATTEMPT - currently 100MB
const MAX_SIZE_TO_ATTEMPT = 100 * 1024 * 1024

/**
 * Lambda triggers on S3 write object event for "raw/" prefix.
 *
 * We write the resulting car to "/complete" namespace if:
 * - S3 Object Metadata has a "Complete" structure 
 * - S3 Object has a DagPB encoded root with a known size "acceptable" and the S3 directory
 * for that root CID already has all the expected chunks to traverse the dag.
 */
async function main(event) {
  const logger = new Logger()
  const s3Client = new S3Client(logger)

  // Get the object from the event
  const bucket = event.Records[0].s3.bucket.name
  const key = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, ' ')
  )

  if (!key.startsWith('raw')) {
    const e = new Error(`lambda should only triggered with raw namespace: CAR with ${key} from bucket ${bucket}`)
    logger.error(e)
    throw e
  }

  const { body, metadata } = await s3Client.getObject(bucket, key)
  // @ts-ignore body has different type from AWS SDK
  const { rootCid, structure, size } = await inspectCar(body, metadata, logger, { bucket, key })
  const completePath = `complete/${rootCid}.car`

  // Written CAR is already complete
  if (structure === 'Complete') {
    console.log('structure is complete')
    await s3Client.putObject(bucket, completePath, body)

    return { rootCid, structure }
  }

  // Validate Written CAR is DagPB encoded and we know its size
  if (!size) {
    logger.info(
      key,
      `Car with root ${rootCid} does not have a DagPB root and we cannot find the size`
    )
    return { rootCid, structure }
  }

  if (size > MAX_SIZE_TO_ATTEMPT) {
    logger.info(
      key,
      `Car with root ${rootCid} is not complete for object ${key} from bucket ${bucket} and its known size is larger than ${MAX_SIZE_TO_ATTEMPT}`
    )
    return { rootCid, structure }
  }

  console.log('get directory')
  const { accumSize } = await s3Client.getDirectoryStat(bucket, key)

  if (size > accumSize) {
    logger.info(
      key,
      `Car with root ${rootCid} is still not entirely uploaded to bucket ${bucket}`
    )
    return { rootCid, structure }
  }

  // Attempt to traverse the full dag
  return { rootCid, structure, directoryStructure: 'Complete' }
}

/**
 * @param {Uint8Array} car
 * @param {Object} metadata
 * @param {Logger} logger
 * @param {S3Inputs} s3Inputs
 */
async function inspectCar(car, metadata, logger, { key, bucket }) {
  let rootCid, structure, size
  try {
    const stat = await carStat(car)
    rootCid = stat.rootCid

    logger.debug(
      key,
      `Obtained root cid ${rootCid} for object ${key} from bucket ${bucket}`
    )

    const inspection = await inspectCarBlocks(rootCid, stat.blocksIterator)
    structure = inspection.structure || metadata.structure
    size = inspection.size

  } catch (err) {
    logger.error(
      err,
      {
        complementMessage: `Error parsing CAR with ${key} from bucket ${bucket}: `
      }
    )
    throw err
  }

  logger.debug(
    key,
    `Obtained structure ${structure} for object ${key} from bucket ${bucket}`
  )

  return {
    rootCid,
    structure,
    size
  }
}

/**
 * @typedef S3Inputs
 * @property {string} bucket
 * @property {string} key
 */

exports.handler = main

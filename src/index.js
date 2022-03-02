'use strict'

const aws = require('aws-sdk')

const { carStat, inspectCarBlocks } = require('./car')
const { logger, elapsed, serializeError } = require('./logging')

// TODO: Refine MAX_SIZE_TO_ATTEMPT - currently 100MB
const MAX_SIZE_TO_ATTEMPT = 100 * 1024 * 1024

// TODO: thin s3 client?

/**
 * Lambda triggers on S3 write object event.
 *
 * With the know object key, we get the S3 object and inspect the CAR content to get its:
 * - rootCid
 * - structure
 * - size
 *
 * We write the resulting car to "/complete" namespace if:
 * - S3 Object Metadata has a "Complete" structure 
 * - S3 Object has a DagPB encoded root with a known size "acceptable" and the S3 directory
 * for that root CID already has all the expected chunks to traverse the dag.
 */
async function main(event) {
  const s3 = new aws.S3({ apiVersion: '2006-03-01' })
  const start = process.hrtime.bigint()

  // Get the object from the event
  const bucket = event.Records[0].s3.bucket.name
  const key = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, ' ')
  )

  if (!key.startsWith('raw')) {
    logger.error(
      `lambda should only triggered with raw namespace: CAR with ${key} from bucket ${bucket}:`
    )
    throw new Error('lambda should only triggered with raw namespace')
  }

  const logOpts = { start, bucket, key }

  const { body, metadata } = await getS3Object(s3, bucket, key, logOpts)
  // @ts-ignore body has different type from AWS SDK
  const { rootCid, structure, size } = await inspectCar(body, metadata, logOpts)

  if (structure === 'Complete') {
    await putS3Object(s3, bucket, rootCid.toString(), body, logOpts)

    return { rootCid, structure }
  }

  /**
   * If inserted CAR from the trigger is not complete, let's verify if:
   * - We know the dag size of the CAR (aka we only support read for dagPB for now)
   * - The know size is bigger than the MAX_SIZE_TO_ATTEMPT
   * - We have already received everything in S3
   */

  if (!size) {
    logger.info(
      { elapsed: elapsed(start), path: key },
      `Car with root ${rootCid} does not have a DagPB root and we cannot find the size`
    )
    return { rootCid, structure }
  }

  if (size > MAX_SIZE_TO_ATTEMPT) {
    logger.info(
      { elapsed: elapsed(start), path: key },
      `Car with root ${rootCid} is not complete for object ${key} from bucket ${bucket} and its known size is larger than ${MAX_SIZE_TO_ATTEMPT}`
    )
    return { rootCid, structure }
  }

  const { accumSize } = await getDirectoryStat(s3, bucket, key, logOpts)

  if (size > accumSize) {
    logger.info(
      { elapsed: elapsed(start), path: key },
      `Car with root ${rootCid} is still not entirely uploaded to bucket ${bucket}`
    )
    return { rootCid, structure }
  }
}

/**
 * @param {Uint8Array} body
 * @param {Object} metadata
 * @param {LogOpts} logOpts
 */
async function inspectCar(body, metadata, { start, key, bucket }) {
  let rootCid, structure, size
  try {
    const stat = await carStat(body)
    rootCid = stat.rootCid

    logger.debug(
      { elapsed: elapsed(start), path: key },
      `Obtained root cid ${rootCid} for object ${key} from bucket ${bucket}`
    )

    const inspection = await inspectCarBlocks(rootCid, stat.blocksIterator)
    structure = inspection.structure || metadata.structure
    size = inspection.size

  } catch (err) {
    logger.error(
      `Error parsing CAR with ${key} from bucket ${bucket}: ${serializeError(
        err
      )}`
    )
    throw err
  }

  logger.debug(
    { elapsed: elapsed(start), path: key },
    `Obtained structure ${structure} for object ${key} from bucket ${bucket}`
  )

  return {
    rootCid,
    structure,
    size
  }
}

/**
 * @param {aws.S3} s3
 * @param {string} bucket
 * @param {string} key
 * @param {LogOpts} logOpts
 */
async function getS3Object(s3, bucket, key, { start }) {
  let s3Object

  try {
    logger.debug(
      { elapsed: elapsed(start), path: key },
      `Getting object ${key} from bucket ${bucket}`
    )

    s3Object = await s3
      .getObject({
        Bucket: bucket,
        Key: key,
      })
      .promise()
  } catch (err) {
    logger.error(
      `Error getting object ${key} from bucket ${bucket}: ${serializeError(
        err
      )}`
    )
    throw err
  }

  return {
    body: s3Object.Body,
    metadata: s3Object.Metadata
  }
}

/**
 * @param {aws.S3} s3
 * @param {string} bucket
 * @param {string} rootCid
 * @param {aws.S3.Body} body
 * @param {LogOpts} logOpts
 */
async function putS3Object(s3, bucket, rootCid, body, { start }) {
  const path = `complete/${rootCid}.car`

  try {
    logger.debug(
      { elapsed: elapsed(start), path },
      `Putting object ${path} to bucket ${bucket}`
    )

    await s3
      .putObject({
        Bucket: bucket,
        Key: path,
        Body: body
      })
      .promise()
  } catch (err) {
    logger.error(
      `Error putting object ${path} to bucket ${bucket}: ${serializeError(
        err
      )}`
    )
    throw err
  }
}

/**
 * @param {aws.S3} s3
 * @param {string} bucket
 * @param {string} key
 * @param {LogOpts} logOpts
 */
async function getDirectoryStat(s3, bucket, key, { start }) {
  const prefix = key.replace(/[^\/]*$/, '').slice(0, -1)

  let accumSize
  try {
    logger.debug(
      { elapsed: elapsed(start), path: key },
      `Getting list of objects of prefix ${prefix} from bucket ${bucket}`
    )
    const s3ListObjects = await s3.listObjectsV2({
      Bucket: bucket,
      Prefix: prefix
    }).promise()

    accumSize = s3ListObjects.Contents.reduce((acc, obj) => acc + obj.Size, 0)
  } catch (err) {
    logger.error(
      `Error listing objects of prefix ${prefix} from bucket ${bucket}: ${serializeError(
        err
      )}`
    )
    throw err
  }

  return {
    accumSize
  }
}

/**
 * @typedef LogOpts
 * @property {bigint} start
 * @property {string} bucket
 * @property {string} key
 */

exports.handler = main

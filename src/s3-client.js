'use strict'

const aws = require('aws-sdk')

class S3Client {
  /**
   * @param {import('./logging.js').Logger} logger
   */
  constructor(logger) {
    this.s3 = new aws.S3({ apiVersion: '2006-03-01' })
    this.logger = logger
  }

  /**
   * @param {string} bucket
   * @param {string} key
   */
  async getObject(bucket, key) {
    let s3Object

    try {
      this.logger.debug(key, `Getting object ${key} from bucket ${bucket}`)

      s3Object = await this.s3
        .getObject({
          Bucket: bucket,
          Key: key,
        })
        .promise()
    } catch (err) {
      this.logger.error(err, {
        complementMessage: `Error getting object ${key} from bucket ${bucket}: `,
      })
      throw err
    }

    return {
      body: s3Object.Body,
      metadata: s3Object.Metadata,
    }
  }

  /**
   * @param {string} bucket
   * @param {string} key
   * @param {aws.S3.Body} body
   */
  async putObject(bucket, key, body) {
    try {
      this.logger.debug(key, `Putting object ${key} to bucket ${bucket}`)

      await this.s3
        .putObject({
          Bucket: bucket,
          Key: key,
          Body: body,
        })
        .promise()
    } catch (err) {
      this.logger.error(err, {
        complementMessage: `Error putting object ${key} to bucket ${bucket}: `,
      })
      throw err
    }
  }

  /**
   * @param {string} bucket
   * @param {string} key
   */
  async getDirectoryStat(bucket, key) {
    const prefix = key.replace(/[^\/]*$/, '').slice(0, -1)

    let accumSize,
      directoryKeys = []
    try {
      this.logger.debug(
        key,
        `Getting list of objects of prefix ${prefix} from bucket ${bucket}`
      )
      const s3ListObjects = await this.s3
        .listObjectsV2({
          Bucket: bucket,
          Prefix: prefix,
        })
        .promise()

      accumSize = s3ListObjects.Contents.reduce((acc, obj) => acc + obj.Size, 0)
      directoryKeys = s3ListObjects.Contents.map((obj) => obj.Key)
    } catch (err) {
      this.logger.error(err, {
        complementMessage: `Error listing objects of prefix ${prefix} from bucket ${bucket}: `,
      })
      throw err
    }

    return {
      accumSize,
      directoryKeys,
    }
  }
}

module.exports = {
  S3Client,
}

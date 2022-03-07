'use strict'

const pino = require('pino')

let destination
try {
  if (process.env.NODE_ENV !== 'production') {
    destination = require('pino-pretty')()
  }
} catch (e) {
  // No-op
}

const logger = pino(
  {
    level: (process.env.NODE_DEBUG ?? '').includes('gateway-read-test')
      ? 'debug'
      : 'info',
    base: undefined,
    timestamp: pino.stdTimeFunctions.isoTime,
  },
  destination
)

class Logger {
  constructor() {
    this.start = process.hrtime.bigint()
  }

  /**
   * @param {string} path
   * @param {string} message
   */
  info(path, message) {
    logger.info(
      { elapsed: elapsed(this.start), path },
      message
    )
  }

  /**
   * @param {string} path
   * @param {string} message
   */
  debug(path, message) {
    logger.debug(
      { elapsed: elapsed(this.start), path },
      message
    )
  }

  /**
   * @param {Error} e
   * @param {Object} [options]
   * @param {String} [options.complementMessage]
   */
  error(e, { complementMessage = '' } = {}) {
    logger.error(`${complementMessage}${serializeError(e)}`)
  }
}

const durationUnits = {
  milliseconds: 1e6,
  seconds: 1e9,
}

function elapsed(startTime, precision = 3, unit = 'milliseconds') {
  const dividend = durationUnits[unit] ?? durationUnits.milliseconds
  return (Number(process.hrtime.bigint() - startTime) / dividend).toFixed(
    precision
  )
}

function serializeError(e) {
  return `[${e.code || e.constructor.name}] ${e.message}`
}

module.exports = {
  Logger
}
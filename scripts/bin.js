#!/usr/bin/env node
const debug = require('debug')
const fs = require('fs')
const path = require('path')

const aws = require('aws-sdk')
const dotenv = require('dotenv')
const sade = require('sade')

const lambdaPolicy = require('../aws/lambda-policy.json')
const roleTrustPolicy = require('../aws/role-trust-policy.json')
const s3Trigger = require('../aws/s3-trigger.json')

const log = debug('assemble-cars:init')
const prog = sade('assemble-cars')
dotenv.config({
  path: path.join(__dirname, '../.env.local')
})

prog
  .command('init')
  .describe('Init lambda')
  .action(initLambda)

prog.parse(process.argv)

async function initLambda() {
  const apiVersion = 'latest'
  const region = mustGetEnv('AWS_REGION')
  const bucket = mustGetEnv('AWS_BUCKET')
  const name = mustGetEnv('NAME')
  mustGetEnv('AWS_ACCESS_KEY_ID')
  mustGetEnv('AWS_SECRET_ACCESS_KEY')

  const iam = new aws.IAM()
  const lambda = new aws.Lambda({ apiVersion, region })
  const s3Api = new aws.S3()

  // Create Role
  const createRoleParams = {
    RoleName: name,
    AssumeRolePolicyDocument: JSON.stringify(roleTrustPolicy),
  }
  const role = await new Promise((resolve, reject) => {
    iam.createRole(createRoleParams, (err, data) => {
      if (err) return reject(err)
      return resolve(data)
    })
  })
  log(role)
  
  // Create Policy
  const createPolicyParams = {
    PolicyName: name,
    PolicyDocument: JSON.stringify(lambdaPolicy)
  }
  const policy = await new Promise((resolve, reject) => {
    iam.createPolicy(createPolicyParams, (err, data) => {
      if (err) return reject(err)
      return resolve(data)
    })
  })
  log(policy)

  // Attach policy params
  const attachPolicyParams = {
    PolicyArn: policy.Policy.Arn,
    RoleName: name,
  }
  await new Promise((resolve, reject) => {
    iam.attachRolePolicy(attachPolicyParams, (err, data) => {
      if (err) return reject(err)
      return resolve(data)
    })
  })

  // Create lambda function
  const zipContents = fs.readFileSync(path.join(__dirname, '../dist/lambda.zip'))
  const createFunctionParams = {
    Code: {
      ZipFile: zipContents,
    },
    FunctionName: name,
    Handler: `${name}.handler`,
    Role: role.Role.Arn,
    Runtime: 'nodejs14.x',
    MemorySize: 512
  }

  const lambdaFn = await new Promise((resolve, reject) => {
    lambda.createFunction(createFunctionParams, (err, data) => {
      if (err) return reject(err)
      return resolve(data)
    })
  })

  log(lambdaFn)
  // Grant lambda permissions to invoke functions
  const grantPermissionParams = {
    FunctionName: name,
    StatementId: 'AllowToBeInvoked',
    Action: 'lambda:InvokeFunction',
    Principal: 's3.amazonaws.com'
  }
  await new Promise((resolve, reject) => {
    lambda.addPermission(grantPermissionParams, (err, data) => {
      if (err) return reject(err)
      return resolve(data)
    })
  })

  // Attach S3 events to lambda function
  s3Trigger.LambdaFunctionConfigurations.map((conf) => {
    conf.LambdaFunctionArn = lambdaFn.FunctionArn
  })  
  const notificationParams = {
    Bucket: bucket,
    NotificationConfiguration: s3Trigger
  }
  await new Promise((resolve, reject) => {
    s3Api.putBucketNotificationConfiguration(notificationParams, (err, data) => {
      if (err) return reject(err)
      return resolve(data)
    })
  })
  log(`Successfully created lambda ${name}`)
}

/**
 * @param {string} name
 */
function mustGetEnv(name) {
  const value = process.env[name]
  if (!value) throw new Error(`missing ${name} environment variable`)
  return value
}

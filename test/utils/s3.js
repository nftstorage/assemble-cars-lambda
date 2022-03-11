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

module.exports = {
  createS3PutEvent
}

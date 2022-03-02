# assemble-cars-lambda

The `assemble-cars-lambda` will trigger on writes to a given S3 bucket and assemble partial raw CAR files into complete CAR files.

Once a CAR is written, the lambda function will inspect its completeness, as well as if its wrapping directory as all the CARs of the DAG. If we can traverse all the DAG, the partial CARs will be joined and stored on a `/complete` namespace.

## Assumptions

There are a few assumptions in place and this lambda function will assemble CARs if:
- S3 Object Metadata has a "Complete" structure 
- S3 Object has a DagPB encoded root with a known size __acceptable__ (100MB) and the S3 directory for that root CID already has all the DAG chunks

## AWS Setup

This project already comes with a script for build and deploy (which is also integrated with Github Actions). However it needs:
- Project creation in AWS
  - It needs a role policy with S3 `s3:GetObject`, `s3:PutObject` and `s3:ListBucket` privileges
- Secrets setup in Repo secrets for automatic deploy
- Environment variables setup

## Development and AWS bootstrap

Set environment variables in a `.env.local` file in the root directory of this project:

```env
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
AWS_REGION=""
AWS_BUCKET=""
NAME="dotstorage-assemble-cars"
```

```sh
npm run init
```

## What's next?

- CBOR
- What to do with really large CAR files ?

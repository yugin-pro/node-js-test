// [START bigquerydatatransfer_quickstart]
import bigqueryDataTransfer from '@google-cloud/bigquery-data-transfer';
import { bqCredentials } from '../credentials.mjs'
const client = new bigqueryDataTransfer.v1.DataTransferServiceClient(bqCredentials);

const requestSource = {
  parent: `projects/${bqCredentials.projectId}/locations/europe`,
  dataSourceIds: ['scheduled_query']
};

async function main() {
  let configList = await getConfiglistScheduledQuery(requestSource)
  replaceQueryInScheduledQuery(configList, ',dm-analyst-flood-room', '')
}


async function getConfiglistScheduledQuery(requestSource) {
  const options = { autoPaginate: false };
  const responses = await client.listTransferConfigs(requestSource, options);

  return responses[0].filter(trConf => trConf.destinationDatasetId.includes('ALERT'))
}

async function replaceQueryInScheduledQuery(configList, fromText, toText) {
  for (const config of configList) {
    let transformedQuery = config.params.fields.query.stringValue.replace(fromText, toText)
    config.params.fields.query.stringValue = transformedQuery
    let updateRequest = {
      transferConfig: config,
      updateMask: { paths: ["params"] }
    }
    let respUpd = await client.updateTransferConfig(updateRequest)
    console.log('updSource', respUpd[0].params.fields.query.stringValue);
  }

}


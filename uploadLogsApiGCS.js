// Imports the Google Cloud client library
import { Storage } from '@google-cloud/storage'
import { bqCredentials, cloudBucket, yandexMetrikaToken, metrikaCounterId } from '../credentials.mjs'
import fetch from 'node-fetch';


const bucket = new Storage(bqCredentials).bucket(cloudBucket);


main(27113291)

async function main(request_id) {
    const request = await getRequestInfo(request_id)
    request.log_request.parts.forEach(part => {
        getRequestPartData(part.part_number, request_id).then(data => {
            bucket.file(`yandex-logs-api/${request.log_request.source}/${request.log_request.date1}/${part.part_number}.csv`).save(data)
        })
        
    })

}


async function getRequestPartData(part, request_id) {
    const options = {
        'headers': {
            'Authorization': 'OAuth ' + yandexMetrikaToken
        }
    };
    const response = await fetch(`https://api-metrika.yandex.net/management/v1/counter/${metrikaCounterId}/logrequest/${request_id}/part/${part}/download`, options);
    return await response.text()

}

async function getRequestInfo(request_id) {
    const options = {
        'headers': {
            'Authorization': 'OAuth ' + yandexMetrikaToken
        }
    };
    const response = await fetch(`https://api-metrika.yandex.net/management/v1/counter/${metrikaCounterId}/logrequest/${request_id}`, options);

    return await response.json();

}


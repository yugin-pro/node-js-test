// Imports the Google Cloud client library
import { Storage } from '@google-cloud/storage'
import { bqCredentials, cloudBucket, yandexMetrikaToken, metrikaCounterId } from '../credentials.mjs'
import fetch from 'node-fetch';


const bucket = new Storage(bqCredentials).bucket(cloudBucket);

let dateList = getDateListBetween('2021-02-01', '2021-02-28')

dateList.then( dateList => {
    downloadLogsController(dateList)
})


// downloadToGcs(27141880)


async function downloadLogsController(dateList = []) {
    for (let date of dateList) {
         createRequest('visits', date).then(request => downloadToGcs(request.log_request.request_id))
         createRequest('hits', date).then(request => downloadToGcs(request.log_request.request_id))
         await new Promise(resolve => setTimeout(resolve, 120000))
    }

}

async function downloadToGcs(request_id) {
    console.log('start download' + request_id);
    let request = await getRequestInfo(request_id)
    delete request.log_request.fields
    console.log('req info', request);
    let counter = 0
    let pArr = []


    while (request.log_request.status !== 'processed') {
        if (counter > 10) {
            return console.log('took over 10 minutes');
        }
        await new Promise(resolve => setTimeout(resolve, 60000))
        request = await getRequestInfo(request_id)
        delete request.log_request.fields
        counter++
        console.log(request.log_request.source, request.log_request.date1, request.log_request.request_id, request.log_request.status, counter);
        console.log(request);
    }

    for (const part of request.log_request.parts) {
        for (let i = 1; i < 5; i++) {
            try {
                let data = await getRequestPartData(part.part_number, request_id)
                await bucket.file(`yandex-logs-api/${request.log_request.source}/${request.log_request.date1}/${part.part_number}.csv`).save(data)
                console.log(request.log_request.request_id + ' uploaded', part);
                i=5
            } catch (error) {
                console.log(part, error);
                await new Promise(resolve => setTimeout(resolve, 1000))
            }
        }

    }
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

async function getRequestList() {
    const options = {
        'headers': {
            'Authorization': 'OAuth ' + yandexMetrikaToken
        }
    };
    const response = await fetch(`https://api-metrika.yandex.net/management/v1/counter/${metrikaCounterId}/logrequests`, options);
    if (response.status == 200) {
        return await response.json();
    }
}

async function createRequest(source, date) {

    let fields = {
        hits: ["ym:pv:watchID", "ym:pv:counterID", "ym:pv:date", "ym:pv:dateTime", "ym:pv:title", "ym:pv:URL", "ym:pv:referer", "ym:pv:UTMCampaign", "ym:pv:UTMContent", "ym:pv:UTMMedium", "ym:pv:UTMSource", "ym:pv:UTMTerm", "ym:pv:browser", "ym:pv:browserMajorVersion", "ym:pv:browserMinorVersion", "ym:pv:browserCountry", "ym:pv:browserEngine", "ym:pv:browserEngineVersion1", "ym:pv:browserEngineVersion2", "ym:pv:browserEngineVersion3", "ym:pv:browserEngineVersion4", "ym:pv:browserLanguage", "ym:pv:clientTimeZone", "ym:pv:cookieEnabled", "ym:pv:deviceCategory", "ym:pv:from", "ym:pv:hasGCLID", "ym:pv:GCLID", "ym:pv:ipAddress", "ym:pv:javascriptEnabled", "ym:pv:mobilePhone", "ym:pv:mobilePhoneModel", "ym:pv:openstatAd", "ym:pv:openstatCampaign", "ym:pv:openstatService", "ym:pv:openstatSource", "ym:pv:operatingSystem", "ym:pv:operatingSystemRoot", "ym:pv:physicalScreenHeight", "ym:pv:physicalScreenWidth", "ym:pv:regionCity", "ym:pv:regionCountry", "ym:pv:regionCityID", "ym:pv:regionCountryID", "ym:pv:screenColors", "ym:pv:screenFormat", "ym:pv:screenHeight", "ym:pv:screenOrientation", "ym:pv:screenWidth", "ym:pv:windowClientHeight", "ym:pv:windowClientWidth", "ym:pv:lastTrafficSource", "ym:pv:lastSearchEngine", "ym:pv:lastSearchEngineRoot", "ym:pv:lastAdvEngine", "ym:pv:artificial", "ym:pv:pageCharset", "ym:pv:isPageView", "ym:pv:link", "ym:pv:download", "ym:pv:notBounce", "ym:pv:lastSocialNetwork", "ym:pv:httpError", "ym:pv:clientID", "ym:pv:networkType", "ym:pv:lastSocialNetworkProfile", "ym:pv:goalsID", "ym:pv:shareService", "ym:pv:shareURL", "ym:pv:shareTitle", "ym:pv:iFrame", "ym:pv:parsedParamsKey1", "ym:pv:parsedParamsKey2", "ym:pv:parsedParamsKey3", "ym:pv:parsedParamsKey4", "ym:pv:parsedParamsKey5", "ym:pv:parsedParamsKey6", "ym:pv:parsedParamsKey7", "ym:pv:parsedParamsKey8", "ym:pv:parsedParamsKey9", "ym:pv:parsedParamsKey10"],
        visits: ["ym:s:visitID", "ym:s:counterID", "ym:s:watchIDs", "ym:s:date", "ym:s:dateTime", "ym:s:dateTimeUTC", "ym:s:isNewUser", "ym:s:startURL", "ym:s:endURL", "ym:s:pageViews", "ym:s:visitDuration", "ym:s:bounce", "ym:s:ipAddress", "ym:s:regionCountry", "ym:s:regionCity", "ym:s:regionCountryID", "ym:s:regionCityID", "ym:s:clientID", "ym:s:networkType", "ym:s:goalsID", "ym:s:goalsSerialNumber", "ym:s:goalsDateTime", "ym:s:goalsPrice", "ym:s:goalsOrder", "ym:s:goalsCurrency", "ym:s:lastTrafficSource", "ym:s:lastAdvEngine", "ym:s:lastReferalSource", "ym:s:lastSearchEngineRoot", "ym:s:lastSearchEngine", "ym:s:lastSocialNetwork", "ym:s:lastSocialNetworkProfile", "ym:s:referer", "ym:s:lastDirectClickOrder", "ym:s:lastDirectBannerGroup", "ym:s:lastDirectClickBanner", "ym:s:lastDirectClickOrderName", "ym:s:lastClickBannerGroupName", "ym:s:lastDirectClickBannerName", "ym:s:lastDirectPhraseOrCond", "ym:s:lastDirectPlatformType", "ym:s:lastDirectPlatform", "ym:s:lastDirectConditionType", "ym:s:lastCurrencyID", "ym:s:from", "ym:s:UTMCampaign", "ym:s:UTMContent", "ym:s:UTMMedium", "ym:s:UTMSource", "ym:s:UTMTerm", "ym:s:openstatAd", "ym:s:openstatCampaign", "ym:s:openstatService", "ym:s:openstatSource", "ym:s:hasGCLID", "ym:s:lastGCLID", "ym:s:firstGCLID", "ym:s:lastSignificantGCLID", "ym:s:browserLanguage", "ym:s:browserCountry", "ym:s:clientTimeZone", "ym:s:deviceCategory", "ym:s:mobilePhone", "ym:s:mobilePhoneModel", "ym:s:operatingSystemRoot", "ym:s:operatingSystem", "ym:s:browser", "ym:s:browserMajorVersion", "ym:s:browserMinorVersion", "ym:s:browserEngine", "ym:s:browserEngineVersion1", "ym:s:browserEngineVersion2", "ym:s:browserEngineVersion3", "ym:s:browserEngineVersion4", "ym:s:cookieEnabled", "ym:s:javascriptEnabled", "ym:s:screenFormat", "ym:s:screenColors", "ym:s:screenOrientation", "ym:s:screenWidth", "ym:s:screenHeight", "ym:s:physicalScreenWidth", "ym:s:physicalScreenHeight", "ym:s:windowClientWidth", "ym:s:windowClientHeight", "ym:s:purchaseID", "ym:s:purchaseDateTime", "ym:s:purchaseAffiliation", "ym:s:purchaseRevenue", "ym:s:purchaseTax", "ym:s:purchaseShipping", "ym:s:purchaseCoupon", "ym:s:purchaseCurrency", "ym:s:purchaseProductQuantity", "ym:s:productsPurchaseID", "ym:s:productsID", "ym:s:productsName", "ym:s:productsBrand", "ym:s:productsCategory", "ym:s:productsCategory1", "ym:s:productsCategory2", "ym:s:productsCategory3", "ym:s:productsCategory4", "ym:s:productsCategory5", "ym:s:productsVariant", "ym:s:productsPosition", "ym:s:productsPrice", "ym:s:productsCurrency", "ym:s:productsCoupon", "ym:s:productsQuantity", "ym:s:impressionsURL", "ym:s:impressionsDateTime", "ym:s:impressionsProductID", "ym:s:impressionsProductName", "ym:s:impressionsProductBrand", "ym:s:impressionsProductCategory", "ym:s:impressionsProductCategory1", "ym:s:impressionsProductCategory2", "ym:s:impressionsProductCategory3", "ym:s:impressionsProductCategory4", "ym:s:impressionsProductCategory5", "ym:s:impressionsProductVariant", "ym:s:impressionsProductPrice", "ym:s:impressionsProductCurrency", "ym:s:impressionsProductCoupon", "ym:s:offlineCallTalkDuration", "ym:s:offlineCallHoldDuration", "ym:s:offlineCallMissed", "ym:s:offlineCallTag", "ym:s:offlineCallFirstTimeCaller", "ym:s:offlineCallURL", "ym:s:parsedParamsKey1", "ym:s:parsedParamsKey2", "ym:s:parsedParamsKey3", "ym:s:parsedParamsKey4", "ym:s:parsedParamsKey5", "ym:s:parsedParamsKey6"]
    }
    let options = {
        'headers': {
            'Authorization': 'OAuth ' + yandexMetrikaToken
        },
        method: 'post',
    }
    let response = await fetch(`https://api-metrika.yandex.net/management/v1/counter/${metrikaCounterId}/logrequests?date1=${date}&date2=${date}&fields=${fields[source].join()}&source=${source}`, options);
    if (response.status == 200) {
        console.log('created Request');
        return await response.json();
    } else {
        return await response.text()
    }
}


function  getDateListBetween(startDate, endDate) {
    let start = new Date(startDate)
    let end =  new Date(endDate)
    let arr = []
    while (start <= end ) {
        arr.push(new Intl.DateTimeFormat("fr-ca").format(start))
        start.setDate(start.getDate() + 1)
        console.log(start);
    }
    return new Promise(resolve => resolve(arr))
    
}


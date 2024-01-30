require('dotenv').config();
const axios = require('axios');
const { ethers } = require('ethers');
const fs = require('fs');
const { gql, request } = require('graphql-request');
const { StakehouseSDK } = require("@blockswaplab/stakehouse-sdk");

const INFURA_PROJECT_ID = process.env.INFURA_PROJECT_ID;
const INFURA_PROJECT_SECRET = process.env.INFURA_PROJECT_SECRET;
const PRIV_KEY_1 = process.env.PRIV_KEY_1;
const MAINNET_BEACON_NODE = process.env.MAINNET_BEACON_NODE;

const getAllBLSPublicKeys = async (sdk) => {

    const lookupQuery = gql`
		query getAllValidators {
            stakehouseAccounts(
                where:{
                    lifecycleStatus_in:["3", "4"]
                }
                first:1000
                orderBy:lifecycleStatus
            ) {
                id
                lifecycleStatus
                knotMetadata {
                  isPartOfIndex
                  savETHIndexId
                }
                depositor
            }
        }
	`;

    const subgraph = (await sdk.constants).stakehouseUrls.SUBGRAPH_ENDPOINTS;

    const response = await request(
        subgraph,
        lookupQuery
    );

    let blsPublicKeys = response.stakehouseAccounts.map(a => a.id);

    console.log("blsPublicKeys: ", blsPublicKeys);
    console.log("num of blsPublicKeys: ", blsPublicKeys.length);
    return response.stakehouseAccounts;
};

const getSweepsForBLSPublicKeys = async (sdk, validatorIndex) => {

    let slotBound;

    try {
        slotBound = await sdk.balanceReport.getStartAndEndSlotByValidatorIndex(validatorIndex);
    } catch(e) {
        throw `ETL error for ${validatorIndex}`;
    }

    const lowerSlot = slotBound[1];
    const upperSlot = slotBound[0];
    if (!lowerSlot || !upperSlot) {
        console.log('lower and upper failure for ', validatorIndex)
        console.log('l', lowerSlot)
        console.log('u', upperSlot)
        return []
    }

    const sweeps = await sdk.balanceReport.getDETHSweeps(validatorIndex, lowerSlot, upperSlot);

    return {validatorIndex, sweeps};
};

const calculateTotalRewards = async (validatorIndex) => {

    const client = axios.create({
        baseURL: `https://beaconcha.in`,
        headers: {
            'Accept': 'application/json'
        }
    });

    const response = await client.get(`/api/v1/validator/${validatorIndex}/performance`);

    return response.data.data;
};

const main = async () => {

    const provider = new ethers.providers.InfuraProvider("mainnet", {
        projectId: INFURA_PROJECT_ID,
        projectSecret: INFURA_PROJECT_SECRET
    });

    const signer = new ethers.Wallet(PRIV_KEY_1, provider);

    const sdk = new StakehouseSDK(signer);

    const blsPublicKeys = await getAllBLSPublicKeys(sdk);

    const reports = await sdk.balanceReport.getFinalisedEpochReportForMultipleBLSKeys(
        MAINNET_BEACON_NODE,
        blsPublicKeys.map(a => a.id)
    );

    reports.sort((a,b) => {
        return a.validatorIndex - b.validatorIndex;
    });

    let validatorIndices = reports[0].validatorIndex.toString();

    for (let i = 1; i < reports.length; ++i) {
        validatorIndices += ("," + reports[i].validatorIndex.toString());
    }

    const beaconChainAPIResponse = await calculateTotalRewards(validatorIndices);

    let promises = [];

    for (let i = 0; i < reports.length; ++i) {
        const promise = getSweepsForBLSPublicKeys(sdk, reports[i].validatorIndex);
        promises.push(promise);
    }

    let listOfSweeps = [];

    await Promise.allSettled(promises).then(
        async (result) => {
            for (let i = 0; i < result.length; ++i) {
                listOfSweeps.push({
                    validatorIndex: reports[i].validatorIndex,
                    blsKey: blsPublicKeys[i].id,
                    depositor: blsPublicKeys[i].depositor,
                    knotMetadata: blsPublicKeys[i].knotMetadata,
                    sweeps: result[i].value.sweeps,
                    sumOfSweeps: result[i].value.sumOfSweeps,
                    beaconAPIRewards: beaconChainAPIResponse[i].performancetotal
                });
            }
        }
    );

    // console.log("List of Sweeps: ", JSON.stringify(listOfSweeps, null, 2));
    await fs.writeFileSync('./mainnet-sweep.json', JSON.stringify(listOfSweeps, null, 2));
};

main();

import asyncio
from yaml import safe_load
from utils.logger import setup_logger
from utils.aio_calls import AioHttpCalls
from utils.decoder import Decoder
import json
import os
from tqdm import tqdm

with open('config.yaml', 'r') as config_file:
    config = safe_load(config_file)

logger = setup_logger(log_level="INFO")

decoder = Decoder(bech32_prefix=config['bech_32_prefix'], logger=logger)

async def get_validators(session):
    validators = await session.get_validators(status=None)
    if validators:
        for validator in validators:
            validator['wallet'] = decoder.convert_valoper_to_account(valoper=validator['valoper'])
            validator['valcons'] = decoder.convert_consenses_pubkey_to_valcons(consensus_pub_key=validator['consensus_pubkey'])
            validator['hex'] = decoder.conver_valcons_to_hex(valcons=validator['valcons'])
            validator['total_signed_blocks'] = 0
            validator['total_missed_blocks'] = 0
            validator['total_proposed_blocks'] = 0
        return validators

async def get_slashing_info(validators, session):
    task = [session.get_slashing_info_archive(validator['valcons']) for validator in validators]
    results = await asyncio.gather(*task)
    for validator, result in zip(validators, results):
        validator['slashing_info'] = result
    return validators

async def get_delegators_number(validators, session):
    task = [session.get_total_delegators(validator['valoper']) for validator in validators]
    results = await asyncio.gather(*task)
    for validator, result in zip(validators, results):
        validator['delegators_count'] = result
    return validators

async def get_validator_creation_info(validators, session):
    task = [session.get_validator_creation_block(validator['valoper']) for validator in validators]
    results = await asyncio.gather(*task)
    for validator, result in zip(validators, results):
        validator['creation_info'] = result
    return validators

async def check_valdiator_tomb(validators, session):
    task = [session.get_validator_tomb(validator['valcons']) for validator in validators]
    results = await asyncio.gather(*task)
    for validator, result in zip(validators, results):
        validator['tombstoned'] = result
    return validators

async def fetch_wallet_transactions(validators, session):
    task = [session.get_transactions_count(validator['wallet']) for validator in validators]
    results = await asyncio.gather(*task)
    for validator, result in zip(validators, results):
        validator['transactions'] = result
    return validators

async def parse_signatures_batches(validators, session, batch_size=1000):
    latest_indexed_height = 1
    if os.path.exists('metrics.json'):
        with open('metrics.json', 'r') as file:
            metrics_data = json.load(file)
            latest_indexed_height = metrics_data.get('latest_height', 1)

    rpc_latest_height = await session.get_latest_block_height_rpc()

    with tqdm(total=rpc_latest_height, desc="Parsing Blocks", unit="block", initial=latest_indexed_height) as pbar:

        for start_height in range(latest_indexed_height, rpc_latest_height, batch_size):
            end_height = min(start_height + batch_size, rpc_latest_height)
            
            tasks = [session.get_block(height=current_height) for current_height in range(start_height, end_height)]
            blocks = await asyncio.gather(*tasks)
            tasks = [session.get_valset_at_block(height=current_height) for current_height in range(start_height, end_height)]
            valsets = await asyncio.gather(*tasks)

            for block, valset_at_height in zip(blocks, valsets):
                for validator in validators:
                    if validator['valcons'] in valset_at_height:
                        if validator['hex'] == block['proposer']:
                            validator['total_proposed_blocks'] += 1
                        if validator['hex'] in block['signatures']:
                            validator['total_signed_blocks'] += 1
                        else:
                            validator['total_missed_blocks'] += 1
        
            metrics_data = {
                'latest_height': end_height,
                'validators': validators
            }
            with open('metrics.json', 'w') as file:
                json.dump(metrics_data, file)
            
            pbar.update(end_height - start_height)
    return validators

async def main():
    async with AioHttpCalls(config=config, logger=logger, timeout=600) as session:
        if not os.path.exists('metrics.json'):
            print('------------------------------------------------------------------------')
            logger.info('Fetching latest validators set')
            validators = await get_validators(session=session)
            print('------------------------------------------------------------------------')
            logger.info('Fetching slashing info')
            validators = await get_slashing_info(validators=validators, session=session)
            print('------------------------------------------------------------------------')
            logger.info('Fetching transactions info')
            validators = await fetch_wallet_transactions(validators=validators, session=session)
            print('------------------------------------------------------------------------')
            logger.info('Fetching delegators info')
            validators = await get_delegators_number(validators=validators, session=session)
            print('------------------------------------------------------------------------')
            logger.info('Fetching validator creation info')
            validators = await get_validator_creation_info(validators=validators, session=session)
            print('------------------------------------------------------------------------')
            logger.info('Fetching tombstones info')
            validators = await check_valdiator_tomb(validators=validators, session=session)
            print('------------------------------------------------------------------------')

            logger.info('Started indexing blocks')
            await parse_signatures_batches(validators=validators, session=session)
        else:
            with open('metrics.json', 'r') as file:
                metrics_data = json.load(file)
                validators = metrics_data.get('validators')
                print('------------------------------------------------------------------------')
                logger.info(f"Continue indexing blocks from {metrics_data.get('latest_height')}")
                await parse_signatures_batches(validators=validators, session=session)
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n------------------------------------------------------------------------')
        logger.info("The script was stopped")
        print('------------------------------------------------------------------------\n')
        exit(0)
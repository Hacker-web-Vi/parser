import asyncio
import json
import os
from math import ceil
from sys import exit
from tqdm import tqdm
from yaml import safe_load
from utils.logger import setup_logger
from utils.aio_calls import AioHttpCalls
from utils.converter import pubkey_to_consensus_hex, pubkey_to_bech32, decompress_pubkey, uncompressed_pub_key_to_evm

with open('config.yaml', 'r') as config_file:
    config = safe_load(config_file)

logger = setup_logger(log_level=config['log_lvl'])

async def get_validators(session: AioHttpCalls, exponent):
    logger.info(f"Fetched validators")
    result = []
    validators = await session.get_validators(status=None)
    if validators:
        for index, validator  in enumerate(validators, start=1):
            info = {}
            info['moniker'] = validator['moniker']
            info['wallet'] = pubkey_to_bech32(pub_key=validator['consensus_pubkey'], bech32_prefix=config['bech_32_prefix'])
            info['evm_wallet'] = uncompressed_pub_key_to_evm(public_key=decompress_pubkey(validator['consensus_pubkey']))
            info['valcons'] = pubkey_to_bech32(pub_key=validator['consensus_pubkey'], bech32_prefix=config['bech_32_prefix'], address_refix='valcons')
            info['hex'] = pubkey_to_consensus_hex(pub_key=validator['consensus_pubkey'])
            info['stake'] = round((validator['tokens'] / (10 ** exponent)), 1) if validator['tokens'] else 0.0
            info['total_signed_blocks'] = 0
            info['total_missed_blocks'] = 0
            info['total_proposed_blocks'] = 0
            info['index'] = index
            result.append(info)
        return result

async def get_slashing_info(validators, session: AioHttpCalls, total_vals, batch_size=10):
    all_validators = []
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_slashing_info_archive(validator['valcons']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, slashing_info in zip(batch, batch_results):
            slashing_info = slashing_info or []
            validator['slashes'] = slashing_info
            logger.info(f"Fetched slashes [{len(slashing_info)}] {validator['moniker'][:15].ljust(20)}[{validator['valoper'].ljust(3)}] | {validator['index']} / {total_vals}")
        
        all_validators.extend(batch)
    
    return all_validators

async def get_delegators_number(validators, session: AioHttpCalls, total_vals, batch_size=10):
    all_validators = []
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_total_delegators(validator['valoper']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, delegators in zip(batch, batch_results):
            delegators = delegators or 0
            validator['delegators_count'] = delegators
            logger.info(f"Fetched delegators [{delegators}] {validator['moniker'][:15].ljust(20)}[{validator['valoper'].ljust(3)}] | {validator['index']} / {total_vals}")
        
        all_validators.extend(batch)
    
    return all_validators

async def get_validator_self_stake(validators, session: AioHttpCalls, total_vals: int, exponent: int, batch_size=10):
    all_validators = []
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_delegator_validator_pair(valoper=validator['valoper'], wallet=validator['wallet']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, tokens in zip(batch, batch_results):
            if tokens:
                tokens_conv = round((tokens / (10 ** exponent)), 1)
            else:
                tokens_conv = 0.0
            validator['self_stake'] = tokens_conv
            logger.info(f"Fetched self stake [{tokens_conv}] {validator['moniker'][:15].ljust(20)}[{validator['valoper'].ljust(3)}] | {validator['index']} / {total_vals}")
        
        all_validators.extend(batch)
    
    return all_validators

async def check_valdiator_tomb(validators, session: AioHttpCalls, total_vals, batch_size=10):
    all_validators = []

    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_validator_tomb(validator['valcons']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, tombstoned in zip(batch, batch_results):
            tombstoned = tombstoned or False
            validator['tombstoned'] = tombstoned
            logger.info(f"Fetched tombstoned [{tombstoned}] {validator['moniker'][:15].ljust(20)}[{validator['valoper'].ljust(3)}] | {validator['index']} / {total_vals}")
        
        all_validators.extend(batch)
    
    return all_validators


async def get_block_signatures(session: AioHttpCalls, height):
    
    signatures = []
    async def fetch_with_retry(height, retries=3):
        for attempt in range(retries):
            try:
                block = await session.get_block(height=height)
                if block and 'result' in block:
                    return block
                else:
                    raise ValueError("Invalid response")
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Retrying block {height} request (attempt {attempt + 1}) due to: {e}")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"Failed to fetch block {height} after {retries} attempts.")
                    return
       
    block = await fetch_with_retry(height=height)
    if block:
        for signature in block['result']['signed_header']['commit']['signatures']:
            signatures.append(signature['validator_address'])
        proposer = block['result']['signed_header']['header']['proposer_address']
        
        return {"height": height, "signatures": signatures, "proposer": proposer}


async def get_all_valset(session: AioHttpCalls, height):
    merged_valsets = []
    page = 1
    total = 0
    count = 0

    async def fetch_with_retry(height, page, retries=3):
        for attempt in range(retries):
            try:
                sublist = await session.get_valset_at_block(height=height, page=page)
                if sublist and 'result' in sublist:
                    return sublist
                else:
                    raise ValueError("Invalid response")
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Retrying page {page} (attempt {attempt + 1}) due to: {e}")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"Failed to fetch valset page {page} after {retries} attempts.")
                    return

    while count < total or total == 0:
        sublist = await fetch_with_retry(height, page)
        
        if not sublist:
            return

        validators = sublist['result']['validators']
        merged_valsets.extend(validator['address'] for validator in validators)
        count += int(sublist['result']['count'])
        total = int(sublist['result']['total'])
        page += 1

    return merged_valsets

# async def get_all_valset(session: AioHttpCalls, height):
#     merged_valsets = []
#     page = 1
#     total = 0
#     count = 0

#     while count < total or total == 0:
#         sublist = await session.get_valset_at_block(height=height, page=page)
        
#         if not sublist or 'result' not in sublist:
#             break

#         validators = sublist['result']['validators']
#         merged_valsets.extend(validator['address'] for validator in validators)
#         count += int(sublist['result']['count'])
#         total = int(sublist['result']['total'])
#         page += 1

#     return merged_valsets

async def parse_signatures_batches(validators, session: AioHttpCalls, start_height: int, general_start_height: int, batch_size=100):

    rpc_latest_height = await session.get_latest_block_height_rpc()
    if not rpc_latest_height:
        logger.error("Failed to fetch RPC latest height. RPC is not reachable. Exiting.")
        exit(1)

    with tqdm(total=rpc_latest_height, desc="Parsing Blocks", unit="block", initial=start_height) as pbar:

        for start_height in range(start_height, rpc_latest_height, batch_size):
            end_height = min(start_height + batch_size, rpc_latest_height)

            blocks_tasks = []
            valset_tasks = []
            
            for current_height in range(start_height, end_height):
                blocks_tasks.append(get_block_signatures(session=session, height=current_height))
                valset_tasks.append(get_all_valset(session=session, height=current_height))

            blocks, valsets = await asyncio.gather(
                asyncio.gather(*blocks_tasks),
                asyncio.gather(*valset_tasks),
            )

            if config.get('sleep_between_blocks_batch_requests'):
                await asyncio.sleep(config['sleep_between_blocks_batch_requests'])

            for block, valset in zip(blocks, valsets):
                if not block:
                    logger.error(f"Failed to query {current_height} block\nMake sure block range {start_height} --> {end_height} is available on the RPC\nOr try to reduce blocks_batch_size size in config\nExiting")
                    exit(1)

                if not valset:
                    logger.error(f"Failed to query valset at block {current_height}\nMake sure block range {start_height} --> {end_height} is available on the RPC\nOr try to reduce blocks_batch_size size in config\nExiting")
                    exit(1)

                logger.debug(f"Block {current_height} | Valset {len(valset)} | Sigantures {len(block['signatures'])}")

                for validator in validators:
                    if validator['hex'] in valset:
                        if validator['hex'] == block['proposer']:
                            validator['total_proposed_blocks'] += 1
                        if validator['hex'] in block['signatures']:
                            validator['total_signed_blocks'] += 1
                        else:
                            validator['total_missed_blocks'] += 1

            metrics_data = {
                'start_height': general_start_height,
                'latest_height': end_height,
                'validators': validators
            }
            with open('metrics.json', 'w') as file:
                json.dump(metrics_data, file)

            logger.debug(f'Metrics saved. latest_height: {end_height}')
            
            if config['log_lvl'] != 'DEBUG':
                pbar.update(end_height - start_height)

async def main():
    async with AioHttpCalls(config=config, logger=logger, timeout=800) as session:
        if not os.path.exists('metrics.json'):
            logger.info('metrics.json file will be created')
            print('------------------------------------------------------------------------')
            logger.info('Fetching latest validators set')
            validators = await get_validators(session=session, exponent=config['denom_exponent'])
            if not validators:
                logger.error("Failed to fetch validators. API not reachable. Exiting")
                exit(1)
            total_vals = len(validators)
            logger.info(f'Fetched {total_vals} validators')
            if config['metrics']['jails']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching slashing info')
                validators = await get_slashing_info(validators=validators, session=session, total_vals=total_vals, batch_size=config['metrics_batch_size'])

            if config['metrics']['delegators']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching delegators info')
                validators = await get_delegators_number(validators=validators, session=session, total_vals=total_vals, batch_size=config['metrics_batch_size'])

            if config['metrics']['self_stake']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching self stake info')
                validators = await get_validator_self_stake(validators=validators, session=session, total_vals=total_vals, exponent=config['denom_exponent'], batch_size=config['metrics_batch_size'])


            if config['metrics']['tombstones']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching tombstones info')
                validators = await check_valdiator_tomb(validators=validators, session=session, total_vals=total_vals, batch_size=config['metrics_batch_size'])
                print('------------------------------------------------------------------------')
                
            if config.get('start_height') is None:
                logger.info('start_height not provided. Will try to fetch lowest height on the RPC')

            start_height = config.get('start_height', 1)
            rpc_lowest_height = await session.fetch_lowest_height()

            if rpc_lowest_height:
                if rpc_lowest_height > start_height:
                    print('------------------------------------------------------------------------')
                    logger.error(f"Config or default start height [{start_height}] < Lowest height available on the RPC [{rpc_lowest_height}]. Edit config or change RPC. Exiting")
                    exit()
            else:
                logger.error(f'Failed to check lowest block height available on the RPC. Exiting')
                exit()

            logger.info(f'Indexing blocks from block: {start_height}')
            print('------------------------------------------------------------------------')

            await parse_signatures_batches(validators=validators, session=session, start_height=start_height, batch_size=config['blocks_batch_size'], general_start_height=start_height)
        else:

            with open('metrics.json', 'r') as file:
                metrics_data = json.load(file)
                validators = metrics_data.get('validators')
                latest_indexed_height = metrics_data.get('latest_height', 1)
                print('------------------------------------------------------------------------')
                logger.info(f"Resuming indexing blocks from {metrics_data.get('latest_height')}")
                await parse_signatures_batches(validators=validators, session=session, start_height=latest_indexed_height, batch_size=config['blocks_batch_size'], general_start_height=metrics_data['start_height'])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n------------------------------------------------------------------------')
        logger.info("The script was stopped")
        print('------------------------------------------------------------------------\n')
        exit(0)

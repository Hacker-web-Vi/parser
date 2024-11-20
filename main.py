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
    validators = await session.get_validators(status=None)
    if validators:
        for index, validator  in enumerate(validators, start=1):
            validator['moniker'] = validator['moniker']
            validator['wallet'] = pubkey_to_bech32(pub_key=validator['consensus_pubkey'], bech32_prefix=config['bech_32_prefix'])
            validator['evm_wallet'] = uncompressed_pub_key_to_evm(public_key=decompress_pubkey(validator['consensus_pubkey']))
            validator['valcons'] = pubkey_to_bech32(pub_key=validator['consensus_pubkey'], bech32_prefix=config['bech_32_prefix'], address_refix='valcons')
            validator['hex'] = pubkey_to_consensus_hex(pub_key=validator['consensus_pubkey'])
            validator['stake'] = round((validator['tokens'] / (10 ** exponent)), 1) if validator['tokens'] else 0.0
            validator['total_signed_blocks'] = 0
            validator['total_missed_blocks'] = 0
            validator['total_proposed_blocks'] = 0
            validator['index'] = index
        return validators

# async def get_slashing_info(validators, session: AioHttpCalls):
#     task = [session.get_slashing_info_archive(validator['valcons']) for validator in validators]
#     results = await asyncio.gather(*task)
#     for validator, result in zip(validators, results):
#         validator['slashes'] = result
#     return validators

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

# async def get_delegators_number(validators, session: AioHttpCalls):
#     task = [session.get_total_delegators(validator['valoper']) for validator in validators]
#     results = await asyncio.gather(*task)
#     for validator, result in zip(validators, results):
#         validator['delegators_count'] = result or 0
#     return validators

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

    # for validator in validators:
    #     result = await session.get_total_delegators(validator['valoper'])
    #     await asyncio.sleep(0.5)
    #     print(f"Delegators: {result}")
    #     validator['delegators_count'] = result or 0
    # return validators

# async def get_validator_creation_info(validators, session: AioHttpCalls):
#     task = [session.get_validator_creation_block(validator['valoper']) for validator in validators]
#     results = await asyncio.gather(*task)
#     for validator, result in zip(validators, results):
#         validator['creation_info'] = result
#     return validators

# async def check_valdiator_tomb(validators, session: AioHttpCalls):
#     task = [session.get_validator_tomb(validator['valcons']) for validator in validators]
#     results = await asyncio.gather(*task)
#     for validator, result in zip(validators, results):
#         validator['tombstoned'] = result
#     return validators

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

    # for validator in validators:
    #     result = await session.get_validator_tomb(validator['valcons'])
    #     await asyncio.sleep(0.5)
    #     print(f"Tomb: {result}")
    #     validator['tombstoned'] = result or False
    # return validators

# async def fetch_wallet_transactions(validators, session: AioHttpCalls):
#     task = [session.get_transactions_count(validator['wallet']) for validator in validators]
#     results = await asyncio.gather(*task)
#     for validator, result in zip(validators, results):
#         validator['transactions'] = result
#     return validators

async def get_all_valset(session: AioHttpCalls, height, pages_to_query):
    valset_tasks = []

    for page in range(1, pages_to_query + 1):
        valset_tasks.append(session.get_valset_at_block_hex(height=height, page=page))
    valset = await asyncio.gather(*valset_tasks)
    
    merged_valsets = []

    for sublist in valset:
        if sublist is not None:
            for itm in  sublist:
                merged_valsets.append(itm)

    return merged_valsets

async def parse_signatures_batches(validators, session: AioHttpCalls, start_height: int, max_active_vals: int, batch_size=300):

    rpc_latest_height = await session.get_latest_block_height_rpc()
    if not rpc_latest_height:
        logger.error("Failed to fetch RPC latest height. RPC is not reachable. Exiting.")
        exit(1)

    pages_to_query = ceil(max_active_vals / 100)

    with tqdm(total=rpc_latest_height, desc="Parsing Blocks", unit="block", initial=start_height) as pbar:

        for start_height in range(start_height, rpc_latest_height, batch_size):
            end_height = min(start_height + batch_size, rpc_latest_height)

            blocks_tasks = []
            valset_tasks = []
            
            for current_height in range(start_height, end_height):
                blocks_tasks.append(session.get_block(height=current_height))
                # valset_tasks.append(session.get_valset_at_block_hex(height=current_height))
                valset_tasks.append(get_all_valset(session=session, height=current_height, pages_to_query=pages_to_query))

            blocks, valsets = await asyncio.gather(
                asyncio.gather(*blocks_tasks),
                asyncio.gather(*valset_tasks),
            )

            if config.get('sleep_between_blocks_batch_requests'):
                await asyncio.sleep(config['sleep_between_blocks_batch_requests'])

            for block, valset in zip(blocks, valsets):
                if block is None:
                    logger.error(f"Failed to query {current_height} block\nMake sure block range {start_height} --> {end_height} is available on the RPC\nOr try to reduce blocks_batch_size size in config\nExiting")
                    exit(1)

                if valset is None:
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

            # if config['metrics']['wallet_transactions']:
                # print('------------------------------------------------------------------------')
                # logger.info('Fetching transactions info')
                # validators = await fetch_wallet_transactions(validators=validators, session=session)

            # if config['metrics']['validator_creation_block']:
                # print('------------------------------------------------------------------------')
                # logger.info('Fetching validator creation info')
                # validators = await get_validator_creation_info(validators=validators, session=session)


             
            if config.get('start_height') is None:
                logger.info('start_height not provided. Will try to fetch lowest height on the RPC')

            start_height = config.get('start_height', 1)
            rpc_lowest_height = await session.fetch_lowest_height()

            if rpc_lowest_height:
                if rpc_lowest_height > start_height:
                    start_height = rpc_lowest_height
                    print('------------------------------------------------------------------------')
                    logger.error(f"Config or default start height [{start_height}] < Lowest height available on the RPC [{rpc_lowest_height}]. Starting from {start_height}")
            else:
                logger.error(f'Failed to check lowest block height available on the RPC [{rpc_lowest_height}]')

            logger.info(f'Indexing blocks from block: {start_height}')
            print('------------------------------------------------------------------------')

            await parse_signatures_batches(validators=validators, session=session, start_height=start_height, max_active_vals=config.get('max_active_vals', 100), batch_size=config['blocks_batch_size'])
        else:

            with open('metrics.json', 'r') as file:
                metrics_data = json.load(file)
                validators = metrics_data.get('validators')
                latest_indexed_height = metrics_data.get('latest_height', 1)
                print('------------------------------------------------------------------------')
                logger.info(f"Resuming indexing blocks from {metrics_data.get('latest_height')}")
                await parse_signatures_batches(validators=validators, session=session, start_height=latest_indexed_height, max_active_vals=config.get('max_active_vals', 100), batch_size=config['blocks_batch_size'])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n------------------------------------------------------------------------')
        logger.info("The script was stopped")
        print('------------------------------------------------------------------------\n')
        exit(0)

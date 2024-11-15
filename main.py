import asyncio
import json
import os
from sys import exit
from tqdm import tqdm
from yaml import safe_load
from utils.logger import setup_logger
from utils.aio_calls import AioHttpCalls
from utils.decoder import Decoder

with open('config.yaml', 'r') as config_file:
    config = safe_load(config_file)

logger = setup_logger(log_level=config['log_lvl'])

decoder = Decoder(bech32_prefix=config['bech_32_prefix'], logger=logger)

async def get_validators(session):
    validators = await session.get_validators(status=None)
    if validators:
        for index, validator  in enumerate(validators, start=1):
            validator['wallet'] = decoder.convert_valoper_to_account(valoper=validator['valoper'])
            validator['valcons'] = decoder.convert_consenses_pubkey_to_valcons(consensus_pub_key=validator['consensus_pubkey'])
            validator['hex'] = decoder.conver_valcons_to_hex(valcons=validator['valcons'])
            validator['total_signed_blocks'] = 0
            validator['total_missed_blocks'] = 0
            validator['total_proposed_blocks'] = 0
            validator['index'] = index
        return validators


async def get_slashing_info(validators, session, total_vals, batch_size=10):
    all_validators = []
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_slashing_info_archive(validator['valcons']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, slashing_info in zip(batch, batch_results):
            validator['slashing_info'] = slashing_info
            logger.info(f"Fetched slashing info for {validator['valoper'].ljust(3)} | {validator['index']} / {total_vals}")
        
        all_validators.extend(batch)
    
    return all_validators

async def get_delegators_number(validators, session, total_vals, batch_size=10):
    all_validators = []
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_total_delegators(validator['valoper']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, slashing_info in zip(batch, batch_results):
            validator['delegators_count'] = slashing_info
            logger.info(f"Fetched delegators info for {validator['valoper'].ljust(3)} | {validator['index']} / {total_vals}")
        
        all_validators.extend(batch)
    
    return all_validators

async def check_valdiator_tomb(validators, session, total_vals, batch_size=10):
    all_validators = []

    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_validator_tomb(validator['valcons']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, slashing_info in zip(batch, batch_results):
            validator['tombstoned'] = slashing_info
            logger.info(f"Fetched double sign info for {validator['valoper'].ljust(3)} | {validator['index']} / {total_vals}")
        
        all_validators.extend(batch)
    
    return all_validators



# async def get_validator_creation_info(validators, session):
    # task = [session.get_validator_creation_block(validator['valoper']) for validator in validators]
    # results = await asyncio.gather(*task)
    # for validator, result in zip(validators, results):
    #     validator['creation_info'] = result
    # return validators

# async def fetch_wallet_transactions(validators, session):
#     task = [session.get_transactions_count(validator['wallet']) for validator in validators]
#     results = await asyncio.gather(*task)
    # for validator, result in zip(validators, results):
        # validator['transactions'] = result
    # return validators

async def get_all_valset(session, height, max_vals):
    valset_tasks = []
    if max_vals <= 100:
        page_max = 1
    elif 100 < max_vals <= 200:
        page_max = 2
    elif 200 < max_vals <= 300:
        page_max = 3
    else:
        page_max = 4

    for page in range(1, page_max + 1):
        valset_tasks.append(session.get_valset_at_block_hex(height=height, page=page))
    valset = await asyncio.gather(*valset_tasks)
    
    merged_valsets = []

    for sublist in valset:
        if sublist is not None:
            for itm in  sublist:
                merged_valsets.append(itm)

    return merged_valsets

async def parse_signatures_batches(validators, session, start_height, batch_size=300):

    rpc_latest_height = await session.get_latest_block_height_rpc()
    if not rpc_latest_height:
        logger.error("Failed to fetch RPC latest height. RPC is not reachable. Exiting.")
        exit(1)

    with tqdm(total=rpc_latest_height, desc="Parsing Blocks", unit="block", initial=start_height) as pbar:

        for start_height in range(start_height, rpc_latest_height, batch_size):
            end_height = min(start_height + batch_size, rpc_latest_height)
            max_vals = config.get('max_number_of_valdiators_ever_in_the_active_set') or 125

            blocks_tasks = []
            valset_tasks = []
            
            for current_height in range(start_height, end_height):
                blocks_tasks.append(session.get_block(height=current_height))
                valset_tasks.append(get_all_valset(session=session, height=current_height, max_vals=max_vals))

            blocks, valsets = await asyncio.gather(
                asyncio.gather(*blocks_tasks),
                asyncio.gather(*valset_tasks),
            )

            for block, valset in zip(blocks, valsets):
                if block is None or valset is None:
                    logger.error("Failed to fetch block/valset info. Try to reduce batch size in config and restart. Exiting")
                    exit(1)

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
            
            if config['log_lvl'] != 'DEBUG':
                pbar.update(end_height - start_height)

async def main():
    async with AioHttpCalls(config=config, logger=logger, timeout=800) as session:
        if not os.path.exists('metrics.json'):
            print('------------------------------------------------------------------------')
            logger.info('Fetching latest validators set')
            validators = await get_validators(session=session)
            
            if not validators:
                logger.error("Failed to fetch validators. API is not reachable. Exiting")
                exit(1)
            
            total_vals = len(validators)

            if config['metrics']['jails']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching slashing info')
                validators = await get_slashing_info(validators=validators, session=session, total_vals=total_vals)

            if config['metrics']['delegators']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching delegators info')
                validators = await get_delegators_number(validators=validators, session=session, total_vals=total_vals)

            if config['metrics']['delegators']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching double sign info')
                validators = await check_valdiator_tomb(validators=validators, session=session, total_vals=total_vals)
            


            # if config['metrics']['validator_creation_block']:
                # print('------------------------------------------------------------------------')
                # logger.info('Fetching validator creation info')
                # validators = await get_validator_creation_info(validators=validators, session=session)

            # if config['metrics']['wallet_transactions']:
                # print('------------------------------------------------------------------------')
                # logger.info('Fetching transactions info')
                # validators = await fetch_wallet_transactions(validators=validators, session=session)
                
                
            if config.get('start_height') is None:
                logger.info(f'Start height not provided. Trying to fetch lowest height on the RPC')

            start_height = config.get('start_height', 0)
            rpc_lowest_height = await session.fetch_lowest_height()

            if rpc_lowest_height:
                if rpc_lowest_height > start_height:
                    start_height = rpc_lowest_height
                    print('------------------------------------------------------------------------')
                    logger.error(f"Config or default start height [{config.get('start_height', 0)}] < Lowest height available on the RPC [{rpc_lowest_height}]")
            else:
                logger.error(f'Failed to check lowest height available on the RPC [{rpc_lowest_height}]')

            logger.info(f'Indexing blocks from the height: {start_height}')
            print('------------------------------------------------------------------------')

            await parse_signatures_batches(validators=validators, session=session, start_height=start_height, batch_size=config['batch_size'])
        else:

            with open('metrics.json', 'r') as file:
                metrics_data = json.load(file)
                validators = metrics_data.get('validators')
                latest_indexed_height = metrics_data.get('latest_height', 1)
                print('------------------------------------------------------------------------')
                logger.info(f"Continue indexing blocks from {metrics_data.get('latest_height')}")
                await parse_signatures_batches(validators=validators, session=session, start_height=latest_indexed_height, batch_size=config['batch_size'])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n------------------------------------------------------------------------')
        logger.info("The script was stopped")
        print('------------------------------------------------------------------------\n')
        exit(0)

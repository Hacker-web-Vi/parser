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
        for i, validator  in enumerate(validators, start=1):
            info = {}
            info['moniker'] = validator['description']['moniker']
            info['valoper'] = validator['operator_address']
            info['wallet'] = pubkey_to_bech32(pub_key=validator['consensus_pubkey']['key'], bech32_prefix=config['bech_32_prefix'])
            info['evm'] = uncompressed_pub_key_to_evm(public_key=decompress_pubkey(validator['consensus_pubkey']['key']))
            info['valcons'] = pubkey_to_bech32(pub_key=validator['consensus_pubkey']['key'], bech32_prefix=config['bech_32_prefix'], address_refix='valcons')
            info['hex'] = pubkey_to_consensus_hex(pub_key=validator['consensus_pubkey']['key'])
            info['stake'] = round((float(validator['tokens']) / (10 ** exponent)), 1) if validator['tokens'] else 0.0
            info['total_signed_blocks'] = 0
            info['total_missed_blocks'] = 0
            info['total_proposed_blocks'] = 0
            info['total_mined_evm_blocks'] = 0
            info['total_processed_evm_txs'] = 0
            info['dates'] = {}
            info['i'] = i
            result.append(info)
        return result

async def get_slashing_info(validators, session: AioHttpCalls, total_vals: int, batch_size: int, sleep_time: int):
    all_validators = []
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_slashing_info_archive(validator['valcons']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, slashing_info in zip(batch, batch_results):
            if slashing_info is None:
                logger.warning(f"Got None for slashing request. Setting empty [] for {validator['moniker'][:15].ljust(20)}[{validator['valoper']}]")
                slashing_info = []

            validator['slashes'] = slashing_info
            logger.info(f"Fetched slashes [{len(slashing_info)}] {validator['moniker'][:15].ljust(20)}[{validator['valoper'].ljust(3)}] | {validator['i']} / {total_vals}")
        
        all_validators.extend(batch)

        if sleep_time:
            logger.info(f"Sleeping for {sleep_time} seconds before processing the next batch...")
            await asyncio.sleep(sleep_time)
    return all_validators

async def get_delegators_number(validators, session: AioHttpCalls, total_vals, batch_size: int, sleep_time: int):
    all_validators = []
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_total_delegators(validator['valoper']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, delegators in zip(batch, batch_results):
            if delegators is None:
                logger.warning(f"Got None for delegators request. Setting 0 for {validator['moniker'][:15].ljust(20)}[{validator['valoper']}]")
                delegators = 0

            validator['delegators_count'] = delegators
            logger.info(f"Fetched delegators [{delegators}] {validator['moniker'][:15].ljust(20)}[{validator['valoper'].ljust(3)}] | {validator['i']} / {total_vals}")
        
        all_validators.extend(batch)

        if sleep_time:
            logger.info(f"Sleeping for {sleep_time} seconds before processing the next batch...")
            await asyncio.sleep(sleep_time)
    return all_validators

async def get_validator_self_stake(validators, session: AioHttpCalls, total_vals: int, exponent: int, batch_size: int, sleep_time: int):
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
            logger.info(f"Fetched self stake [{tokens_conv}] {validator['moniker'][:15].ljust(20)}[{validator['valoper'].ljust(3)}] | {validator['i']} / {total_vals}")
        
        all_validators.extend(batch)

        if sleep_time:
            logger.info(f"Sleeping for {sleep_time} seconds before processing the next batch...")
            await asyncio.sleep(sleep_time)

    return all_validators

async def check_valdiator_tomb(validators, session: AioHttpCalls, total_vals, batch_size: int, sleep_time: int):
    all_validators = []

    for i in range(0, len(validators), batch_size):
        batch = validators[i:i + batch_size]
        batch_tasks = []
        for validator in batch:
            batch_tasks.append(session.get_validator_tomb(validator['valcons']))
        
        batch_results = await asyncio.gather(*batch_tasks)

        for validator, tombstoned in zip(batch, batch_results):
            if tombstoned is None:
                logger.warning(f"Got None for tombstone request. Setting False for {validator['moniker'][:15].ljust(20)}[{validator['valoper']}]")
                tombstoned = False

            validator['tombstoned'] = tombstoned
            logger.info(f"Fetched tombstoned [{tombstoned}] {validator['moniker'][:15].ljust(20)}[{validator['valoper'].ljust(3)}] | {validator['i']} / {total_vals}")
        
        all_validators.extend(batch)

        if sleep_time:
            logger.info(f"Sleeping for {sleep_time} seconds before processing the next batch...")
            await asyncio.sleep(sleep_time)
            
    return all_validators


async def get_block_signatures(session: AioHttpCalls, height):
    
    async def fetch_with_retry(height, retries=3):
        for attempt in range(retries):
            try:
                block = await session.get_block(height=height)
                if block and 'result' in block:
                    if attempt > 0:
                        logger.info(f"Successfully fetched block {height} after {attempt + 1} attempt(s).")

                    return block
                else:
                    raise ValueError("Invalid response")
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Retrying block {height} request (attempt {attempt + 1}) due to: {e}")
                    await asyncio.sleep(3)
                else:
                    logger.error(f"Failed to fetch block {height} after {retries} attempt(s).")
                    return
       
    block = await fetch_with_retry(height=height)
    if block:

        signed_header = block['result']['signed_header']
        signatures = [
            signature['validator_address']
            for signature in signed_header['commit']['signatures']
        ]
        proposer = signed_header['header']['proposer_address']
        block_time = signed_header['header']['time'].split('T')[0]
        return {
            "height": height,
            "signatures": signatures,
            "proposer": proposer,
            "time": block_time
        }

async def get_evm_block_data(session: AioHttpCalls, height):
    
    async def fetch_with_retry(height, retries=3):
        for attempt in range(retries):
            try:
                block = await session.get_evm_block(height=height)
                if block and 'result' in block:
                    if attempt > 0:
                        logger.info(f"Successfully fetched EVM block {height} after {attempt + 1} attempt(s).")

                    return block
                else:
                    raise ValueError("Invalid response")
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Retrying EVM block {height} request (attempt {attempt + 1}) due to: {e}")
                    await asyncio.sleep(3)
                else:
                    logger.error(f"Failed to fetch EVM block {height} after {retries} attempt(s).")
                    return
       
    block = await fetch_with_retry(height=height)
    if block:
        return {
            "height": height,
            "miner": block['result']['miner'],
            "num_tx": len(block['result']['transactions']),
        }
    
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
                    if attempt > 0:
                        logger.info(f"Successfully fetched valset at height {height} & page {page} after {attempt + 1} attempt(s).")
                    return sublist
                else:
                    raise ValueError("Invalid response")
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Retrying valset request at height {height} / page {page} (attempt {attempt + 1}) due to: {e}")
                    await asyncio.sleep(3)
                else:
                    logger.error(f"Failed to fetch valset page {page} after {retries} attempt(s).")
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

async def parse_signatures_batches(validators,
                                   session: AioHttpCalls,
                                   metrics_dir: str,
                                   start_height: int,
                                   end_height: int,
                                   batch_size: int,
                                   sleep_between_blocks_batch: int,
                                   update_bar: bool,
                                   initial_start_height: int,
                                   day_boundaries: dict
                                   ):
    os.makedirs(metrics_dir, exist_ok=True)

    if not end_height:
        end_height = await session.get_latest_block_height_rpc()
        if not end_height:
            logger.error("Failed to fetch RPC latest height. RPC is not reachable. Exiting.")
            exit(1)
    
    try:
        with tqdm(total=end_height, desc="Parsing Blocks", unit="block", initial=start_height) as pbar:

            for height in range(start_height, end_height, batch_size):
                latest_height = min(height + batch_size, end_height)

                blocks_tasks = []
                valset_tasks = []
                evm_blocks_tasks = []
                
                for current_height in range(height, latest_height):
                    blocks_tasks.append(get_block_signatures(session=session, height=current_height))
                    valset_tasks.append(get_all_valset(session=session, height=current_height))
                    evm_blocks_tasks.append(get_evm_block_data(session=session, height=current_height-1))

                blocks, valsets, evm_blocks = await asyncio.gather(
                    asyncio.gather(*blocks_tasks),
                    asyncio.gather(*valset_tasks),
                    asyncio.gather(*evm_blocks_tasks),
                )

                if sleep_between_blocks_batch:
                    await asyncio.sleep(sleep_between_blocks_batch)

                for block, valset, evm_block in zip(blocks, valsets, evm_blocks):

                    if not block:
                        logger.error(f"Failed to query {current_height} block\nMake sure block range {start_height} --> {latest_height} is available on the RPC\nOr try to reduce blocks_batch_size size in config\nExiting")
                        exit()

                    if not evm_block:
                        logger.error(f"Failed to query {current_height-1} EVM block\nMake sure block range {start_height} --> {latest_height-1} is available on the EVM RPC\nOr try to reduce blocks_batch_size size in config\nExiting")
                        exit()

                    if not valset:
                        logger.error(f"Failed to query valset at block {current_height}\nMake sure block range {start_height} --> {latest_height} is available on the RPC\nOr try to reduce blocks_batch_size size in config\nExiting")
                        exit()

                    # TO INCLUDE GENESIS FIRST BLOCK INTO THE FIRST DAY
                    if block['time'] == '2024-04-16':
                        block['time'] = '2024-10-25'

                    if block['time'] not in day_boundaries:
                        day_boundaries[block['time']] = {'start': block['height'], 'txs': 0}
                    else:
                        day_boundaries[block['time']]['txs'] += evm_block['num_tx']

                    logger.debug(f"Block {current_height} | Valset {len(valset)} | Sigantures {len(block['signatures'])}")

                    for validator in validators:
                        validator['dates'].setdefault(block['time'], {'signed_count': 0,
                                                                    'missed_count': 0,
                                                                    'proposed_count': 0,
                                                                    'mined_evm_blocks_count': 0,
                                                                    'processed_evm_tx_count': 0
                                                                    })
                        if validator['hex'] in valset:
                            if validator['hex'] == block['proposer']:
                                validator['total_proposed_blocks'] += 1
                                validator['dates'][block['time']]['proposed_count'] += 1
                        
                            if validator['evm'] == evm_block['miner']:
                                validator['total_mined_evm_blocks'] += 1
                                validator['total_processed_evm_txs'] += evm_block['num_tx']
                                validator['dates'][block['time']]['mined_evm_blocks_count'] += 1
                                validator['dates'][block['time']]['processed_evm_tx_count'] += evm_block['num_tx']

                            if validator['hex'] in block['signatures']:
                                validator['total_signed_blocks'] += 1
                                validator['dates'][block['time']]['signed_count'] += 1
                            else:
                                validator['total_missed_blocks'] += 1
                                validator['dates'][block['time']]['missed_count'] += 1

                metrics_data = {
                    'start_height': initial_start_height,
                    'latest_height': latest_height,
                    'day_boundaries': day_boundaries,
                    'validators': validators
                }
                with open(f"{metrics_dir}/metrics.json", 'w') as file:
                    json.dump(metrics_data, file)
                
                logger.debug(f'Metrics saved. latest_height: {metrics_data["latest_height"]}')

                if update_bar:
                    pbar.update(latest_height - height)

    except KeyboardInterrupt:
        logger.info("Interrupted. Saving metrics...")
    finally:
        with open(f"{metrics_dir}/metrics.json", 'w') as file:
            json.dump(metrics_data, file)
        logger.info(f'Metrics saved. Latest Height: {metrics_data["latest_height"]}')

async def main():
    async with AioHttpCalls(config=config, logger=logger, timeout=800) as session:
        if not os.path.exists(f"{config['metrics_dir']}/metrics.json"):
            logger.info('metrics.json file will be created')
            print('------------------------------------------------------------------------')
            logger.info('Fetching latest validators set')
            validators = await get_validators(session=session, exponent=config['denom_exponent'])
            if not validators:
                logger.error("Failed to fetch validators. API not reachable. Exiting")
                exit()
            total_vals = len(validators)
            logger.info(f'Fetched {total_vals} validators')
            if config['metrics']['jails']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching slashing info')
                validators = await get_slashing_info(validators=validators, session=session, total_vals=total_vals, batch_size=config['metrics_batch_size'], sleep_time = config['sleep_between_metrics_batch_requests'])

            if config['metrics']['delegators']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching delegators info')
                validators = await get_delegators_number(validators=validators, session=session, total_vals=total_vals, batch_size=config['metrics_batch_size'], sleep_time = config['sleep_between_metrics_batch_requests'])

            if config['metrics']['self_stake']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching self stake info')
                validators = await get_validator_self_stake(validators=validators, session=session, total_vals=total_vals, exponent=config['denom_exponent'], batch_size=config['metrics_batch_size'], sleep_time = config['sleep_between_metrics_batch_requests'])


            if config['metrics']['tombstones']:
                print('------------------------------------------------------------------------')
                logger.info('Fetching tombstones info')
                validators = await check_valdiator_tomb(validators=validators, session=session, total_vals=total_vals, batch_size=config['metrics_batch_size'], sleep_time = config['sleep_between_metrics_batch_requests'])
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

            await parse_signatures_batches(validators=validators,
                                           session=session,
                                           start_height=start_height,
                                           end_height=config['end_height'],
                                           metrics_dir=config['metrics_dir'],
                                           batch_size=config['blocks_batch_size'],
                                           update_bar=True if config['log_lvl'] != 'DEBUG' else False,
                                           sleep_between_blocks_batch=config['sleep_between_blocks_batch_requests'],
                                           initial_start_height=start_height,
                                           day_boundaries={}
                                           )
        else:
            with open(f"{config['metrics_dir']}/metrics.json", 'r') as file:
                metrics_data = json.load(file)
            validators = metrics_data.get('validators')
            latest_indexed_height = metrics_data.get('latest_height', 1)
            print('------------------------------------------------------------------------')
            logger.info(f"Resuming indexing blocks from {metrics_data.get('latest_height')}")
            await parse_signatures_batches(validators=validators,
                                            session=session,
                                            start_height=latest_indexed_height,
                                            end_height=config['end_height'],
                                            metrics_dir=config['metrics_dir'],
                                            batch_size=config['blocks_batch_size'],
                                            update_bar=True if config['log_lvl'] != 'DEBUG' else False,
                                            sleep_between_blocks_batch=config['sleep_between_blocks_batch_requests'],
                                            initial_start_height=metrics_data['start_height'],
                                            day_boundaries=metrics_data['day_boundaries']
                                            )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n------------------------------------------------------------------------')
        logger.info("The script was stopped")
        print('------------------------------------------------------------------------\n')
        exit(0)

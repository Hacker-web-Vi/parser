
import asyncio
import json
import os
from sys import exit
from tqdm import tqdm
from yaml import safe_load
from utils.logger import setup_logger
from utils.aio_calls import AioHttpCalls
from utils.decoder import Decoder
from utils.extension_parser import ExtensionParser
from concurrent.futures import ThreadPoolExecutor
import time

with open('config.yaml', 'r') as config_file:
    config = safe_load(config_file)

logger = setup_logger(log_level=config['log_lvl'])

decoder = Decoder(bech32_prefix=config['bech_32_prefix'], logger=logger)

extension_parser = ExtensionParser(logger=logger)

def process_extension(tx: str):
    try:

        extension_validators = extension_parser.parse_votes_extension(tx=tx)
        data = {}
        for validator in extension_validators:
            valcons = decoder.convert_consenses_pubkey_to_valcons(consensus_pub_key=None, address_data=validator['validator_address'])
            hex = decoder.conver_valcons_to_hex(valcons=valcons)
            data[hex] = 1 if validator['pairs'] else 0

        return data
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

async def process_extensions_batch(txs):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        futures = [loop.run_in_executor(executor, process_extension, tx) for tx in txs]
        return await asyncio.gather(*futures)
    
async def main(start_height = 1849095, batch_size=100):
    async with AioHttpCalls(config = config, logger = logger, timeout=800) as session:
        rpc_latest_height = await session.get_latest_block_height_rpc()

        if not rpc_latest_height:
            logger.error("Failed to fetch RPC latest height. RPC is not reachable. Exiting.")
            exit(1)

        for start_height in range(start_height, rpc_latest_height, batch_size):
            end_height = min(start_height + batch_size, rpc_latest_height)

            tx_tasks = []
            
            for current_height in range(start_height, end_height):
                tx_tasks.append(session.get_extension_tx(height=current_height - 1)) 
            
            txs = await asyncio.gather(*tx_tasks)

            
            start_time = time.time()
            parsed_extensions = [process_extension(tx) for tx in txs]
            # parsed_extensions = await process_extensions_batch(txs= txs)
            end_time = time.time()

            print(f"Batch processed: {len(parsed_extensions)} transactions in {end_time - start_time:.2f} seconds")



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n------------------------------------------------------------------------')
        logger.info("The script was stopped")
        print('------------------------------------------------------------------------\n')
        exit(0)
import aiohttp
import traceback
import time

class AioHttpCalls:

    def __init__(
                 self,
                 config,
                 logger,
                 timeout = 10
                 ):
                 
        self.api = config['api']
        self.rpc = config['rpc']
        self.logger = logger
        self.timeout = timeout
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.session.close()
    
    async def handle_request(self, url, callback, include_latency=False):
        try:
            start_time = time.time()
            async with self.session.get(url, timeout=self.timeout) as response:
                end_time = time.time()
                
                if 200 <= response.status < 300:
                    data = await callback(response.json())
                    if include_latency:
                        data['latency'] = round(end_time - start_time, 2)
                    return data
                
                elif response.status == 500 and '/block?height=1' in url:
                    data = await callback(response.json())
                    return data
        
                else:
                    self.logger.debug(f"Request to {url} failed with status code {response.status}")
                    return None
                
        except aiohttp.ClientError as e:
            self.logger.debug(f"Issue with making request to {url}: {e}")
            return None
        
        except TimeoutError as e:
            self.logger.debug(f"Issue with making request to {url}. TimeoutError: {e}")
            return None

        except Exception as e:
            self.logger.debug(f"An unexpected error occurred: {e}")
            traceback.print_exc()
            return None

    async def get_latest_block_height_rpc(self) -> str:
        url = f"{self.rpc}/abci_info"

        async def process_response(response):
            data = await response
            return int(data.get('result', {}).get('response', {}).get('last_block_height'))
        
        return await self.handle_request(url, process_response)

    async def get_total_delegators(self, valoper: str) -> str:
        url = f"{self.api}/cosmos/staking/v1beta1/validators/{valoper}/delegations?pagination.count_total=true"
        
        async def process_response(response):
            data = await response
            return int(data.get('pagination', {}).get('total', 0))
        
        return await self.handle_request(url, process_response)
    
    async def get_validator_tomb(self, valcons: str) -> dict:
        url = f"{self.api}/cosmos/slashing/v1beta1/signing_infos/{valcons}"

        async def process_response(response):
            data = await response
            return data.get('val_signing_info',{}).get('tombstoned', False)

        return await self.handle_request(url, process_response) 
    

    async def get_delegator_validator_pair(self, valoper: str, wallet: str) -> str:
        url = f"{self.api}/cosmos/staking/v1beta1/validators/{valoper}/delegations/{wallet}"
        
        async def process_response(response):
            data = await response
            return float(data.get('delegation_response', {}).get('balance', {}).get('amount', 0))
        
        return await self.handle_request(url, process_response)

    async def get_validators(self, status: str = None) -> dict:
        status_urls = {
            "BOND_STATUS_BONDED": f"{self.api}/cosmos/staking/v1beta1/validators?status=BOND_STATUS_BONDED&pagination.limit=100000",
            "BOND_STATUS_UNBONDED": f"{self.api}/cosmos/staking/v1beta1/validators?status=BOND_STATUS_UNBONDED&pagination.limit=100000",
            "BOND_STATUS_UNBONDING": f"{self.api}/cosmos/staking/v1beta1/validators?status=BOND_STATUS_UNBONDING&pagination.limit=100000",
            None: f"{self.api}/cosmos/staking/v1beta1/validators?&pagination.limit=100000"
        }
        url = status_urls.get(status, status_urls[None])
        async def process_response(response):
            data = await response
            validators = []
            for validator in data['validators']:
                info = {'valoper': validator.get('operator_address'),
                        'consensus_pubkey': validator.get('consensus_pubkey',{}).get('key'),
                        'moniker': validator.get('description',{}).get('moniker'),
                        'tokens': float(validator.get('tokens', 0.0))}
                
                validators.append(info)
            return validators
        
        return await self.handle_request(url, process_response)
    
    async def get_slashing_info_archive(self, valcons: str):
        url = f'{self.rpc}/block_search?query="slash.address%3D%27{valcons}%27"'
        
        async def process_response(response):
            blocks = []
            data = await response
            if data.get('result',{}).get('blocks'):
                for block in data['result']['blocks']:
                    blocks.append({'height': block.get('block',{}).get('header',{}).get('height'), 'time': block.get('block',{}).get('header',{}).get('time')})
                return blocks
        return await self.handle_request(url, process_response)
    
    async def get_block(self, height):
        url = f"{self.rpc}/commit?height={height}"

        async def process_response(response):
            data = await response
            return data
        return await self.handle_request(url, process_response)
    
    async def get_valset_at_block(self, height, page):
        url = f"{self.rpc}/validators?height={height}&page={page}&per_page=100"
        self.logger.debug(f"Requesting valset at block {height} & page {page}")

        async def process_response(response):
            data = await response
            return data
        
        return await self.handle_request(url, process_response)
    
    async def fetch_lowest_height(self) -> int:
        url = f"{self.rpc}/block?height=1"

        async def process_response(response):
            data = await response

            error_data = data.get("error")
            if error_data:
                error_message = error_data.get("data", None)
                if error_message:
                    return int(error_message.split()[-1])
            
            return int(data.get("result", {}).get("block", {}).get("header", {}).get("height"))
                
        return await self.handle_request(url, process_response)
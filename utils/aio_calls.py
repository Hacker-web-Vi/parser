import asyncio
import aiohttp
import traceback
import time
import json

class AioHttpCalls:

    def __init__(
                 self,
                 config,
                 logger,
                 timeout = 10
                 ):
                 
        self.api = config['api']
        self.rpc = config['rpc']
        self.stake_denom = config['stake_denom']
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
                
                elif response.status == 501 and '/cosmos/bank/v1beta1/supply/' in url and '/by_denom?' not in url:
                    data = await self.get_supply_by_denom()
                    self.logger.debug(f"Request to {url} failed with status code {response.status}. Trying get_supply_by_denom")
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
    
    async def get_active_wallets(self):
        url = f"{self.api}/cosmos/auth/v1beta1/accounts?pagination.limit=10&pagination.count_total=true"
        
        async def process_response(response):
            data = await response
            return data.get('pagination', {}).get('total')
        
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
    
    async def get_validator_creation_block(self, valoper: str) -> dict:
        url=f"{self.api}/cosmos/tx/v1beta1/txs?events=create_validator.validator%3D%27{valoper}%27"

        async def process_response(response):
            data = await response
            if data.get('tx_responses', []):
                return {'block': data.get('tx_responses',[{}])[0].get('height'),'time': data.get('tx_responses',[{}])[0].get('timestamp'), 'txhash': data.get('tx_responses',[{}])[0].get('txhash')}

        return await self.handle_request(url, process_response)

    async def get_transactions_count(self, wallet: str) -> dict:
        url=f"{self.rpc}/tx_search?query=%22message.sender=%27{wallet}%27%22"

        async def process_response(response):
            data = await response
            status_0_count = 0
            other_status_count = 0
            governance_participation = False
            if data.get('result',{}).get('txs', []):
                for tx in data["result"]["txs"]:
                    if tx["tx_result"]["code"] == 0:
                        status_0_count += 1
                        log_entries = json.loads(tx["tx_result"]["log"])
                        for entry in log_entries:
                            events = entry.get("events", [])
                            if events:
                                for event in events:
                                    attributes = event.get("attributes", [])
                                    if attributes:
                                        for attr in attributes:
                                            if attr.get("value") == "/cosmos.gov.v1.MsgVote":
                                                governance_participation = True
                    else:
                        other_status_count += 1


                return {'successful': status_0_count,'failed': other_status_count, 'total': data.get('result',{}).get('total_count', 0), 'gov_participate': governance_participation}

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
                info = {'moniker': validator.get('description',{}).get('moniker'),
                        'valoper': validator.get('operator_address'),
                        'commission': validator.get('commission', {}).get('commission_rates', {}).get('rate', '0.0'),
                        'consensus_pubkey': validator.get('consensus_pubkey',{}).get('key')}
                
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
    
    async def get_valset_at_block(self, height):
        url = f"{self.api}/cosmos/base/tendermint/v1beta1/validatorsets/{height}?&pagination.limit=100000"
        
        async def process_response(response):
            data = await response
            valcons = []
            for validator in data['validators']:
                valcons.append(validator['address'])
            return valcons
        return await self.handle_request(url, process_response)
    
    async def get_block(self, height):
        url = f"{self.rpc}/commit?height={height}"

        async def process_response(response):
            data = await response
            signatures = []
        
            for signature in data['result']['signed_header']['commit']['signatures']:
                signatures.append(signature['validator_address'])
            proposer = data['result']['signed_header']['header']['proposer_address']
            
            return {"height": height, "signatures": signatures, "proposer": proposer}

        return await self.handle_request(url, process_response)
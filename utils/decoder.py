import base64
from hashlib import sha256
import bech32

class Decoder:
    def __init__(self, bech32_prefix : str, logger):
        self.bech32_prefix = bech32_prefix
        self.logger = logger

    def convert_consenses_pubkey_to_valcons(self, consensus_pub_key: str, bech32_prefix: str = None) -> str:
        try:
            bech32_prefix = bech32_prefix or self.bech32_prefix
            pubkey_raw = base64.b64decode(consensus_pub_key)
            address_data = sha256(pubkey_raw).digest()[:20]
            data = bech32.convertbits(address_data, 8, 5)
            bech32_address = bech32.bech32_encode(f"{bech32_prefix}valcons", data)
            return bech32_address
                
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")
            return None

    def conver_valcons_to_hex(self, valcons: str) -> str:
        try:
            hrp, data = bech32.bech32_decode(valcons)
            witness = bech32.convertbits(data, 5, 8, False)
            hex_address = ''.join(format(byte, '02x') for byte in witness).upper()
            return hex_address
        except Exception as e:
                self.logger.error(f"An unexpected error occurred: {e}")
                return None

    def convert_valoper_to_account(self, valoper: str) -> str:
        try:
            prefix, words = bech32.bech32_decode(valoper)
            account_prefix = prefix.replace("valoper", "")
            account_address = bech32.bech32_encode(account_prefix, words)
            return account_address
        
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")
            return None
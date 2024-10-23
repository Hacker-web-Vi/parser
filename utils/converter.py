from base64 import b64decode
from ecdsa import SECP256k1, VerifyingKey
from web3 import Web3
from eth_utils import to_checksum_address
from hashlib import sha256
from bech32 import bech32_encode, convertbits
from Crypto.Hash import RIPEMD160

def decompress_pubkey(pub_key):
    pubkey_bytes = b64decode(pub_key)
    if len(pubkey_bytes) != 33:
        raise ValueError("Public key is not compressed or has invalid length")
    vk = VerifyingKey.from_string(pubkey_bytes, curve=SECP256k1)
    return vk.pubkey.point.to_bytes()

def uncompressed_pub_key_to_evm(public_key):
    keccak_hash = Web3().keccak(public_key)
    return to_checksum_address('0x' + keccak_hash[-20:].hex())

def pubkey_to_bech32(pub_key, bech32_prefix, address_refix = ""):
        pubkey_bytes = b64decode(pub_key)
        sha256_digest = sha256(pubkey_bytes).digest()
        ripemd160 = RIPEMD160.new()
        ripemd160.update(sha256_digest)
        ripemd160_digest = ripemd160.digest()
        converted_bits = convertbits(ripemd160_digest, 8, 5)
        return bech32_encode(f"{bech32_prefix+address_refix}", converted_bits)

def pubkey_to_consensus_hex(pub_key):
    pubkey_bytes = b64decode(pub_key)
    sha256_digest = sha256(pubkey_bytes).digest()
    ripemd160 = RIPEMD160.new()
    ripemd160.update(sha256_digest)
    ripemd160_digest = ripemd160.digest()

    consensus_hex = ''.join(format(byte, '02x') for byte in ripemd160_digest).upper()

    return consensus_hex
"""
CPSC 5520, Seattle University
Lab 5: Blockchain
Author: Sai Lavanya Kanakam

Execution details: Please use following cmds to run the script,

CMD (full lab5 specifications):
python3 lab5.py
python3 lab5.py > skanakam_output

CMD (skip downloading block headers, use hardcoded header hash for block#489370, download full block, modify transaction and publish):
python3 lab5.py skip_blockheader_download

Description: This script performs the following,
1. Tries to connect to a hardcoded peer endpoint.
2. Falls back to reading 'nodes_main.txt' file in the current directory for list of endpoints (excl onion nodes) and tries to establish a connection.
3. Upon successful connection with a sync node, we then initiate version message exchange.
4. Messages handled during initiation: version, verack, sendheaders, sendcmpct, feefilter, addr, ping, pong
5. It then downloads full list of block headers (~ 608K) in an iterative batch download with 2K headers per batch.
6. It also auto-reconnects for certain retry attempts upon connection failure during this process and re-initiates version messages.
7. It then downloads "489370" block using the header hash from previous fetches.
8. Upon block download we print entire list of transactions (~ 2462 Txs).
9. It then changes the output address of one of the transaction in this block and re-calculates the merkle root hash.
10. Then sends the block message to the sync node.
11. Finally, it processes the REJECT message sent by sync node and exits.

Stats:
Total block headers downloaded = ~ 608000
Total time to download entire block headers = ~ 62 mins
Total number of transactions downloaded and printed in Block#489370 = 2462 Txs
Total time to download Block#489370 = 72 secs
Total time to taken by the script = ~ 65 mins 
"""
import sys
import socket
from datetime import datetime
from time import strftime, gmtime
import time
import hashlib
import copy

DEFAULT_PEER_IP_FILE_PATH = 'nodes_main.txt'
MICROS_PER_SECOND = 1_000_000
HDR_SZ = 24
# Messages exchanged between nodes
CMD_VERSION = 'version'
CMD_VERACK = 'verack'
CMD_SENDCMPCT = 'sendcmpct'
CMD_PING = 'ping'
CMD_PONG = 'pong'
CMD_FEEFILTER = 'feefilter'
CMD_ADDR = 'addr'
CMD_GETHEADERS = 'getheaders'
CMD_HEADERS = 'headers'
CMD_GETDATA = 'getdata'
CMD_BLOCK = 'block'
CMD_INV = 'inv'
CMD_REJECT = 'reject'
MY_SU_ID = 4089370
COIN = 100000000  # 1 btc in satoshis
BLOCK_HEADERS = []
MAX_SOCK_AUTO_RECONNECT = 10
# Tx OP CODES for PubKey and Sign Scripts validations. Used in fetching output address from a Tx Output.
BYTES_TO_PUSH_HASH = '14'
OP_DUP_HEX = '76'
OP_HASH160_HEX = 'a9'
OP_EQUAL_HEX = '87'
OP_EQUALVERIFY = '88'
OP_CHECKSIG = 'ac'
P2SH_ADDR_OP_CODES_PREFIX_HEX = OP_HASH160_HEX + BYTES_TO_PUSH_HASH
P2SH_ADDR_OP_CODES_SUFFIX_HEX = OP_EQUAL_HEX
P2PKH_ADDR_OP_CODES_PREFIX_HEX = OP_DUP_HEX + OP_HASH160_HEX + BYTES_TO_PUSH_HASH
P2PKH_ADDR_OP_CODES_SUFFIX_HEX = OP_EQUALVERIFY + OP_CHECKSIG

def compactsize_t(n):
    '''
    Serializing variable-length integers used in messages
    Refer: https://bitcoin.org/en/developer-reference#compactsize-unsigned-integers
    '''
    if n <= 252:
        return uint8_t(n)
    if n <= 0xffff:
        return uint8_t(0xfd) + uint16_t(n)
    if n <= 0xffffffff:
        return uint8_t(0xfe) + uint32_t(n)
    return uint8_t(0xff) + uint64_t(n)

def unmarshal_compactsize(b):
    '''
    Deserializing variable-length integers used in messages
    Refer: https://bitcoin.org/en/developer-reference#compactsize-unsigned-integers
    '''
    key = b[0]
    if key == 0xff:
        return b[0:9], unmarshal_uint(b[1:9])
    if key == 0xfe:
        return b[0:5], unmarshal_uint(b[1:5])
    if key == 0xfd:
        return b[0:3], unmarshal_uint(b[1:3])
    
    return b[0:1], unmarshal_uint(b[0:1])

def bool_t(flag):
    return uint8_t(1 if flag else 0)

def to_bool(i_uint8):
    return True if unmarshal_uint(i_uint8) == 1 else False

def ipv6_from_ipv4(ipv4_str):
    pchIPv4 = bytearray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff])
    return pchIPv4 + bytearray((int(x) for x in ipv4_str.split('.')))

def rev_endian(string):
    return string
    #return ''.join(reversed([string[i:i+2] for i in range(0, len(string), 2)]))

def ipv6_to_ipv4(ipv6):
    return '.'.join([str(b) for b in ipv6[12:]])

def uint8_t(n):
    return int(n).to_bytes(1, byteorder='little', signed=False)

def uint16_t(n):
    return int(n).to_bytes(2, byteorder='little', signed=False)

def int32_t(n):
    return int(n).to_bytes(4, byteorder='little', signed=True)

def uint32_t(n):
    return int(n).to_bytes(4, byteorder='little', signed=False)

def int64_t(n):
    return int(n).to_bytes(8, byteorder='little', signed=True)

def uint64_t(n):
    return int(n).to_bytes(8, byteorder='little', signed=False)

def unmarshal_int(b):
    return int.from_bytes(b, byteorder='little', signed=True)

def unmarshal_uint(b):
    return int.from_bytes(b, byteorder='little', signed=False)

def double_sha256(payload):
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()

def checksum(payload):
    return double_sha256(payload)[:4]

def print_message(msg, text=None, skip_printing_payload = False):
    """
    Report the contents of the given bitcoin message
    :param msg: bitcoin message including header
    :return: Tuple(command name, result)
    """
    print('\n{}MESSAGE'.format('' if text is None else (text + ' ')))
    print('({}) {}'.format(len(msg), msg[:60].hex() + ('' if len(msg) < 60 else '...')))
    payload = msg[HDR_SZ:]
    command, is_valid_cksum = print_header(msg[:HDR_SZ], checksum(payload))
    msg_kv = {} # Only parsing and returning msgs which the receiver needs to act on
    
    if not is_valid_cksum or skip_printing_payload:
        return command, msg_kv
    
    if command == CMD_VERSION:
        print_version_msg(payload)
    elif command == CMD_SENDCMPCT:
        msg_kv = print_sendcmpct_msg(payload)
    elif command == CMD_PING:
        msg_kv = print_eightbyte_uint64_msg(payload, CMD_PING, 'nonce')
    elif command == CMD_PONG:
        print_eightbyte_uint64_msg(payload, CMD_PONG, 'nonce')
    elif command == CMD_FEEFILTER:
        msg_kv = print_eightbyte_uint64_msg(payload, CMD_FEEFILTER, 'feerate')
    elif command == CMD_ADDR:
        print_addr_msg(payload)
    elif command == CMD_GETHEADERS:
        print_get_headers_msg(payload)
    elif command == CMD_HEADERS:
        print_headers_msg(payload)
    elif command == CMD_GETDATA:
        print_get_data_msg(payload, CMD_GETDATA)
    elif command == CMD_BLOCK:
        msg_kv = print_block_msg(payload)
    elif command == CMD_REJECT:
        print_reject_msg(payload)
    elif command == CMD_INV:
        print_get_data_msg(payload, CMD_INV)
    elif len(payload) > 0:
        print('Unknown command. Payload = {}'.format(payload))
    return command, msg_kv

def print_reject_msg(b):
    '''
    Deserializes and prints REJECT message payload.
    Refer: https://bitcoin.org/en/developer-reference#reject
    Example msg:
    
    REJECT
    --------------------------------------------------------
    05                               message_bytes 5
    626c6f636b                       message block
    10                               code
    09                               reason_bytes 9
    686967682d68617368               reason high-hash
    c3a424b0fa7b8332fb73c65bb64daf794b541f114b90cd2b2200f004b342a252 extra data
    '''
    message_bytes, msg_size = unmarshal_compactsize(b)
    start_index = len(message_bytes)
    message = b[start_index:start_index+msg_size]
    start_index += msg_size
    code = b[start_index:start_index+1]
    start_index += 1
    reason_bytes, reason_size = unmarshal_compactsize(b[start_index:])
    start_index += len(reason_bytes)
    reason = b[start_index:start_index+reason_size]
    start_index += reason_size
    extra_data = b[start_index:]

    # print report
    prefix = '  '
    print(prefix + 'REJECT')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} message_bytes {}'.format(prefix, message_bytes.hex(), msg_size))
    print('{}{:32} message {}'.format(prefix, message.hex(), message.decode('utf-8')))
    print('{}{:32} code'.format(prefix, code.hex()))
    print('{}{:32} reason_bytes {}'.format(prefix, reason_bytes.hex(), reason_size))
    print('{}{:32} reason {}'.format(prefix, reason.hex(), reason.decode('utf-8')))
    print('{}{:32} extra data'.format(prefix, extra_data.hex()))

def print_block_msg(b):
    '''
    Deserializes and prints block message payload. Received from sync node in response to getdata message.
    :param msg: serialized block message payload
    :return: deserialized block object as part of the result map
    
    Refer: https://bitcoin.org/en/developer-reference#serialized-blocks
    '''
    block = Block()
    block.deserialize(b)
    # print report
    prefix = '  '
    print(prefix + 'BLOCK')
    print(prefix + '-' * 56)
    prefix *= 2
    # Block header
    print('{}{:32} version {}'.format(prefix, int32_t(block.b_version).hex(), block.b_version))
    print('{}{:32} prev block header hash'.format(prefix, block.hash_prev_block))
    print('{}{:32} merkle root hash'.format(prefix, block.hash_merkle_root))
    print('{}{:32} time {}'.format(prefix, uint32_t(block.b_time).hex(), block.b_time))
    print('{}{:32} nBits {}'.format(prefix, uint32_t(block.nbits).hex(), block.nbits))
    print('{}{:32} nonce {}'.format(prefix, uint32_t(block.nonce).hex(), block.nonce))
    # txn_count
    print('{}{:32} txn count {}'.format(prefix, compactsize_t(len(block.transactions)).hex(), len(block.transactions)))
    # txns
    tx_cnt = 1
    txprefix = prefix * 2
    for tx in block.transactions:
        print('\n{}Transaction#{}:'.format(prefix, tx_cnt))
        print(prefix + '-' * 20)
        print('{}{:32} tx version {}'.format(prefix, int32_t(tx.tx_version).hex(), tx.tx_version))
        print('{}{:32} tx_in count {}'.format(prefix, compactsize_t(len(tx.tx_in)).hex(), len(tx.tx_in)))
        tx_in_cnt = 1
        for txin in tx.tx_in:
            print('{}TxInput#{}:'.format(prefix, tx_in_cnt))
            print(prefix + '-' * 10)
            print('{}{:32} prev output hash TXID'.format(txprefix, txin.prev_out.hash))
            print('{}{:32} output index {}'.format(txprefix, uint32_t(txin.prev_out.index).hex(), txin.prev_out.index))
            print('{}{:32} script bytes {}'.format(txprefix, compactsize_t(len(txin.script_sig)).hex(), len(txin.script_sig)))
            print('{}{:32} signature script'.format(txprefix, txin.script_sig))
            tx_in_cnt += 1
        print('{}{:32} tx_out count {}'.format(prefix, compactsize_t(len(tx.tx_out)).hex(), len(tx.tx_out)))
        tx_out_cnt = 1
        for txout in tx.tx_out:
            print('{}TxOutput#{}:'.format(prefix, tx_out_cnt))
            print_transaction_output(txout, txprefix, prefix)
            tx_out_cnt += 1
        print('{}{:32} lock_time {}'.format(prefix, uint32_t(tx.lock_time).hex(), tx.lock_time))
        tx_cnt += 1
    print('{}{:32} Again Total txn count {}'.format(prefix, compactsize_t(len(block.transactions)).hex(), len(block.transactions)))
    return {'block': block}

def print_transaction_output(txout, txprefix, prefix):
    '''
    Deserializes and prints transaction output. Also used while printing the modified output transaction.
    :param msg: serialized tx output payload
    Refer: https://bitcoin.org/en/developer-reference#serialized-blocks
    '''
    print(prefix + '-' * 10)
    print('{}{:32} value (satoshis) {}'.format(txprefix, int64_t(txout.value).hex(), txout.value))
    print('{}{:32} pk_script bytes {}'.format(txprefix, compactsize_t(len(txout.pk_script)).hex(), len(txout.pk_script)))
    print('{}{:32} pk_script'.format(txprefix, txout.pk_script))

def print_get_data_msg(b, cmd):
    '''
    Deserializes and prints getdata message payload. Sent to sync node to request full block download.
    :param msg: serialized getdata message payload
    Refer: https://bitcoin.org/en/developer-reference#getdata
    '''
    count_bytes, count = unmarshal_compactsize(b)
    
    # print report
    prefix = '  '
    print(prefix + cmd.upper())
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} count {}'.format(prefix, count_bytes.hex(), count))
    start_index = len(count_bytes)
    for i in range(start_index, count*36, 36):
        print('{}{:32} type {}'.format(prefix, b[i:i+4].hex(), unmarshal_uint(b[i:i+4])))
        print('{}{:32} hash'.format(prefix, b[i+4:i+36].hex()))

def print_headers_msg(b):
    '''
    Deserializes and prints headers message payload. Received from sync node containing list of block headers.
    :param msg: serialized headers message payload
    Refer: https://bitcoin.org/en/developer-reference#headers
    '''
    cnt_bytes, count = unmarshal_compactsize(b)
    start_index = len(cnt_bytes)
    #block_headers = []
    num_headers = 1
    # print report
    prefix = '  '
    print(prefix + 'HEADERS')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} count {}'.format(prefix, cnt_bytes.hex(), count))
    print('{}Printing top 2 and last 2 hashes of latest fetch:'.format(prefix))
    for i in range(start_index, count*81, 81):
        b_h = BlockHeader()
        b_h.deserialize(b[i:i+80])
        BLOCK_HEADERS.append(b_h)
        if num_headers <= 2 or num_headers > count-2:
            print('{}{:64} block header#{}'.format(prefix, bytearray.fromhex(b_h.get_header_hash())[::-1].hex(), len(BLOCK_HEADERS) - 1)) # changing to big endian
        elif num_headers == 3:
            print('{}{}'.format(prefix, '.' * 10))
        num_headers += 1

def print_get_headers_msg(b):
    '''
    Deserializes and prints getheaders message payload. This message requests sync node to send list of headers (2K per req).
    :param msg: serialized headers message payload
    Refer: https://bitcoin.org/en/developer-reference#getheaders
    '''
    start_index = 4
    ver = b[:start_index]
    hash_cnt_hex, hash_cnt = unmarshal_compactsize(b[start_index:])
    #stop_hash = b[len(b)-32:]
    start_index += len(hash_cnt_hex)
    # print report
    prefix = '  '
    print(prefix + 'GETHEADERS')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} version {}'.format(prefix, ver.hex(), unmarshal_uint(ver)))
    print('{}{:32} hash count {}'.format(prefix, hash_cnt_hex.hex(), hash_cnt))
    for i in range(start_index, start_index + hash_cnt*32, 32):
        print('{}{:32} block header hashes'.format(prefix, b[i:i+32].hex()))
    start_index = start_index+hash_cnt*32
    print('{}{:32} stop hash'.format(prefix, b[start_index:start_index+32].hex()))

def print_eightbyte_uint64_msg(b, cmd_name, field_name):
    '''
    Deserializes and prints feefilter, ping and pong message payload.
    :param msg: serialized feefilter/ping/pong message payload
    :return map of result containing deserialized payload
    Refer: https://bitcoin.org/en/developer-reference#feefilter
    '''
    val = b[:8]
    # print report
    prefix = '  '
    print(prefix + cmd_name.upper())
    print(prefix + '-' * 56)
    prefix *= 2
    deserialized_val = unmarshal_uint(val)
    print('{}{:32} {} {}'.format(prefix, val.hex(), field_name, deserialized_val))
    return {field_name: deserialized_val}

def print_sendcmpct_msg(b):
    """
    Report the contents of the given bitcoin sendcmpct message (sans the header).
    :param payload: sendcmpct message contents
    :return map of result containing deserialized payload
    Refer: https://bitcoin.org/en/developer-reference#sendcmpct
    """
    announce, version = b[:1], b[1:]
    # print report
    prefix = '  '
    print(prefix + 'SENDCMPCT')
    print(prefix + '-' * 56)
    prefix *= 2
    b_a = to_bool(announce)
    deserialized_val = unmarshal_int(version)
    print('{}{:32} announce {}'.format(prefix, announce.hex(), b_a))
    print('{}{:32} version {}'.format(prefix, version.hex(), deserialized_val))
    return {'announce': b_a, 'version': deserialized_val}

def print_addr_msg(b):
    '''
    Deserializes and prints addr message payload.
    :param msg: serialized addr message payload
    Refer: https://bitcoin.org/en/developer-reference#addr
    '''
    ip_addr_cnt_size = len(b) % 30 # 30 bytes corresponds to 1 network IP address entry
    ip_addr_cnt = unmarshal_uint(b[:ip_addr_cnt_size])
    nw_ip_addrs = [b[i:i+30] for i in range(ip_addr_cnt, len(b), 30)]
    # print report
    prefix = '  '
    print(prefix + 'ADDR')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} IP address count {}'.format(prefix, b[:ip_addr_cnt_size].hex(), ip_addr_cnt))
    for nw_ip in nw_ip_addrs:
        epoch_time, your_services, rec_host, rec_port = nw_ip[:4], nw_ip[4:12], nw_ip[12:28], nw_ip[28:30]
        time_str = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime(unmarshal_int(epoch_time)))
        print('{}{:32} epoch time {}'.format(prefix, epoch_time.hex(), time_str))
        print('{}{:32} your services'.format(prefix, your_services.hex()))
        print('{}{:32} your host {}'.format(prefix, rec_host.hex(), ipv6_to_ipv4(rec_host)))
        print('{}{:32} your port {}'.format(prefix, rec_port.hex(), unmarshal_uint(rec_port)))

def print_version_msg(b):
    """
    Report the contents of the given bitcoin version message (sans the header)
    :param payload: version message contents
    """
    # pull out fields
    version, my_services, epoch_time, your_services = b[:4], b[4:12], b[12:20], b[20:28]
    rec_host, rec_port, my_services2, my_host, my_port = b[28:44], b[44:46], b[46:54], b[54:70], b[70:72]
    nonce = b[72:80]
    user_agent_size, uasz = unmarshal_compactsize(b[80:])
    i = 80 + len(user_agent_size)
    user_agent = b[i:i + uasz]
    i += uasz
    start_height, relay = b[i:i + 4], b[i + 4:i + 5]
    extra = b[i + 5:]

    # print report
    prefix = '  '
    print(prefix + 'VERSION')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} version {}'.format(prefix, version.hex(), unmarshal_int(version)))
    print('{}{:32} my services'.format(prefix, my_services.hex()))
    time_str = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime(unmarshal_int(epoch_time)))
    print('{}{:32} epoch time {}'.format(prefix, epoch_time.hex(), time_str))
    print('{}{:32} your services'.format(prefix, your_services.hex()))
    print('{}{:32} your host {}'.format(prefix, rec_host.hex(), ipv6_to_ipv4(rec_host)))
    print('{}{:32} your port {}'.format(prefix, rec_port.hex(), unmarshal_uint(rec_port)))
    print('{}{:32} my services (again)'.format(prefix, my_services2.hex()))
    print('{}{:32} my host {}'.format(prefix, my_host.hex(), ipv6_to_ipv4(my_host)))
    print('{}{:32} my port {}'.format(prefix, my_port.hex(), unmarshal_uint(my_port)))
    print('{}{:32} nonce'.format(prefix, nonce.hex()))
    print('{}{:32} user agent size {}'.format(prefix, user_agent_size.hex(), uasz))
    print('{}{:32} user agent \'{}\''.format(prefix, user_agent.hex(), str(user_agent, encoding='utf-8')))
    print('{}{:32} start height {}'.format(prefix, start_height.hex(), unmarshal_uint(start_height)))
    print('{}{:32} relay {}'.format(prefix, relay.hex(), bytes(relay) != b'\0'))
    if len(extra) > 0:
        print('{}{:32} EXTRA!!'.format(prefix, extra.hex()))

def print_header(header, expected_cksum=None):
    """
    Report the contents of the given bitcoin message header
    :param header: bitcoin message header (bytes or bytearray)
    :param expected_cksum: the expected checksum for this version message, if known
    :return: message type
    """
    magic, command_hex, payload_size, cksum = header[:4], header[4:16], header[16:20], header[20:]
    command = str(bytearray([b for b in command_hex if b != 0]), encoding='utf-8')
    psz = unmarshal_uint(payload_size)
    is_valid_cksum = True
    if expected_cksum is None:
        verified = ''
    elif expected_cksum == cksum:
        verified = '(verified)'
    else:
        is_valid_cksum = False
        verified = '(WRONG!! ' + expected_cksum.hex() + ')'
    prefix = '  '
    print(prefix + 'HEADER')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} magic'.format(prefix, magic.hex()))
    print('{}{:32} command: {}'.format(prefix, command_hex.hex(), command))
    print('{}{:32} payload size: {}'.format(prefix, payload_size.hex(), psz))
    print('{}{:32} checksum {}'.format(prefix, cksum.hex(), verified))
    return command, is_valid_cksum

class BlockHeader:
    '''
    BlockHeader class to help serialize/deserialize a block header.
    BlockHeader is serialized in the 80 byte format and then hashed as part of Bitcoin’s proof-of-work algorithm.
    Refer: https://bitcoin.org/en/developer-reference#block-headers
    '''
    def __init__(self, header=None):
        '''
        version (int32_t): 4 bytes
        previous block header hash (char[32]): 32 bytes
        merkle root hash (char[32]): 32 bytes
        time (int32_t): 4 bytes
        nBits (int32_t): 4 bytes
        nonce (int32_t): 4 bytes

        Total size = 80 bytes
        '''
        if header is None:
            self.set_defaults()
        else:
            self.b_version = header.b_version
            self.hash_prev_block = header.hash_prev_block
            self.hash_merkle_root = header.hash_merkle_root
            self.b_time = header.b_time
            self.nbits = header.nbits
            self.nonce = header.nonce

    def set_defaults(self):
        '''
        Sets default values upon new object creation
        '''
        self.b_version = 1
        self.hash_prev_block = 0
        self.hash_merkle_root = 0
        self.b_time = 0
        self.nbits = 0
        self.nonce = 0

    def deserialize(self, b):
        '''
        Deserializes 80 byte block header using helper functions.
        Refer: https://bitcoin.org/en/developer-reference#block-headers
        '''
        self.b_version = unmarshal_int(b[:4])
        self.hash_prev_block = b[4:36].hex()
        self.hash_merkle_root = b[36:68].hex()
        self.b_time = unmarshal_uint(b[68:72])
        self.nbits = unmarshal_uint(b[72:76])
        self.nonce = unmarshal_uint(b[76:80])

    def serialize(self):
        '''
        Serializes block header into 80-byte format
        Refer: https://bitcoin.org/en/developer-reference#block-headers
        '''
        ver = int32_t(self.b_version)
        h_prev_blk = bytearray.fromhex(self.hash_prev_block)
        h_merkle_rt = bytearray.fromhex(self.hash_merkle_root)
        ser_time = uint32_t(self.b_time)
        ser_nbits = uint32_t(self.nbits)
        ser_nonce = uint32_t(self.nonce)
        header_bytes = (ver + h_prev_blk + h_merkle_rt + ser_time + ser_nbits + ser_nonce)
        return header_bytes

    def get_header_hash(self):
        '''
        Calculates double hash (sha256) of the header
        :return block header hash in hex decimal format
        Refer: https://bitcoin.org/en/developer-reference#block-headers
        '''
        ver = int32_t(self.b_version)
        h_prev_blk = bytearray.fromhex(self.hash_prev_block)
        h_merkle_rt = bytearray.fromhex(self.hash_merkle_root)
        ser_time = uint32_t(self.b_time)
        ser_nbits = uint32_t(self.nbits)
        ser_nonce = uint32_t(self.nonce)
        header_bytes = (ver + h_prev_blk + h_merkle_rt + ser_time + ser_nbits + ser_nonce)

        return double_sha256(header_bytes).hex()

class Block(BlockHeader):
    '''
    Block class to help serialize/deserialize a block. Blocks are the data stored on the blockchain.
    Inherits BlockHeader class.
    Refer: https://bitcoin.org/en/developer-reference#serialized-blocks
    '''
    def __init__(self, header=None):
        '''Class super class constructor. Contains a list of transactions.'''
        super(Block, self).__init__(header)
        self.transactions = []

    def deserialize(self, b):
        '''
        Deserializes a block. Invokes deserialize methods of BlockHeader and Transaction classes as well.
        :param: serialized block
        '''
        super(Block, self).deserialize(b)
        tx_hex, tx_cnt = unmarshal_compactsize(b[80:])
        size = 80 + len(tx_hex)

        for i in range(tx_cnt):
            tx = Transaction()
            size += tx.deserialize(b[size:])
            self.transactions.append(tx)

    def serialize(self):
        '''
        Serializes a block. Invokes serialize methods of BlockHeader and Transaction classes as well.
        :return: serialized block
        '''
        ser_block_header = super(Block, self).serialize()
        ser_txn_cnt = compactsize_t(len(self.transactions))
        ser_block = ser_block_header + ser_txn_cnt
        for tx in self.transactions:
            ser_block += tx.serialize()

        return ser_block

    @classmethod
    def get_merkle_root(cls, hashes):
        '''
        Calculates merkle root by recursively double hashing the input starting from leaf up to root node.
        :hashes: hashes of each transaction in a block
        :return: merkle root value calculated from Tx hashes
        '''
        while len(hashes) > 1:
            newhashes = []
            for i in range(0, len(hashes), 2):
                i2 = min(i+1, len(hashes)-1)
                newhashes.append(rev_endian(double_sha256(bytearray.fromhex(hashes[i]) + bytearray.fromhex(hashes[i2])).hex()))
            hashes = newhashes
        return hashes[0]

    def calc_merkle_root(self):
        '''
        Calculates merkle root by iterating over all the transactions in a block to fetch corresponding Tx hashes.
        :return: merkle root value calculated from Tx hashes
        '''
        hashes = []
        for tx in self.transactions:
            h = tx.get_tx_hash()
            hashes.append(h)
        return self.get_merkle_root(hashes)
    
class OutPoint:
    '''
    An output in a transaction which contains two fields:
    1. a value field for transferring zero or more satoshis and
    2. a pubkey script for indicating what conditions must be fulfilled for those satoshis to be further spent.
    Refer: https://bitcoin.org/en/developer-reference#raw-transaction-format
    '''
    def __init__(self, o_hash=0, index=0):
        self.hash = o_hash
        self.index = index

    def deserialize(self, b):
        '''
        Deserializes an outpoint
        :param: serialized outpoint
        :return: size of serialized outpoint
        '''
        self.hash = b[:32].hex()
        self.index = unmarshal_uint(b[32:36])
        return len(b)

    def serialize(self):
        '''
        Serializes an outpoint
        :return: serialized outpoint
        '''
        return bytearray.fromhex(self.hash) + uint32_t(self.index)

class TxIn:
    '''
    An input in a transaction which contains three fields: an outpoint, a signature script, and a sequence number.
    Refer: https://bitcoin.org/en/developer-reference#raw-transaction-format
    '''
    def __init__(self, outpoint=None, script_sig='', sequence=0):
        if outpoint is None:
            self.prev_out = OutPoint()
        else:
            self.prev_out = outpoint
        self.script_sig = script_sig
        self.sequence = sequence

    def deserialize(self, b):
        '''
        Deserializes a transaction input
        :param: serialized Tx input
        :return: size of serialized input
        '''
        self.prev_out = OutPoint()
        start_index = self.prev_out.deserialize(b[:36])
        s_b_hex, script_bytes_size = unmarshal_compactsize(b[start_index:])
        start_index = start_index + len(s_b_hex)
        end_index = start_index + script_bytes_size

        self.script_sig = b[start_index:end_index].hex()
        start_index = end_index
        end_index = end_index + 4
        self.sequence = unmarshal_uint(b[start_index:end_index])
        return end_index

    def serialize(self):
        '''
        Serializes a transaction input
        :return: serialized Tx input
        '''
        ser_outpoint = self.prev_out.serialize()
        ser_script_bytes = compactsize_t(len(bytearray.fromhex(self.script_sig)))
        ser_script_sig = bytearray.fromhex(self.script_sig)
        ser_seq = uint32_t(self.sequence)

        return ser_outpoint + ser_script_bytes + ser_script_sig + ser_seq

class TxOut:
    '''
    An output in a transaction which contains two fields:
    1. a value field for transferring zero or more satoshis and
    2. a pubkey script for indicating what conditions must be fulfilled for those satoshis to be further spent.
    Refer: https://bitcoin.org/en/developer-reference#raw-transaction-format
    '''
    def __init__(self, value=0, pk_script=''):
        self.value = value
        self.pk_script = pk_script

    def deserialize(self, b):
        '''
        Deserializes a transaction output
        :param: serialized Tx output
        :return: size of serialized Tx output
        '''
        self.value = unmarshal_int(b[:8])

        pk_script_hex, pk_script_bytes = unmarshal_compactsize(b[8:])
        start_index = 8 + len(pk_script_hex)
        end_index = start_index + pk_script_bytes
        self.pk_script = b[start_index:end_index].hex()
        return end_index

    def serialize(self):
        '''
        Serializes a transaction output
        :return: serialized Tx output
        '''
        ser_val = int64_t(self.value)
        ser_pk_script_bytes = compactsize_t(len(bytearray.fromhex(self.pk_script)))
        ser_pk_script = bytearray.fromhex(self.pk_script)

        return ser_val + ser_pk_script_bytes + ser_pk_script

class Transaction:
    '''
    A transaction is a transfer of Bitcoin value that is broadcast to the network and collected into blocks.
    A transaction typically references previous transaction outputs as new transaction inputs and dedicates all input Bitcoin values to new outputs.
    Refer: https://bitcoin.org/en/developer-reference#raw-transaction-format
    '''
    def __init__(self, tx=None):
        if tx is None:
            self.tx_version = 1
            self.tx_in = []
            self.tx_out = []
            self.lock_time = 0
        else:
            self.tx_version = tx.tx_version
            self.tx_in = copy.deepcopy(tx.tx_in)
            self.tx_out = copy.deepcopy(tx.tx_out)
            self.lock_time = tx.lock_time

    def deserialize(self, b):
        '''
        Deserializes a transaction
        :param: serialized Tx
        '''
        size = 4
        self.tx_version = unmarshal_int(b[:size]) # 4 bytes version
        tx_in_hex, tx_in_cnt = unmarshal_compactsize(b[size:]) # 1 byte (Varies)
        size += len(tx_in_hex)
        for i in range(tx_in_cnt):
            tx_input = TxIn()
            size += tx_input.deserialize(b[size:])
            self.tx_in.append(tx_input)
        
        tx_out_hex, tx_out_cnt = unmarshal_compactsize(b[size:])
        size += len(tx_out_hex)
        for i in range(tx_out_cnt):
            tx_output = TxOut()
            size += tx_output.deserialize(b[size:])
            self.tx_out.append(tx_output)
        
        self.lock_time = unmarshal_uint(b[size:size+4])
        size = size + 4
        return size

    def serialize(self):
        '''
        Serializes a transaction
        :return: serialized Tx
        '''
        ser_tx = int32_t(self.tx_version)
        ser_tx += compactsize_t(len(self.tx_in))
        for tx_input in self.tx_in:
            ser_tx += tx_input.serialize()
        ser_tx += compactsize_t(len(self.tx_out))
        for tx_output in self.tx_out:
            ser_tx += tx_output.serialize()
        ser_tx += uint32_t(self.lock_time)

        return ser_tx

    def get_tx_hash(self):
        '''TransactionID'''
        return rev_endian(double_sha256(self.serialize()).hex())

class BitCoin(object):
    '''
    This class helps connect to a sync node in a blockchain network.
    Helps download block headers, blocks and modifies a transaction output address and puslibshes to the network.
    '''
    def __init__(self, genesis_block, peer_endpoint=None, peer_node_ips_file_path=None):
        '''
        Initiates connection to a valid sync node upon object creation.
        '''
        self.cur_reconnect_attempt = 0
        self.received_ping_nonce = None
        self.genesis_block = genesis_block
        BLOCK_HEADERS.append(genesis_block)
        self.peer_node_ips_file_path = peer_node_ips_file_path
        self.peer_sock = self.peer_discovery_and_connect(peer_endpoint)
        if not self.peer_sock:
            raise ValueError('Unable to connect to a peer node. Please provide a valid node address.')

        self.peer_host, self.peer_port = self.peer_sock.getpeername()
        self.init_version_msgs(self.peer_sock)

    def peer_discovery_and_connect(self, peer_endpoint=None):
        '''
        Tries to connect to the given peer endpoint. If unable to connect, it iterates through all the endpoints specified in 'nodes_main.txt' file.
        :param peer_endpoint: Input endpoint of a sync node in a bitcoin network. Example: '76.182.211.41:443'
        '''
        sock = None

        if peer_endpoint:
            sock = self.connect_to_peer(peer_endpoint)

        if sock is None:
            peer_endpoints = self.get_peer_endpoints()
            for endpoint in peer_endpoints:
                sock = self.connect_to_peer(endpoint)
                if sock:
                    break
        return sock

    def init_version_msgs(self, sock):
        '''
        1. Sends a version message to a successfully connected sync node.
        2. Receives a version message from a sync node
        3. Receives a verack message from a sync node
        4. Also processes following messages received from a sync node: sendheaders, sendcmpct, feefilter, addr, ping
        5. Sends a pong message in response to a ping message from a sync node.
        :param sock: connection to a sync node.
        '''
        version_payload = self.create_version_message()
        header_message = self.create_header_message(CMD_VERSION, version_payload)
        message = header_message + version_payload
        print_message(message, 'sending')

        sock.sendall(message)
        resp = self.recv_all(sock)
        cmds_received, result = self.pr_get_cmds(resp)

        if CMD_VERSION in cmds_received:
            verack_msg = self.create_verack_msg()
            print_message(verack_msg, 'sending')
            sock.sendall(verack_msg)

            resp = self.recv_all(sock)
            cmds, result = self.pr_get_cmds(resp)
            cmds_received.extend(cmds)

        if CMD_PING in cmds_received and self.received_ping_nonce:
            pong_msg = self.create_pong_msg()
            print_message(pong_msg, 'sending')
            sock.sendall(pong_msg)

            resp = self.recv_all(sock)
            cmds, result = self.pr_get_cmds(resp)
            cmds_received.extend(cmds)

    def auto_reconnect(self):
        '''
        1. Upon connection failure, this method helps auto reconnect to a sync node so that current node can resume the work.
        2. Tries to re-connect for a max of MAX_SOCK_AUTO_RECONNECT times.
        3. Sleeps between attempts to reconnect
        4. Upon exceeding the max tries, it will raise an exception.
        '''
        has_reconnected = False
        while self.cur_reconnect_attempt <= MAX_SOCK_AUTO_RECONNECT:
            try:
                peer_endpoint = '{}:{}'.format(self.peer_host, self.peer_port)
                self.peer_sock = self.peer_discovery_and_connect(peer_endpoint)
                if not self.peer_sock:
                    raise ValueError('Unable to connect to a peer node. Please provide a valid node address.')
                has_reconnected = True
                host, port = self.peer_sock.getpeername()
                if self.peer_host != host or self.peer_port != port:
                    print('Sync node has changed from {} to {}'.format(peer_endpoint, host + ':' + str(port)))
                    self.peer_host = host
                    self.peer_port = port
                self.init_version_msgs(self.peer_sock)
                break
            except Exception as err:
                print('Failed to establish a connection with a peer. Error = {}'.format(err))
                time.sleep(1)
            finally:
                self.cur_reconnect_attempt += 1
        if not has_reconnected:
            print('Exceeded max attempts ({}) to establish a successful connection with a peer node. Exiting...'.format(MAX_SOCK_AUTO_RECONNECT))
            raise Exception('Exceeded max attempts ({}) to establish a successful connection with a peer node. Exiting...'.format(MAX_SOCK_AUTO_RECONNECT))

    def initial_block_download(self):
        '''
        Total time taken: 62 mins
        1. Downloads around ~ 608000 block headers from block chain
        2. Downloads in batches with a max batch size of 2000
        3. Persists all the block headers for later lookups
        '''
        max_headers_per_fetch = 2000
        trip = 1

        # current block height according to blockexplorer = 606859 (~ 304 batch fetches)
        retry_attempt = 0
        while True:
            pre_fetch_size = len(BLOCK_HEADERS)
            # since latest local header hash
            get_headers_message = self.create_get_headers_msg(1, BLOCK_HEADERS[len(BLOCK_HEADERS) - 1].get_header_hash())
            print_message(get_headers_message, 'sending')

            self.peer_sock.sendall(get_headers_message)
            resp = self.recv_all(self.peer_sock)
            self.pr_get_cmds(resp)

            if len(BLOCK_HEADERS) - pre_fetch_size < max_headers_per_fetch:
                retry_attempt += 1
            if len(BLOCK_HEADERS) >= 606859 or retry_attempt > 10: #(len(BLOCK_HEADERS) - pre_fetch_size) < max_headers_per_fetch or len(BLOCK_HEADERS) >= 606859:
                break # If sync node returned < 2000 headers or if we have at least ~ 606K headers then stop
            trip += 1

        print('Finished downloading all header blocks in the chain in {} trips'.format(trip))

    def get_block_by_height(self, block_height_to_fetch = 489370):
        '''
        Total time taken: 72 secs
        1. Downloads entire block for the given height.
        2. Looks up block header hash from the previous download
        3. Fallsback to hardcoded hash value for 489370 block, if unable to find the hash value from previously downloaded headers.
        4. Prints the full list of transactions
        :param block_height_to_fetch: height of the block in the blockchain which needs to be downloaded
        '''
        # 4089370 (My_SUID) % 600000 = 489370 (height)
        h = None
        if block_height_to_fetch >= len(BLOCK_HEADERS):
            print('Using hard coded header hash as the fetched header is short of the needed height. Total headers received = {} and block height to fetch = {}'.format(len(BLOCK_HEADERS), block_height_to_fetch))
            h = '000000000000000000db6b17c00afb63873a17c905d8ef625751611b94aa9cd7' # block header hash in big endian of block at height = suid % 600000
            h = bytearray.fromhex(h)[::-1].hex() # endianness
        else:
            h = BLOCK_HEADERS[block_height_to_fetch].get_header_hash()
            
        get_data_msg = self.create_get_data_msg(h)

        print_message(get_data_msg, 'sending')
        self.peer_sock.sendall(get_data_msg)
        resp = self.recv_all(self.peer_sock)
        cmds_received, result = self.pr_get_cmds(resp)

        return result['block']

    def modify_transaction_and_broadcast(self, block):
        '''
        1. Selects a transaction from the given block and parses out the output address in that transaction.
        2. Handles OpCodes used by both payment types (P2PHK and P2SH).
        3. Replaces the output addr with a hard coded address and reconstructs the block.
        4. Re-calculates the merkle root value upon modifying the transaction
        5. Sends the block using the block message to the sync node.
        :param block: Previously downloaded block (full block)
        '''
        modified_output_account_addr = 'dd897937f62a0dd99de15d036d8911ba044a668b'
        tx_index = int(len(block.transactions)/2) # can also use random.randint(0, len(txs))
        tx = block.transactions[tx_index]
        tx_output_index = int(len(tx.tx_out)/2)
        tx_output = tx.tx_out[tx_output_index]
        original_pk_script = tx_output.pk_script
        original_merkle_root = block.hash_merkle_root
        payment_type = None
        original_output_addr = None
        modified_pk_script = None

        print('=' * 100)
        print('\nModifying Transaction#{} with output index {}. Following are the details,\n'.format(tx_index, tx_output_index))      
        if original_pk_script.startswith(P2PKH_ADDR_OP_CODES_PREFIX_HEX):
            payment_type = 'P2PKH'
            original_output_addr = original_pk_script[len(P2PKH_ADDR_OP_CODES_PREFIX_HEX):-len(P2PKH_ADDR_OP_CODES_SUFFIX_HEX)]
            modified_pk_script = P2PKH_ADDR_OP_CODES_PREFIX_HEX + modified_output_account_addr + P2PKH_ADDR_OP_CODES_SUFFIX_HEX
        else:
            payment_type = 'P2SH'
            original_output_addr = original_pk_script[len(P2SH_ADDR_OP_CODES_PREFIX_HEX):-len(P2SH_ADDR_OP_CODES_SUFFIX_HEX)]
            modified_pk_script = P2SH_ADDR_OP_CODES_PREFIX_HEX + modified_output_account_addr + P2SH_ADDR_OP_CODES_SUFFIX_HEX

        print('Original Transaction Output:')
        prefix = '   '
        print_transaction_output(tx_output, prefix, prefix)

        tx_output.pk_script = modified_pk_script
        tx.tx_out[tx_output_index] = tx_output
        block.transactions[tx_index] = tx
        modified_merkle_root = block.calc_merkle_root()
        block.hash_merkle_root = modified_merkle_root
        
        print('Modified Transaction Output:')
        print_transaction_output(tx_output, prefix, prefix)
        print('\nOriginal output acct addr = {} and merkle root = {}\nModified output acct addr = {} and merkle root = {}\n'.format(original_output_addr, original_merkle_root, modified_output_account_addr, modified_merkle_root))

        block_msg = self.create_block_msg(block)
        self.peer_sock.settimeout(15)
        print_message(block_msg, 'sending', True)

        for i in range(3):
            try:
                self.peer_sock.sendall(block_msg)
                break
            except Exception as err:
                print('Failed due to send the modified block message due to error: {}. Reconnecting...'.format(err))
                self.auto_reconnect()
                if i == 2:
                    raise err
        
        self.peer_sock.settimeout(0.5)
        resp = self.recv_all(self.peer_sock)
        self.pr_get_cmds(resp)

    def create_block_msg(self, block):
        '''
        Creates a serialized block message to be send to sync node
        '''
        payload = block.serialize()
        header_message = self.create_header_message(CMD_BLOCK, payload)

        return header_message + payload
        

    def create_get_data_msg(self, block_header_hash):
        '''
        Creates a serialized getdata message to be send to sync node
        '''
        count = compactsize_t(1)
        # lookup single block
        inv_type = uint32_t(2) # MSG_BLOCK
        inv_hash = bytearray.fromhex(block_header_hash)

        payload = count + inv_type + inv_hash
        header_message = self.create_header_message(CMD_GETDATA, payload)

        return header_message + payload

    def create_get_headers_msg(self, hash_count, hashes_hex, stop_hash = None):
        '''
        Creates a serialized getheaders message to be send to sync node
        '''
        ver = int32_t(self.version)
        hash_cnt = compactsize_t(hash_count)
        blk_header_hashes = bytearray.fromhex(hashes_hex)
        if not stop_hash:
            stop_hash = 32 * b"\00"
        else:
            stop_hash = bytearray.fromhex(stop_hash)
        payload = ver + hash_cnt + blk_header_hashes + stop_hash

        header_message = self.create_header_message(CMD_GETHEADERS, payload)

        return header_message + payload

    def create_pong_msg(self):
        '''
        Creates a serialized pong message to be send to sync node
        '''
        pong_payload = uint64_t(self.received_ping_nonce)
        header_message = self.create_header_message(CMD_PONG, pong_payload)
        return (header_message + pong_payload)

    def create_verack_msg(self):
        '''
        Creates a serialized verack message to be send to sync node
        '''
        verack_payload = bytes()
        header_message = self.create_header_message(CMD_VERACK, verack_payload)
        return (header_message + verack_payload)        

    def pr_get_cmds(self, raw_resp):
        '''
        Prints the received response
        :param raw_resp Raw response received from sync node
        '''
        cmds_received = []
        msgs = []
        index = raw_resp.rfind(self.magic)
        while raw_resp and index >= 0:
            msgs.append(raw_resp[index:])
            raw_resp = raw_resp[:index]
            index = raw_resp.rfind(self.magic)

        result = {}
        while len(msgs) > 0:
            cmd, msg_kv = print_message(msgs.pop(), 'received')
            result.update(msg_kv)
            if cmd == CMD_PING and 'nonce' in msg_kv:
                self.received_ping_nonce = msg_kv['nonce']
            cmds_received.append(cmd)

        return cmds_received, result

    def recv_all(self, sock, buz_size=4096):
        '''
        Receives messages from sync node
        '''
        resp = bytearray()
        data = None
        while True:
            time.sleep(0.3)
            try:
                data = sock.recv(buz_size)
            except socket.timeout as e:
                return resp
            except ConnectionError as err:
                print('Received connection error. Error msg = {}'.format(err))
                self.auto_reconnect()
            if not data: break
            resp.extend(data)
            if len(data) < buz_size: break
        return resp

    def create_header_message(self, cmd_name, payload):
        '''
        magic (char[4]): 4 bytes
        command name (char[12]): 12 bytes
        payload size (uint32_t): 4 bytes
        checksum (char[4]): 4 bytes
        '''
        self.magic = bytes.fromhex('f9beb4d9')
        cmd_to_bytes = bytearray(cmd_name, encoding='utf-8')
        command_name = cmd_to_bytes + (12 - len(cmd_to_bytes)) * b"\00"
        payload_size = uint32_t(len(payload))
        cksum = checksum(payload)
        
        header_message = self.magic + command_name + payload_size + cksum

        return header_message

    def create_version_message(self):
        '''
        version (int32_t): 4 bytes
        services (uint64_t): 8 bytes
        timestamp (int64_t): 8 bytes
        addr_recv services (uint64_t): 8 bytes
        addr_recv IP address (char[16]): 16 bytes
        addr_recv port (uint16_t): 2 bytes
        addr_trans services (uint64_t): 8 bytes
        addr_trans IP address (char[16]): 16 bytes
        addr_trans port (uint16_t): 2 bytes
        nonce (uint64_t): 8 bytes
        user_agent bytes (CompactSize): Varies
        user_agent (string): Varies
        start_height (int32_t): 4 bytes
        relay (bool): 1 byte
        '''
        self.version = 70015
        my_version = int32_t(self.version)
        services = uint64_t(0)
        timestamp = int64_t(((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * MICROS_PER_SECOND))
        addr_recv_services = uint64_t(1)
        addr_recv_ip_addr = ipv6_from_ipv4(self.peer_host)
        addr_recv_port = uint16_t(self.peer_port)
        addr_trans_services = uint64_t(0)
        addr_trans_ip_addr = ipv6_from_ipv4(socket.gethostbyname('localhost'))
        addr_trans_port = uint16_t(8333) # My listening port number
        nonce = uint64_t(0)
        user_agent_bytes = compactsize_t(0)
        start_height = int32_t(0)
        relay = bool_t(False)

        payload = my_version + services + timestamp + addr_recv_services + addr_recv_ip_addr + addr_recv_port + addr_trans_services + addr_trans_ip_addr + addr_trans_port + nonce + user_agent_bytes + start_height + relay

        return payload

    def connect_to_peer(self, peer_endpoint):
        '''
        Connects to a peer using the provided endpoint. Handles both IPV4 and IPV6 addresses
        :param peer_endpoint host name and port number of a peer in the bitcoin network
        '''
        try:
            sock_af = socket.AF_INET # default ipv4
            endpoint = peer_endpoint
            last_colon_index = peer_endpoint.rfind(':')
            last_bracket_index = peer_endpoint.rfind(']')

            if last_colon_index > last_bracket_index:
                try:
                    port = int(peer_endpoint[last_colon_index+1:]) # works for both ipv4 and ipv6
                except ValueError:
                    print('Non-numeric port in endpoint = {}'.format(peer_endpoint))
                    raise
            host = peer_endpoint[:last_colon_index] # eg: IPV6: [2a0f:a300:bad::70bc] or IPV4: 202.215.222.230

            if host and host[0] == '[' and host[-1] == ']':
                # if IPV6 host then strip off the brackets
                host = host[1:-1]
                sock_af = socket.AF_INET6

            print('Attempting to connect to host = {} and port = {}'.format(host, str(port)))
            peer_sock = socket.socket(sock_af, socket.SOCK_STREAM)
            peer_sock.settimeout(0.5)
            peer_sock.connect((host, port))
            print('Successfully established connection with peer endpoint = {}'.format(peer_endpoint))
            return peer_sock
        except Exception as err:
            peer_sock.close()
            print('Failed to connect to peer endpoint = {} and error = {}'.format(peer_endpoint, err))
            return None
        
    def get_peer_endpoints(self):
        '''Reads nodes_main.txt file and process ip addrs. Assumes valid file with valid ip addrs. Excludes onion nodes.'''
        peer_endpoints = []
        
        file_path = self.peer_node_ips_file_path
        if file_path == None:
            file_path = DEFAULT_PEER_IP_FILE_PATH
        with open(file_path) as fp:
            line = fp.readline()
            while line:
                endpoint = line.strip()
                if 'onion' not in endpoint:
                    peer_endpoints.append(endpoint)
                line = fp.readline()
        return peer_endpoints

if __name__ == '__main__':
    start_time = time.time()
    print('Starting the node at {}'.format(datetime.now()))

    genesis_block = Block()
    ser_gen_blk = b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00;\xa3\xed\xfdz{\x12\xb2z\xc7,>gv\x8fa\x7f\xc8\x1b\xc3\x88\x8aQ2:\x9f\xb8\xaaK\x1e^J)\xab_I\xff\xff\x00\x1d\x1d\xac+|\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xffM\x04\xff\xff\x00\x1d\x01\x04EThe Times 03/Jan/2009 Chancellor on brink of second bailout for banks\xff\xff\xff\xff\x01\x00\xf2\x05*\x01\x00\x00\x00CA\x04g\x8a\xfd\xb0\xfeUH'\x19g\xf1\xa6q0\xb7\x10\\\xd6\xa8(\xe09\t\xa6yb\xe0\xea\x1fa\xde\xb6I\xf6\xbc?L\xef8\xc4\xf3U\x04\xe5\x1e\xc1\x12\xde\\8M\xf7\xba\x0b\x8dW\x8aLp+k\xf1\x1d_\xac\x00\x00\x00\x00"
    genesis_block.deserialize(ser_gen_blk)
    peer_nodes_ip_file_path = './nodes_main.txt' # Hard coded nodes_main.txt file path
    peer_ip = None # '76.182.211.41:443' #'1.255.226.167:8333' # Hardcode peer ip
    bit_coin = BitCoin(genesis_block, peer_ip, peer_nodes_ip_file_path)
    try:
        if len(sys.argv) < 2 or sys.argv[1] != 'skip_blockheader_download':
            start_time_header_download = time.time()
            bit_coin.initial_block_download()
            print('Total number of headers downloaded = {} and total time taken to download = {} secs'.format(len(BLOCK_HEADERS) - 1, time.time() - start_time_header_download))

        start_time_block_download = time.time()
        height = MY_SU_ID % 600000 # height = 489370
        block = bit_coin.get_block_by_height(height)
        print('Total time taken to download block at height {} is = {} secs'.format(height, time.time() - start_time_block_download))
        bit_coin.modify_transaction_and_broadcast(block)
    finally:
        print('Exiting the node at {}. Total time taken for script to complete = {} secs'.format(datetime.now(), time.time() - start_time))
    

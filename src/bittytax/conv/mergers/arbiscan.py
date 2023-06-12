# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2023

from .etherscan import _do_merge_etherscan, TXNS, TOKENS, NFTS, INTERNAL_TXNS
from ..datamerge import DataMerge
from ..out_record import TransactionOutRecord
from ..parsers.arbiscan import ARBISCAN_TXNS, ARBISCAN_TOKENS, ARBISCAN_NFTS, ARBISCAN_INT, WALLET, WORKSHEET_NAME

STAKE_ADDRESSES = []

def merge_arbiscan(data_files):
    # Do same merge as Etherscan
    merge = _do_merge_etherscan(data_files, STAKE_ADDRESSES)

    if merge:
        # Change Etherscan parsers to FantomScan
        if TOKENS in data_files:
            data_files[TOKENS].parser.worksheet_name = WORKSHEET_NAME
            for data_row in data_files[TOKENS].data_rows:
                if data_row.t_record:
                    address = data_row.t_record.wallet[- abs(TransactionOutRecord.WALLET_ADDR_LEN):]
                    data_row.t_record.wallet = "%s-%s" % (WALLET, address)

        if NFTS in data_files:
            data_files[NFTS].parser.worksheet_name = WORKSHEET_NAME
            for data_row in data_files[NFTS].data_rows:
                if data_row.t_record:
                    address = data_row.t_record.wallet[- abs(TransactionOutRecord.WALLET_ADDR_LEN):]
                    data_row.t_record.wallet = "%s-%s" % (WALLET, address)

    return merge

DataMerge("ArbiScan fees & multi-token transactions",
          {TXNS: {'req': DataMerge.MAN, 'obj': ARBISCAN_TXNS},
           TOKENS: {'req': DataMerge.OPT, 'obj': ARBISCAN_TOKENS},
           NFTS: {'req': DataMerge.OPT, 'obj': ARBISCAN_NFTS},
           INTERNAL_TXNS: {'req': DataMerge.OPT, 'obj': ARBISCAN_INT}},
          merge_arbiscan)
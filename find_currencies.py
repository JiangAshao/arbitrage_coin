# -*- coding: utf-8 -*-
# @Time    : 2021-08-23 21:26
# @Author  : Ashao

import json
import requests
from sqlite_db import sqlite_conn, sqlite_execute, sqlite_close

host = "https://api.gateio.ws"
prefix = "/api/v4"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

conn, cur = sqlite_conn()

# db_currencies = sqlite_execute(f'SELECT currency FROM currencies;', conn, cur)
# db_currencies = list(map(lambda x: x[0], db_currencies))

# # 刷新所有币种
# url = '/spot/currencies'
# currencies = requests.request('GET', host + prefix + url, headers=headers)
# currencies = currencies.json()
# for c in currencies:
#     currency = c.get("currency")
#     delisted = c.get("delisted")
#     withdraw_disabled = c.get("withdraw_disabled")
#     withdraw_delayed = c.get("withdraw_delayed")
#     deposit_disabled = c.get("deposit_disabled")
#     trade_disabled = c.get("trade_disabled")
#     if c.get("currency") in db_currencies:
#         sqlite_execute(f'UPDATE "currencies" SET "delisted"={delisted}, "withdraw_disabled"={withdraw_disabled}, "withdraw_delayed"={withdraw_delayed}, "deposit_disabled"={deposit_disabled}, "trade_disabled"={trade_disabled} WHERE currency={currency};', conn, cur)
#     else:
#         sqlite_execute(f'insert into "currencies" ( "currency", "delisted", "withdraw_disabled", "withdraw_delayed", "deposit_disabled", "trade_disabled") values ("{currency}","{delisted}","{withdraw_disabled}","{withdraw_delayed}","{deposit_disabled}","{trade_disabled}");', conn, cur)
# exit()

# # 可用交易对USDT
# available_currencies = sqlite_execute(f'SELECT currency FROM currencies WHERE "delisted"="False" AND "withdraw_disabled"="False" AND "withdraw_delayed"="False" AND "deposit_disabled"="False" AND "trade_disabled"="False";', conn, cur)
# available_currencies = list(map(lambda x: x[0], available_currencies))
# url = '/spot/currency_pairs'
# currency_pairs = requests.request('GET', host + prefix + url, headers=headers).json()
# currency_pairs = list(map(lambda x: x.get("id"), currency_pairs))
# available_pairs = list(set(map(lambda x: x + "_USDT", available_currencies)) & set(currency_pairs))
# print(available_pairs)
# exit()

available_pairs = ['VIDY_USDT', 'DPR_USDT', 'LBA_USDT', 'HNS_USDT', 'WAR_USDT', 'DASH_USDT', 'AGS_USDT', 'DSD_USDT', 'CHR_USDT', 'MATIC_USDT', 'DSLA_USDT', 'LABS_USDT', 'BEL_USDT', 'KP3R_USDT', 'QLC_USDT', 'SYLO_USDT', 'WSG_USDT', 'SUKU_USDT', 'ZIL_USDT', 'WBTC_USDT', 'AMP_USDT', 'USDC_USDT', 'WEST_USDT', 'BOX_USDT', 'ETC_USDT', 'SWOP_USDT', 'WAXP_USDT', 'CRT_USDT', 'XMARK_USDT', 'FIN_USDT', 'SNOW_USDT', 'OKB_USDT', 'CARDS_USDT', 'VTHO_USDT', 'PYR_USDT', 'TORN_USDT', 'KEX_USDT', 'CS_USDT', 'BMI_USDT', 'SFP_USDT', 'LOWB_USDT', 'SNT_USDT', 'DVP_USDT', 'SHIB_USDT', 'CUDOS_USDT', 'ADX_USDT', 'YFDAI_USDT', 'BXH_USDT', 'PCX_USDT', 'XNFT_USDT', 'BSCPAD_USDT', 'WXT_USDT', 'MER_USDT', 'YFI_USDT', 'DUSK_USDT', 'NEO_USDT', 'MRCH_USDT', 'WHALE_USDT', 'EOSDAC_USDT', 'SUSD_USDT', 'NSURE_USDT', 'GXS_USDT', 'DOGE_USDT', 'ZSC_USDT', 'GARD_USDT', 'FTI_USDT', 'GLM_USDT', 'LTC_USDT', 'BIFIF_USDT', 'SKT_USDT', 'MARSH_USDT', 'DDD_USDT', 'WNXM_USDT', 'RARI_USDT', 'COFI_USDT', 'HGET_USDT', 'LYM_USDT', 'RVC_USDT', 'XLM_USDT', 'PET_USDT', 'JST_USDT', 'DIGG_USDT', 'CORN_USDT', 'MTR_USDT', 'LKR_USDT', 'ALEPH_USDT', 'YLD_USDT', 'TRX_USDT', 'SLP_USDT', 'CNNS_USDT', 'PARA_USDT', 'IAG_USDT', 'ABBC_USDT', 'EXRD_USDT', 'COMBO_USDT', 'COS_USDT', 'CGG_USDT', 'FREE_USDT', 'WHITE_USDT', 'FEI_USDT', 'CQT_USDT', 'NIIFI_USDT', 'QI_USDT', 'ANC_USDT', 'COMP_USDT', 'RIF_USDT', 'FLUX_USDT', 'LEO_USDT', 'INK_USDT', 'BTCBEAR_USDT', 'STRONG_USDT', 'EFI_USDT', 'UNCX_USDT', 'REM_USDT', 'NEAR_USDT', 'HERO_USDT', 'XPR_USDT', 'JGN_USDT', 'DIS_USDT', '10SET_USDT', 'AME_USDT', 'PMON_USDT', 'MBOX_USDT', 'SHARE_USDT', 'ARMOR_USDT', 'XOR_USDT', 'ALY_USDT', 'DPET_USDT', 'LSS_USDT', 'NWC_USDT', 'SRK_USDT', 'KNC_USDT', 'AQT_USDT', 'ZRX_USDT', 'TKO_USDT', 'XRPBULL_USDT', 'WING_USDT', 'DEXE_USDT', 'FINE_USDT', 'RUFF_USDT', 'PAY_USDT', 'MDA_USDT', 'VIDYX_USDT', 'DUCK2_USDT', 'FAR_USDT', 'MATH_USDT', 'ENJ_USDT', 'ALAYA_USDT', 'GRIN_USDT', 'STAR_USDT', 'NOIA_USDT', 'GAME_USDT', 'BORING_USDT', 'TRADE_USDT', 'RAI_USDT', 'RAD_USDT', 'BABYDOGE_USDT', 'C98_USDT', 'REEF_USDT', 'GS_USDT', 'QSP_USDT', 'POLS_USDT', 'DFL_USDT', 'FRA_USDT', 'XMR_USDT', 'WAVES_USDT', 'CSPR_USDT', 'ANT_USDT', 'GALA_USDT', 'QTUM_USDT', 'FORM_USDT', 'BLANKV2_USDT', 'HMT_USDT', 'PERA_USDT', 'GHST_USDT', 'FROG_USDT', 'ONX_USDT', 'ZPT_USDT', 'KLAY_USDT', 'KAVA_USDT', 'LIKE_USDT', 'FARM_USDT', 'KEY_USDT', 'RSR_USDT', 'RAZE_USDT', 'CZZ_USDT', 'USDG_USDT', 'CRU_USDT', 'FORTH_USDT', 'STC_USDT', 'ODDZ_USDT', 'MDS_USDT', 'DPY_USDT', 'ETHBEAR_USDT', 'MPH_USDT', 'DOWS_USDT', 'SPA_USDT', 'DORA_USDT', 'EOS_USDT', 'DYP_USDT', 'MTV_USDT', 'VET_USDT', 'CIR_USDT', 'BAL_USDT', 'XRPBEAR_USDT', 'ELON_USDT', 'AKITA_USDT', 'SFI_USDT', 'SNET_USDT', 'GAS_USDT', 'KONO_USDT', 'CHNG_USDT', 'ARRR_USDT', 'XVG_USDT', 'PICKLE_USDT', 'FIRE_USDT', 'RENBTC_USDT', 'CELR_USDT', 'KAI_USDT', 'ZEN_USDT', 'DERC_USDT', 'AIOZ_USDT', 'SAFEMOON_USDT', 'LAVA_USDT', 'SWTH_USDT', 'TBE_USDT', 'DREP_USDT', 'LIT_USDT', 'UNFI_USDT', 'NAFT_USDT', 'MAPS_USDT', 'BADGER_USDT', 'BTT_USDT', 'CFI_USDT', 'SXP_USDT', 'HUSD_USDT', 'YFII_USDT', 'KEEP_USDT', 'NMT_USDT', 'XRP_USDT', 'RED_USDT', 'WSIENNA_USDT', 'WILD_USDT', 'API3_USDT', 'GTH_USDT', 'BASE_USDT', 'OXY_USDT', 'PCNT_USDT', 'SAITO_USDT', 'CTI_USDT', 'AXS_USDT', 'INJ_USDT', 'MUSE_USDT', 'DCR_USDT', 'MTRG_USDT', 'EOSBULL_USDT', 'DFYN_USDT', 'OCTO_USDT', 'SENC_USDT', 'DG_USDT', 'GUM_USDT', 'VSO_USDT', 'LUNA_USDT', 'HIVE_USDT', 'TARA_USDT', 'WRX_USDT', 'NUX_USDT', 'WOM_USDT', 'STBU_USDT', 'ONS_USDT', 'SRM_USDT', 'WIKEN_USDT', 'BAO_USDT', 'BAMBOO_USDT', 'COTI_USDT', 'BAKE_USDT', 'TLM_USDT', 'TOOLS_USDT', 'DOS_USDT', 'NULS_USDT', 'BKC_USDT', 'RBC_USDT', 'IOI_USDT', 'ARES_USDT', 'BAND_USDT', 'EHASH_USDT', 'SALT_USDT', 'POOL_USDT', 'BFC_USDT', 'MINA_USDT', 'HSC_USDT', 'OPIUM_USDT', 'UNISTAKE_USDT', 'OLT_USDT', 'CRV_USDT', 'K21_USDT', 'AUDIO_USDT', 'KFC_USDT', 'LTO_USDT', 'SCRT_USDT', 'NFTB_USDT', 'EOSBEAR_USDT', 'FILDA_USDT', 'UTK_USDT', 'UOS_USDT', 'XCH_USDT', 'SLM_USDT', 'MITH_USDT', 'PLA_USDT', 'ALN_USDT', 'CREAM_USDT', 'KIF_USDT', 'ALPHR_USDT', 'PBR_USDT', 'TLOS_USDT', 'ORAO_USDT', 'REQ_USDT', 'FRONT_USDT', 'STND_USDT', '100X_USDT', 'BSCS_USDT', 'ONT_USDT', 'MASK_USDT', 'NRV_USDT', 'PKF_USDT', 'SLIM_USDT', 'SUSHI_USDT', 'IDEA_USDT', 'KTN_USDT', 'RAY_USDT', 'OXT_USDT', 'FXF_USDT', 'ROUTE_USDT', 'CKB_USDT', 'RARE_USDT', 'JASMY_USDT', 'BLES_USDT', 'STOX_USDT', 'TVK_USDT', 'OPA_USDT', 'PRQ_USDT', 'HYDRA_USDT', 'IOTX_USDT', 'PHA_USDT', 'ICP_USDT', 'ORN_USDT', 'RLY_USDT', 'FRAX_USDT', 'HEGIC_USDT', 'UMA_USDT', 'PAX_USDT', 'BUSY_USDT', 'BAGS_USDT', 'KTON_USDT', 'SOP_USDT', 'MTA_USDT', 'PHTR_USDT', 'KISHU_USDT', 'GT_USDT', 'MTL_USDT', 'METIS_USDT', 'KIN_USDT', 'MARS_USDT', 'STX_USDT', 'DOCK_USDT', 'BUSD_USDT', 'IOST_USDT', 'TIDAL_USDT', 'IHT_USDT', 'OMG_USDT', 'PERP_USDT', 'FUSE_USDT', 'PBTC35A_USDT', 'UNI_USDT', 'FST_USDT', 'RCN_USDT', 'BAC_USDT', 'AVAX_USDT', 'STPT_USDT', 'COVER_USDT', 'NOA_USDT', 'FLM_USDT', 'BOND_USDT', 'RLC_USDT', 'CLV_USDT', 'GRT_USDT', 'RVN_USDT', 'SKRT_USDT', 'SWAP_USDT', 'MDT_USDT', 'OGN_USDT', 'PI_USDT', 'NFTX_USDT', 'SPI_USDT', 'STARL_USDT', 'FUEL_USDT', 'FEAR_USDT', 'BCD_USDT', 'HOGE_USDT', 'ISP_USDT', 'MM_USDT', 'TUSD_USDT', 'ROOK_USDT', 'DOG_USDT', 'WNCG_USDT', 'ONC_USDT', 'XEND_USDT', 'XTZ_USDT', 'WICC_USDT', 'UMX_USDT', 'AXIS_USDT', 'OLY_USDT', 'BAS_USDT', 'ANY_USDT', 'HBAR_USDT', 'PSG_USDT', 'BFT_USDT', 'SKM_USDT', 'PRARE_USDT', 'ARGON_USDT', 'EPS_USDT', 'SKL_USDT', 'FXS_USDT', 'DOGGY_USDT', 'SMT_USDT', 'BLY_USDT', 'XAVA_USDT', 'UMB_USDT', 'EWT_USDT', 'NBS_USDT', 'SOUL_USDT', 'QBT_USDT', 'KSM_USDT', 'LEV_USDT', 'LAYER_USDT', 'MED_USDT', 'MIX_USDT', 'HOD_USDT', 'YGG_USDT', 'LON_USDT', 'BANK_USDT', 'HOT_USDT', 'SLICE_USDT', 'UST_USDT', 'SPHRI_USDT', 'PERI_USDT', 'SHOPX_USDT', 'WIN_USDT', 'GDAO_USDT', 'XYM_USDT', 'ALCX_USDT', 'THETA_USDT', 'O3_USDT', 'DHX_USDT', 'PEARL_USDT', 'ACH_USDT', 'TWT_USDT', 'SUTER_USDT', 'ORC_USDT', 'ESS_USDT', 'TROY_USDT', 'ATOM_USDT', 'ONG_USDT', 'RFUEL_USDT', 'MTN_USDT', 'PUNDIX_USDT', 'VRA_USDT', 'PNT_USDT', 'AKRO_USDT', 'CART_USDT', 'QNT_USDT', 'FET_USDT', 'DNXC_USDT', 'STEP_USDT', 'LRC_USDT', 'XVS_USDT', 'TRIBE_USDT', 'CORE_USDT', 'POLC_USDT', 'NU_USDT', 'POLK_USDT', 'ZCN_USDT', 'MET_USDT', 'ETHBULL_USDT', 'ASD_USDT', 'SOL_USDT', 'SC_USDT', 'SUPER_USDT', 'XCUR_USDT', 'HYVE_USDT', 'L3P_USDT', 'ATD_USDT', 'CHAIN_USDT', 'STN_USDT', 'BUY_USDT', 'CAPS_USDT', 'SFG_USDT', 'FIO_USDT', 'BTCBULL_USDT', 'DF_USDT', 'DAI_USDT', 'EGLD_USDT', 'TPT_USDT', 'POLY_USDT', 'BOA_USDT', 'ERN_USDT', 'MFT_USDT', 'CDT_USDT', 'BOSON_USDT', 'KLV_USDT', 'REVV_USDT', 'PWAR_USDT', 'APN_USDT', 'BCUG_USDT', 'GOF_USDT', 'KPAD_USDT', 'CHZ_USDT', 'SHR_USDT', 'PST_USDT', 'BXC_USDT', 'CTK_USDT', 'TSHP_USDT', 'ARPA_USDT', 'SAKE_USDT', 'CAKE_USDT', 'BCH_USDT', 'BDP_USDT', 'WOO_USDT', 'TOMO_USDT', 'SHFT_USDT', 'BONDED_USDT', 'WGRT_USDT', 'FIL_USDT', 'FSN_USDT', 'HORD_USDT', 'TON_USDT', 'DAFI_USDT', 'DAO_USDT', 'XEM_USDT', 'FIRO_USDT', 'AR_USDT', 'BLZ_USDT', 'VRT_USDT', 'RAZOR_USDT', 'SWRV_USDT', 'CEL_USDT', 'YIELD_USDT', 'YFX_USDT', 'FIS_USDT', 'CRP_USDT', 'LPT_USDT', 'ALGO_USDT', 'BEAM_USDT', 'LAT_USDT', 'RSV_USDT', 'MKR_USDT', 'HARD_USDT', 'PDEX_USDT', 'HNT_USDT', 'ADA_USDT', 'MANA_USDT', 'MIS_USDT', 'INSUR_USDT', 'RNDR_USDT', 'BEPRO_USDT', 'HAPI_USDT', 'HT_USDT', 'ORAI_USDT', 'QKC_USDT', 'KINE_USDT', 'SPS_USDT', 'JULD_USDT', 'CELO_USDT', 'COFIX_USDT', 'KALM_USDT', 'MOFI_USDT', 'OCEAN_USDT', 'AUCTION_USDT', 'MAN_USDT', 'BNTY_USDT', 'PNK_USDT', 'KGC_USDT', 'UDO_USDT', 'GTC_USDT', 'MDX_USDT', 'NAX_USDT', 'CRBN_USDT', 'INV_USDT', 'LBK_USDT', 'POOLZ_USDT', 'SERO_USDT', 'FTT_USDT', 'PROPS_USDT', 'PIG_USDT', 'NSBT_USDT', 'NFT_USDT', 'DEK_USDT', 'RFOX_USDT', 'RAMP_USDT', 'BZZ_USDT', 'NKN_USDT', 'XED_USDT', 'COOK_USDT', 'HPB_USDT', 'UFT_USDT', 'ZEC_USDT', 'DKA_USDT', 'STMX_USDT', 'FEVR_USDT', 'XPRT_USDT', 'JFI_USDT', 'EGG_USDT', 'BMON_USDT', 'OKT_USDT', 'YOP_USDT', 'DRGN_USDT', 'CONV_USDT', 'ZEE_USDT', 'SAFEMARS_USDT', 'OCC_USDT', 'ROOBEE_USDT', 'AAVE_USDT', 'CWS_USDT', 'SNX_USDT', 'BZRX_USDT', 'EPK_USDT', 'BTC_USDT', 'PNL_USDT', 'NAOS_USDT', 'AERGO_USDT', 'PROM_USDT', 'DOP_USDT', 'VRX_USDT', 'ORBS_USDT', 'SAND_USDT', 'CVC_USDT', 'CRE_USDT', 'LIME_USDT', 'AE_USDT', 'MBL_USDT', 'IPAD_USDT', 'CHESS_USDT', 'ALPA_USDT', 'SKILL_USDT', 'IDV_USDT', 'HIT_USDT', 'NEST_USDT', 'DDIM_USDT', 'BURP_USDT', 'TFUEL_USDT', 'DEGO_USDT', 'TNC_USDT', 'RATING_USDT', '88MPH_USDT', 'HC_USDT', 'OOE_USDT', 'EDG_USDT', 'STEEM_USDT', 'BLACK_USDT', 'SUN_USDT', 'ASS_USDT', 'LION_USDT', 'POND_USDT', 'MOMA_USDT', 'BYN_USDT', 'ALICE_USDT', 'ATP_USDT', 'SASHIMI_USDT', 'CTSI_USDT', 'RDN_USDT', 'KYL_USDT', 'VALUE_USDT', 'GNX_USDT', 'A5T_USDT', 'BTS_USDT', 'DDOS_USDT', 'TCP_USDT', 'RAGE_USDT', 'HOPR_USDT', 'ERG_USDT', 'SLNV2_USDT', 'VELO_USDT', 'BIRD_USDT', 'DOT_USDT', 'SUP_USDT', 'GITCOIN_USDT', 'FTM_USDT', 'HAI_USDT', 'ONE_USDT', 'LEMD_USDT', 'VAI_USDT', 'MXC_USDT', 'SDAO_USDT', 'AST_USDT', 'STR_USDT', 'ZKS_USDT', 'SMTY_USDT', 'ILV_USDT', 'DHV_USDT', 'LPOOL_USDT', 'OAX_USDT', 'EZ_USDT', 'DIA_USDT', 'LIEN_USDT', 'PNG_USDT', 'LAMB_USDT', 'CVP_USDT', 'DLTA_USDT', '1INCH_USDT', 'BTG_USDT', 'ADEL_USDT', 'OST_USDT', 'DFA_USDT', 'FIDA_USDT', 'ALPHA_USDT', 'FLOW_USDT', 'OIN_USDT', 'MAHA_USDT', 'REP_USDT', 'SFIL_USDT', 'OPEN_USDT', 'FUN_USDT', 'ELEC_USDT', 'TCT_USDT', 'RING_USDT', 'APYS_USDT', 'LEMO_USDT', 'STORJ_USDT', 'NAS_USDT', 'FIC_USDT', 'GLQ_USDT', 'IRIS_USDT', 'KAR_USDT', 'SWINGBY_USDT', 'OMI_USDT', 'OCN_USDT', 'ZCX_USDT', 'ABT_USDT', 'DENT_USDT', 'MIR_USDT', 'FAN_USDT', 'NBOT_USDT', 'DBC_USDT', 'YAM_USDT', 'POWR_USDT', 'ANKR_USDT', 'ARCX_USDT', 'GO_USDT', 'GEM_USDT', 'LINA_USDT', 'DUCK_USDT', 'AUTO_USDT', 'ICX_USDT', 'CFX_USDT', 'VSP_USDT', 'OM_USDT', 'LOCG_USDT', 'LOON_USDT', 'CRO_USDT', 'DX_USDT', 'TAI_USDT', 'BRY_USDT', 'WOZX_USDT', 'MOBI_USDT', 'NMR_USDT', 'GDT_USDT', 'ETHA_USDT', 'SLRS_USDT', 'RFR_USDT', 'ALPACA_USDT', 'STRAX_USDT', 'LINK_USDT', 'NORD_USDT', 'REN_USDT', 'BIT_USDT', 'PERL_USDT', 'AKT_USDT', 'BAT_USDT', 'OVR_USDT', 'ETH_USDT', 'ESD_USDT', 'TRU_USDT', 'NYZO_USDT', 'TOTM_USDT', 'FLY_USDT', 'FOR_USDT', 'CBK_USDT', 'GRAP_USDT', 'ELF_USDT', 'DODO_USDT', 'UNN_USDT', 'DFND_USDT', 'CVX_USDT', 'AVA_USDT', 'CREDIT_USDT', 'RUNE_USDT', 'CORAL_USDT', 'XCAD_USDT', 'QUICK_USDT', 'WOOP_USDT', 'OLYMPUS_USDT', 'BTCST_USDT', 'STAKE_USDT', 'BDT_USDT']

sqlite_execute(f'DROP TABLE IF EXISTS "tmp_currency_pairs";', conn, cur)
sqlite_execute(f'CREATE TABLE "tmp_currency_pairs"( "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "currency_pair" varchar(32, 0), "spacing" decimal DEFAULT 0, "volume" decimal DEFAULT 0, "bid" decimal DEFAULT 0, "ask" decimal DEFAULT 0, "bid_volume" decimal DEFAULT 0, "ask_volume" decimal DEFAULT 0);', conn, cur)
for currency_pair in available_pairs:
    order_book = '/spot/order_book?currency_pair=' + currency_pair
    r = requests.request('GET', host + prefix + order_book, headers=headers)
    if isinstance(r.json(), dict):
        r_json = r.json()
        asks = r_json.get("asks")
        bids = r_json.get("bids")

        if bids and asks:
            ask = asks[0][0]
            bid = bids[0][0]
            ask_volume = asks[0][1]
            bid_volume = bids[0][1]
            spacing = (float(ask) / float(bid) - 1)
            query_param = '/spot/candlesticks?limit=1&interval=1d&currency_pair=' + currency_pair
            candlesticks = requests.request('GET', host + prefix + query_param, headers=headers).json()
            volume = candlesticks[0][1]
            sqlite_execute(f'insert into "tmp_currency_pairs" ( "currency_pair", "spacing", "volume", "bid", "ask", "bid_volume", "ask_volume") values ("{currency_pair}",{spacing},{volume},{bid},{ask}, {bid_volume}, {ask_volume});', conn, cur)
            print(f'pair:{currency_pair},spacing:{spacing},交易量:{volume} bid:{bid},ask:{ask} bid_volume:{bid_volume},ask_volume:{ask_volume}')
    else:
        print(f'r数据异常:{r.text},order_book:{order_book}')
        exit()
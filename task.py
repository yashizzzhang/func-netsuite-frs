
def main():
    
    import time
    import pandas as pd
    import numpy as np
    import requests
    from datetime import datetime
    import pytz
    import os
    """## Connect API"""

    from azure.keyvault.secrets import SecretClient
    from azure.identity import DefaultAzureCredential
    import json

    ## credential obtained from managed identity or azure login, to access azure SQL and KV
    def_credential = DefaultAzureCredential()

    ## connect to Azure Key Vault securely
    #keyVaultName = os.getenv('KEY_VAULT_NAME')
    keyVaultName = "sea-it-prd-frs-kv"
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    kv_client = SecretClient(vault_url=KVUri, credential=def_credential)

    account_id = kv_client.get_secret('PRD-ACCOUNT-ID').value
    consumer_key = kv_client.get_secret('PRD-CONSUMER-KEY').value
    consumer_secret = kv_client.get_secret('PRD-CONSUMER-SECRET').value
    token_id = kv_client.get_secret('PRD-TOKEN-ID').value
    token_secret = kv_client.get_secret('PRD-TOKEN-SECRET').value
    script = kv_client.get_secret('PRD-SCRIPT').value
    realm = kv_client.get_secret('PRD-REALM').value
    gcp_sa = kv_client.get_secret('PRD-GCP-SERVICE-ACCOUNT').value

    from ns_data_extractor_api import Netsuite
    ns = Netsuite(account_id, realm, consumer_key, consumer_secret, token_id, token_secret, script)

    """## Global Parameters"""

    ## Setting Global Parameters
    from datetime import datetime
    start_time = time.time()
    current_year = datetime.now().year
    today_iso = datetime.now().strftime("%Y-%m-%d")
    appscript_webapp_url = 'https://script.google.com/macros/s/AKfycbzhqliDq6bVBcsYBxkgyVphec586FDOTSC-Ax_g-vpcNsDM3Lp0EyjjnATRvItfQNV2/exec'
    ## Toggle
    period_end = '2024-05-31'

    """# Data Extraction

    ### Sales Order (so_df)
    """

    ss= '''
    {
    "type": "salesorder",
    "filters": [
        {
        "name": "type",
        "operator": "anyof",
        "values": [
            "SalesOrd"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "taxline",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "quantity",
        "operator": "isnotempty",
        "values": [],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        }
    ],
    "columns": [
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "so_id",
        "type": "text",
        "formula": "TO_CHAR({internalId})",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "so_tran_id",
        "type": "text",
        "formula": "{number}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "global_project",
        "type": "text",
        "formula": "{cseg_global_project}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "so_tran_date",
        "type": "text",
        "formula": "TO_CHAR({trandate},'YYYY-MM-DD')",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "subsidiary",
        "type": "text",
        "formula": "{subsidiary.tranprefix}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "status",
        "type": "text",
        "formula": "{status}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "department",
        "type": "text",
        "formula": "{department}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "business_line",
        "type": "text",
        "formula": "{custbody_so_order_business_line}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "quote_no",
        "type": "text",
        "formula": "{custbody_sales_quotation_no}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "po_ref",
        "type": "text",
        "formula": "{otherrefnum}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "customer",
        "type": "text",
        "formula": "{customermain.altname}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "regco",
        "type": "text",
        "formula": "{classnohierarchy}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "sales_rep",
        "type": "text",
        "formula": "{salesrep}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "item_group",
        "type": "text",
        "formula": "case when {item.custitem_item_group} is not null then {item.custitem_item_group}  else 'No-Item-Group' end",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "base_curr",
        "type": "text",
        "formula": "{subsidiary.currency}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "tran_curr",
        "type": "text",
        "formula": "{currency}",
        "sortdir": "NONE"
        },
        {
        "name": "formulanumeric",
        "summary": "GROUP",
        "label": "exchange_rate",
        "type": "float",
        "formula": "{exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_amount_net_notax_base",
        "type": "currency",
        "formula": "{netamountnotax}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_mtce_amount_net_notax_base",
        "type": "currency",
        "formula": "CASE WHEN UPPER({item.name}) LIKE '%-MTCE-%' THEN {netamountnotax} * {exchangerate}      ELSE 0    END",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_ps_amount_net_notax_base",
        "type": "currency",
        "formula": "CASE WHEN UPPER({item.name}) LIKE '%-PS-%' THEN {netamountnotax} * {exchangerate}      ELSE 0    END",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_indirect_net_notax_base",
        "type": "currency",
        "formula": "CASE WHEN UPPER({item.name}) LIKE '%-INDIRECT-%' THEN {netamountnotax} * {exchangerate}      ELSE 0    END",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_amount_gross_notax_base",
        "type": "currency",
        "formula": "{grossamount}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_discount_base",
        "type": "currency",
        "formula": "{grossamount} - {netamountnotax}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_tax_base",
        "type": "currency",
        "formula": "{taxitem.rate} * {netamountnotax} / 100",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_amount_net_tax_base",
        "type": "currency",
        "formula": "{netamountnotax} * ( 100 +{taxitem.rate})/100 ",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_cost_estimate_base",
        "type": "currency",
        "formula": "{costestimate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulapercent",
        "summary": "SUM",
        "label": "so_actual_gm_pct_base",
        "type": "percent",
        "formula": "({netamountnotax} - {costestimate}) / nullif({netamountnotax},0)",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_amount_net_notax_tran",
        "type": "currency",
        "formula": "{netamountnotax} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_amount_gross_notax_tran",
        "type": "currency",
        "formula": "{grossamount} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_discount_tran",
        "type": "currency",
        "formula": "({grossamount} - {netamountnotax}) / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_tax_tran",
        "type": "currency",
        "formula": "{taxitem.rate} * {netamountnotax} / 100/{exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_amount_net_tax_tran",
        "type": "currency",
        "formula": "{netamountnotax} * ( 100 +{taxitem.rate})/100  / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "so_cost_estimate_tran",
        "type": "currency",
        "formula": "{costestimate} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulapercent",
        "summary": "SUM",
        "label": "so_actual_gpm_pct_tran",
        "type": "percent",
        "formula": "({netamountnotax} - {costestimate} ) / nullif({netamountnotax},0) / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_amount_net_notax_base",
        "type": "currency",
        "formula": "{totalamount} - {taxtotal}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_tax_base",
        "type": "currency",
        "formula": "{taxtotal}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_cost_estimate_base",
        "type": "currency",
        "formula": "{trancostestimate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_approved_net_notax_base",
        "type": "currency",
        "formula": "{custbody_so_approved_salesamt_basecurr}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_approved_budgeted_cost_base",
        "type": "currency",
        "formula": "{custbody_so_approved_est_cost_basecurr}",
        "sortdir": "NONE"
        },
        {
        "name": "formulapercent",
        "summary": "MIN",
        "label": "so_overall_approved_gpm_pct_base",
        "type": "percent",
        "formula": "({custbody_so_approved_salesamt_basecurr} - {custbody_so_approved_est_cost_basecurr}) / NULLIF({custbody_so_approved_salesamt_basecurr},0)",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_amount_net_notax_tran",
        "type": "currency",
        "formula": "({totalamount} - {taxtotal}) / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_tax_tran",
        "type": "currency",
        "formula": "{taxtotal}/{exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_cost_estimate_tran",
        "type": "currency",
        "formula": "{trancostestimate} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_approved_net_notax_tran",
        "type": "currency",
        "formula": "{custbody_so_approved_sales_amount}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "MIN",
        "label": "so_overall_approved_budgeted_cost_tran",
        "type": "currency",
        "formula": "{custbody_so_approved_est_cost}",
        "sortdir": "NONE"
        },
        {
        "name": "formulapercent",
        "summary": "MIN",
        "label": "so_overall_approved_gpm_pct_tran",
        "type": "percent",
        "formula": "({custbody_so_approved_sales_amount} - {custbody_so_approved_est_cost}) / NULLIF({custbody_so_approved_sales_amount} ,0)",
        "sortdir": "NONE"
        },
        {
        "name": "formulapercent",
        "summary": "MIN",
        "label": "so_overall_actual_gpm_pct",
        "type": "percent",
        "formula": "({totalamount}-{taxtotal}-{trancostestimate}) / NULLIF({totalamount}-{taxtotal},0)",
        "sortdir": "NONE"
        },
        {
        "name": "formulapercent",
        "summary": "MIN",
        "label": "so_overall_gpm_pct_gap",
        "type": "percent",
        "formula": "(({custbody_so_approved_salesamt_basecurr} - {custbody_so_approved_est_cost_basecurr}) / NULLIF({custbody_so_approved_salesamt_basecurr},0)) - (({totalamount}-{taxtotal}-{trancostestimate}) / NULLIF({totalamount}-{taxtotal},0))",
        "sortdir": "NONE"
        }
    ],
    "settings": [
        {
        "name": "consolidationtype",
        "value": "NONE"
        },
        {
        "name": "includeperiodendtransactions",
        "value": "F"
        }
    ]
    }
    '''

    so_df = ns.runSearch(ss, 50000)

    """### Sales Order FX Adjustment"""

    ss = '''
    {
    "type": "revenuearrangement",
    "filters": [
        {
        "name": "type",
        "operator": "anyof",
        "values": [
            "RevArrng"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "applyinglinktype",
        "operator": "anyof",
        "values": [
            "OrdReval"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "accounttype",
        "join": "applyingtransaction",
        "operator": "anyof",
        "values": [
            "Income"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "posting",
        "join": "applyingtransaction",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "voided",
        "join": "applyingtransaction",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        }
    ],
    "columns": [
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "so_id",
        "type": "text",
        "formula": "TO_CHAR({custbody_linked_sales_order.id})",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "item_group",
        "type": "text",
        "formula": "case when {item.custitem_item_group} is not null then {item.custitem_item_group}  else 'No-Item-Group' end",
        "sortdir": "NONE"
        },
        {
        "name": "applyinglinkamount",
        "summary": "SUM",
        "label": "sofxadj_amount_base",
        "type": "currency",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "MIN",
        "label": "sofxadj_journals",
        "type": "text",
        "formula": "NS_CONCAT(DISTINCT(CONCAT({applyingtransaction.number},TO_CHAR({applyingtransaction.tranDate},' (YYYY-MM-DD)'))))",
        "sortdir": "NONE"
        }
    ],
    "settings": [
        {
        "name": "consolidationtype",
        "value": "ACCTTYPE"
        },
        {
        "name": "includeperiodendtransactions",
        "value": "F"
        }
    ]
    }
    '''

    sofxadj_df = ns.runSearch(ss, max_rows=50000)

    """### Sales Invoice (si_by_so_df, si_by_so_itemgroup_df)"""

    ss = '''
    {
    "type": "invoice",
    "filters": [
        {
        "name": "type",
        "operator": "anyof",
        "values": [
            "CustInvc"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "taxline",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "quantity",
        "operator": "isnotempty",
        "values": [],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "posting",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "join": "custbody_linked_sales_order",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        }
    ],
    "columns": [
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "so_id",
        "type": "text",
        "formula": "TO_CHAR({custbody_linked_sales_order.internalid})",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "item_group",
        "type": "text",
        "formula": "case when {item.custitem_item_group} is not null then {item.custitem_item_group}  else 'No-Item-Group' end",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_amount_net_notax_base",
        "type": "currency",
        "formula": "{netamountnotax}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_amount_gross_notax_base",
        "type": "currency",
        "formula": "{grossamount}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_discount_base",
        "type": "currency",
        "formula": "{grossamount} - {netamountnotax}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_tax_base",
        "type": "currency",
        "formula": "{taxitem.rate} * {netamountnotax} / 100",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_amount_net_tax_base",
        "type": "currency",
        "formula": "{netamountnotax} * ( 100 +{taxitem.rate})/100 ",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_amount_net_notax_tran",
        "type": "currency",
        "formula": "{netamountnotax} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_amount_gross_notax_tran",
        "type": "currency",
        "formula": "{grossamount} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_discount_tran",
        "type": "currency",
        "formula": "({grossamount} - {netamountnotax}) / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_tax_tran",
        "type": "currency",
        "formula": "{taxitem.rate} * {netamountnotax} / 100/{exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_amount_net_tax_tran",
        "type": "currency",
        "formula": "{netamountnotax} * ( 100 +{taxitem.rate})/100  / {exchangerate}",
        "sortdir": "NONE"
        }
    ],
    "settings": [
        {
        "name": "consolidationtype",
        "value": "NONE"
        },
        {
        "name": "includeperiodendtransactions",
        "value": "F"
        }
    ]
    }
    '''

    si_by_so_itemgroup_df = ns.runSearch(ss, max_rows=50000)

    ss = '''
    {
    "type": "invoice",
    "filters": [
        {
        "name": "type",
        "operator": "anyof",
        "values": [
            "CustInvc"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "taxline",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "posting",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "join": "custbody_linked_sales_order",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        }
    ],
    "columns": [
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "so_id",
        "type": "text",
        "formula": "TO_CHAR({custbody_linked_sales_order.internalid})",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_overall_amount_net_notax_base",
        "type": "currency",
        "formula": "{totalamount} - {taxtotal}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_overall_tax_base",
        "type": "currency",
        "formula": "{taxtotal}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_overall_amount_net_tax_base",
        "type": "currency",
        "formula": "{totalamount}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_overall_amount_net_notax_tran",
        "type": "currency",
        "formula": "({totalamount} - {taxtotal}) / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_overall_tax_tran",
        "type": "currency",
        "formula": "{taxtotal} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "si_overall_amount_net_tax_tran",
        "type": "currency",
        "formula": "{totalamount} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "MIN",
        "label": "si_overall_tran_count",
        "type": "text",
        "formula": "COUNT(distinct{internalid})",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "MIN",
        "label": "si_overall_tranid",
        "type": "text",
        "formula": "NS_CONCAT( DISTINCT({number}) )",
        "sortdir": "NONE"
        }
    ],
    "settings": [
        {
        "name": "consolidationtype",
        "value": "NONE"
        },
        {
        "name": "includeperiodendtransactions",
        "value": "F"
        }
    ]
    }
    '''

    si_by_so_df = ns.runSearch(ss, max_rows=50000)

    """### Credit Note (cn_by_itemgroup_df, cn_by_so_df)"""

    ss = '''
    {
    "type": "creditmemo",
    "filters": [
        {
        "name": "type",
        "operator": "anyof",
        "values": [
            "CustCred"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "taxline",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "quantity",
        "operator": "isnotempty",
        "values": [],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "posting",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "join": "custbody_linked_sales_order",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        }
    ],
    "columns": [
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "so_id",
        "type": "text",
        "formula": "TO_CHAR({custbody_linked_sales_order.internalid})",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "item_group",
        "type": "text",
        "formula": "case when {item.custitem_item_group} is not null then {item.custitem_item_group}  else 'No-Item-Group' end",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_amount_net_notax_base",
        "type": "currency",
        "formula": "{netamountnotax}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_tax_base",
        "type": "currency",
        "formula": "{taxitem.rate} * {netamountnotax} / 100",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_amount_net_tax_base",
        "type": "currency",
        "formula": "{netamountnotax} * ( 100 +{taxitem.rate})/100 ",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_amount_net_notax_tran",
        "type": "currency",
        "formula": "{netamountnotax} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_tax_tran",
        "type": "currency",
        "formula": "{taxitem.rate} * {netamountnotax} / 100/{exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_amount_net_tax_tran",
        "type": "currency",
        "formula": "{netamountnotax} * ( 100 +{taxitem.rate})/100  / {exchangerate}",
        "sortdir": "NONE"
        }
    ],
    "settings": [
        {
        "name": "consolidationtype",
        "value": "NONE"
        },
        {
        "name": "includeperiodendtransactions",
        "value": "F"
        }
    ]
    }
    '''

    cn_by_so_itemgroup_df = ns.runSearch(ss, max_rows=50000)

    ss = '''
    {
    "type": "creditmemo",
    "filters": [
        {
        "name": "type",
        "operator": "anyof",
        "values": [
            "CustCred"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "taxline",
        "operator": "is",
        "values": [
            "F"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "posting",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        },
        {
        "name": "mainline",
        "join": "custbody_linked_sales_order",
        "operator": "is",
        "values": [
            "T"
        ],
        "isor": false,
        "isnot": false,
        "leftparens": 0,
        "rightparens": 0
        }
    ],
    "columns": [
        {
        "name": "formulatext",
        "summary": "GROUP",
        "label": "so_id",
        "type": "text",
        "formula": "TO_CHAR({custbody_linked_sales_order.internalid})",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_overall_amount_net_notax_base",
        "type": "currency",
        "formula": "{totalamount} - {taxtotal}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_overall_tax_base",
        "type": "currency",
        "formula": "{taxtotal}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_overall_amount_net_tax_base",
        "type": "currency",
        "formula": "{totalamount}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_overall_amount_net_notax_tran",
        "type": "currency",
        "formula": "({totalamount} - {taxtotal}) / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_overall_tax_tran",
        "type": "currency",
        "formula": "{taxtotal} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulacurrency",
        "summary": "SUM",
        "label": "cn_overall_amount_net_tax_tran",
        "type": "currency",
        "formula": "{totalamount} / {exchangerate}",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "MIN",
        "label": "cn_overall_tran_count",
        "type": "text",
        "formula": "COUNT(distinct{internalid})",
        "sortdir": "NONE"
        },
        {
        "name": "formulatext",
        "summary": "MIN",
        "label": "cn_overall_tranid",
        "type": "text",
        "formula": "NS_CONCAT( DISTINCT({number}) )",
        "sortdir": "NONE"
        }
    ],
    "settings": [
        {
        "name": "consolidationtype",
        "value": "NONE"
        },
        {
        "name": "includeperiodendtransactions",
        "value": "F"
        }
    ]
    }
    '''

    cn_by_so_df = ns.runSearch(ss, max_rows=50000)

    """### Revenue Plan and Time based"""

    ## Revenue Plan (rp_df)
    sql = '''
    select
    RA.custbody_linked_sales_order as so_id,
    TO_CHAR(AP.startDate, 'YYYY-MM') as rp_posting_period,
    BUILTIN.DF (I.custitem_item_group) as item_group,
    SUM(PR.amount) as rp_amount_base
    from
    revenuePlan as RP
    inner join revenuePlanPlannedRevenue as PR on PR.revenuePlan = RP.id
    inner join accountingPeriod as AP on AP.id = PR.postingPeriod
    inner join revenueElement as RE on RE.id = RP.createdFrom
    inner join item as I on I.id = RE.item
    inner join transactionLine as TL on TL.revenueElement = RE.id
    inner join transaction as RA on RA.id = TL.transaction
    where
    RA.custbody_linked_sales_order is not null
    group by
    RA.custbody_linked_sales_order,
    AP.startDate,
    BUILTIN.DF (I.custitem_item_group)
    '''
    rp_df = ns.runSuiteQLPaged(sql, max_rows=50000)

    sql = '''
    select
    RAT.custbody_linked_sales_order as so_id,
    BUILTIN.DF (I.custitem_item_group) as item_group,
    I.fullName as tb_rp_item_name,
    MIN(RP.amount) as tb_rp_amount_base,
    MAX(PR.totalrecognized) as tb_pr_max_base,
    MAX(PR.percenttotalrecognized) as tb_pr_pct_max,
    TO_CHAR(RP.revrecstartdate, 'YYYY-MM-DD') as tb_pr_start_date,
    TO_CHAR(RP.revrecenddate, 'YYYY-MM-DD') as tb_pr_end_date,
    RAT.tranId as tb_ra_tran_id,
    RP.recordNumber as tb_rp_record_number,
    from
    revenuePlan as RP
    inner join item as I on I.id = RP.item
    inner join revenueElement as RE on RE.id = RP.createdFrom
    inner join transaction as RAT on RAT.id = RE.revenueArrangement
    inner join revenuePlanPlannedRevenue as PR on PR.revenuePlan = RP.id
    where
    RP.revenueRecognitionRule = 7 --time based
    -- and RAT.custbody_linked_sales_order = 4378
    group by
    RAT.custbody_linked_sales_order,
    BUILTIN.DF (I.custitem_item_group),
    I.fullName,
    RP.revrecstartdate,
    RP.revrecenddate,
    RAT.tranId,
    RP.recordNumber
    '''
    tb_df = ns.runSuiteQLPaged(sql, max_rows=50000)
    tb_df['item_group'] = tb_df.item_group.fillna('No-Item-Group')

    """### GR (gr_df, gr_loc_df)"""

    sql = '''
    SELECT
    T.custbody_linked_sales_order as so_id,
    BUILTIN.DF (TL.location) as location,
    BUILTIN.DF (I.custitem_item_group) as item_group,
    SUM(TL.rateAmount * TL.quantity) as total_gr_base
    from
    Transaction as T
    INNER JOIN TransactionLine as TL on TL.transaction = T.id
    INNER JOIN Item as I on I.id = TL.item
    where
    T.type = 'ItemRcpt'
    and TL.taxLine = 'F'
    and TL.mainLine = 'F'
    and T.posting = 'T'
    and I.itemType = 'InvtPart'
    GROUP BY
    custbody_linked_sales_order,
    BUILTIN.DF (I.custitem_item_group),
    BUILTIN.DF (TL.location)
    '''
    gr_loc_df = ns.runSuiteQLPaged(sql, max_rows=50000) ## with location
    gr_df = gr_loc_df.loc[:, ['so_id','item_group','total_gr_base']].groupby(by=['so_id','item_group']).sum() #remove location

    """### Inventory Adjustment (iadj)"""

    sql = '''
    SELECT
    T.custbody_linked_sales_order as so_id,
    BUILTIN.DF (I.custitem_item_group) as item_group,
    SUM(TL.rateAmount * TL.quantity) as iadj_amount_base
    from
    Transaction as T
    INNER JOIN TransactionLine as TL on TL.transaction = T.id
    INNER JOIN Item as I on I.id = TL.item
    where
    T.type = 'InvAdjst'
    and TL.taxLine = 'F'
    and TL.mainLine = 'F'
    and T.posting = 'T'
    GROUP BY
    custbody_linked_sales_order,
    BUILTIN.DF (I.custitem_item_group)
    '''
    iadj_df = ns.runSuiteQLPaged(sql, max_rows=50000)

    """### Purchase Order (po_df)"""

    sql = '''
    SELECT
    T.custbody_linked_sales_order as so_id,
    BUILTIN.DF (I.custitem_item_group) as item_group,
    SUM(TL.rateAmount * TL.quantity) as po_amount_base,
    SUM(TL.rateAmount * TL.quantityShipRecv) as po_amount_received_base,
    SUM(
        (TL.quantity - TL.quantityShipRecv) * TL.rateAmount
    ) as po_amount_open_base
    from
    Transaction as T
    INNER JOIN TransactionLine as TL on TL.transaction = T.id
    INNER JOIN Item as I on I.id = TL.item
    where
    T.type = 'PurchOrd'
    and TL.taxLine = 'F'
    and TL.mainLine = 'F'
    and T.status != 'Y'
    and custbody_linked_sales_order is not null
    GROUP BY
    custbody_linked_sales_order,
    BUILTIN.DF (I.custitem_item_group)
    '''
    po_df = ns.runSuiteQLPaged(sql, max_rows=50000)

    """### Advance To Supplier (adv_df)"""

    sql = '''
    SELECT
    SOT.id as so_id,
    T.isbookspecific as book_specific,
    SUM(AL.amount) as advance_to_supplier_base
    from
    Transaction as T
    inner join TransactionAccountingLine as AL on AL.transaction = T.id
    inner join Account as A on A.id = AL.account
    inner join TransactionLine as TL on TL.id = AL.transactionLine
    and TL.transaction = AL.transaction
    inner join Transaction as SOT on SOT.tranId = SUBSTR(TL.memo, 0, 11)
    and SOT.type = 'SalesOrd'
    where
    T.type = 'Journal'
    and T.posting = 'T'
    and A.acctnumber IN (141100) -- Advance To Supplier
    group by
    SOT.id,
    T.isbookspecific
    '''
    adv_df = ns.runSuiteQLPaged(sql, max_rows=50000)

    """### Accrued Cost (acc_df)"""

    sql = '''
    SELECT
    SOT.id as so_id,
    SUM(AL.amount) as accrued_cost
    from
    Transaction as T
    inner join TransactionAccountingLine as AL on AL.transaction = T.id
    inner join Account as A on A.id = AL.account
    inner join TransactionLine as TL on TL.id = AL.transactionLine
    and TL.transaction = AL.transaction
    inner join Transaction as SOT on SOT.tranId = SUBSTR(TL.memo, 0, 11)
    and SOT.type = 'SalesOrd'
    where
    T.type = 'Journal'
    and T.posting = 'T'
    and A.acctnumber IN (203002) -- Accrued Cost
    group by
    SOT.id
    '''
    acc_df = ns.runSuiteQLPaged(sql, max_rows=50000)

    """### Delivery Order (do_df)"""

    sql = '''
    SELECT
    T.custbody_linked_sales_order as so_id,
    BUILTIN.DF (TL.location) as location,
    BUILTIN.DF (I.custitem_item_group) as item_group,
    SUM(TL.rateAmount * TL.quantity) as do_amount_base
    from
    Transaction as T
    INNER JOIN TransactionLine as TL on TL.transaction = T.id
    INNER JOIN Item as I on I.id = TL.item
    where
    T.type = 'ItemShip'
    and TL.taxLine = 'F'
    and TL.mainLine = 'F'
    and T.posting = 'T'
    and I.itemType = 'InvtPart'
    GROUP BY
    custbody_linked_sales_order,
    BUILTIN.DF (I.custitem_item_group),
    BUILTIN.DF (TL.location)
    '''
    do_loc_df = ns.runSuiteQLPaged(sql, max_rows=50000) ## with location
    do_df = do_loc_df.loc[:, ['so_id','item_group','do_amount_base']].groupby(by=['so_id','item_group']).sum() #remove location

    """### Cost Of Sales (cos_df)"""

    sql = '''
    SELECT
    SOT.custbody_linked_sales_order as so_id,
    TO_CHAR(AP.startDate, 'YYYY-MM') as cos_posting_period,
    sum(AL.amount) as cos_amount
    from
    Transaction as T
    inner join TransactionAccountingLine as AL on AL.transaction = T.id
    inner join Account as A on A.id = AL.account
    inner join accountingPeriod as AP on AP.id = T.postingPeriod
    inner join TransactionLine as TL on TL.id = AL.transactionLine
    and TL.transaction = AL.transaction
    inner join Transaction as SOT on SOT.id= TL.createdFrom
    where
    --T.type = 'Journal'
    T.posting = 'T'
    and (
        A.acctnumber LIKE '5011%'
        or A.acctnumber LIKE '5012%'
    )
    group by
    SOT.custbody_linked_sales_order,
    AP.startDate
    '''
    cos_df = ns.runSuiteQLPaged(sql, max_rows=50000)

    """### Customer Payment (cp_df)"""

    sql = '''
    SELECT
    CP.id as cp_id,
    CP.tranId as cp_tran_id,
    TO_CHAR(CP.tranDate, 'YYYY-MM-DD') as cp_tran_date,
    NTLL.foreignAmount as cp_foreign_amt,
    SI.id as si_id,
    SI.tranId as si_tran_id,
    SI.custbody_linked_sales_order as si_so_id
    FROM
    NextTransactionLineLink AS NTLL
    inner join Transaction as CP on CP.id = NTLL.nextDoc
    inner join Transaction as SI on SI.id = NTLL.previousDoc and SI.custbody_linked_sales_order is not null
    WHERE
    NTLL.nextType = 'CustPymt'
    and NTLL.previousType = 'CustInvc'
    '''
    cp_df = ns.runSuiteQLPaged(sql, max_rows=50000)

    ## Split Current Year and Prev Years Amount
    cp_curr_year = pd.to_datetime(cp_df.cp_tran_date).dt.year == current_year
    cp_prev_year = ~cp_curr_year
    cp_df['cp_curr_year_amt_tran'] = cp_df.loc[cp_curr_year, 'cp_foreign_amt']
    cp_df['cp_prev_year_amt_tran'] = cp_df.loc[cp_prev_year, 'cp_foreign_amt']

    ## Prepare data to join with SI
    cols = ['si_id','cp_curr_year_amt_tran','cp_prev_year_amt_tran']
    cp_by_si_df = cp_df.loc[:, cols].groupby('si_id').sum()

    """# Transformation

    ### Reencoding Columns
    """

    ENCODE_NO_ITEM_GROUP = 'No-Item-Group'
    gr_loc_df['item_group'] = gr_loc_df.item_group.fillna(ENCODE_NO_ITEM_GROUP)
    iadj_df['item_group'] = iadj_df.item_group.fillna(ENCODE_NO_ITEM_GROUP)
    po_df['item_group'] = po_df.item_group.fillna(ENCODE_NO_ITEM_GROUP)
    do_loc_df['item_group'] = do_loc_df.item_group.fillna(ENCODE_NO_ITEM_GROUP)
    rp_df['item_group'] = rp_df.item_group.fillna(ENCODE_NO_ITEM_GROUP)

    """### Structural Transformation

    #### GR

    Transform locations from rows into columns
    """

    gr_loc_tf = gr_loc_df.pivot(index=['so_id','item_group'], columns='location', values='total_gr_base')
    gr_loc_tf.columns = [ c+'_gr' for c in gr_loc_tf.columns]

    """#### DO

    Transform locations from rows into columns
    """

    do_loc_tf = do_loc_df.pivot(index=['so_id','item_group'], columns='location', values='do_amount_base')
    do_loc_tf.columns = [ c+'_do' for c in do_loc_tf.columns]
    do_loc_tf.insert(0,'total_do_base',do_loc_tf.sum(axis=1, min_count=1))

    """#### Revenue Plan"""

    ## transform months to pivot
    rp_tf = rp_df.pivot(index = ['so_id','item_group'], columns='rp_posting_period', values='rp_amount_base').fillna(0)
    ## prep months data
    prev_year_cols = [c for c in rp_tf.columns if str(current_year) not in c ]
    curr_year_cols = [c for c in rp_tf.columns if str(current_year) in c ]

    ## combine previous year cols into one
    df1 = pd.DataFrame(rp_tf.loc[:, prev_year_cols].sum(axis=1), columns=['rp_prev_years']) ## combine previous years

    ## cut out current year months, fill up all future months as well
    df2 = rp_tf.loc[:, curr_year_cols] ## current year months
    months = [f"{current_year}-{i:02}" for i in range(1, 13)]
    for m in months:
        if m not in curr_year_cols:
            df2[m] = 0
    df2.columns = ['rp_'+c for c in df2.columns]

    ## merge them
    rp_tf = pd.concat([df1,df2], axis=1).reset_index() ## reset index to convert into dataframe, so that ra_tran_id will appear after joning in later section

    """#### Revenue Arrangement
    We transform the revenue plan months from row into columns. Then group the previous year into one single column.

    #### Advance To Supplier
    """

    adv_tf = adv_df.pivot(index=['so_id'], columns='book_specific', values='advance_to_supplier_base')
    adv_tf.columns = ['adv_not_specific', 'adv_specific']
    adv_tf.insert(0, 'total_adv', adv_tf.sum(axis=1, min_count=1))

    """#### COS"""

    cos_tf = cos_df.pivot(index = ['so_id'], columns='cos_posting_period', values='cos_amount').fillna(0)

    ## prep months data
    current_year = datetime.now().year
    prev_year_cols = [c for c in cos_tf.columns if str(current_year) not in c ]
    curr_year_cols = [c for c in cos_tf.columns if str(current_year) in c ]


    ## combine previous year cols into one
    df1 = pd.DataFrame(cos_tf.loc[:, prev_year_cols].sum(axis=1), columns=['cos_prev_years']) ## combine previous years

    ## cut out current year months, fill up all future months as well
    df2 = cos_tf.loc[:, curr_year_cols] ## current year months
    months = [f"{current_year}-{i:02}" for i in range(1, 13)]
    for m in months:
        if m not in curr_year_cols:
            df2[m] = 0
    df2.columns = ['cos_'+c for c in df2.columns]

    ## merge them
    cos_tf = pd.concat([df1,df2], axis=1).reset_index() ## reset index to convert into dataframe, so that ra_tran_id will appear after joning in later section

    """#### Customer Payment"""

    # ## Join to get the commission table
    # comm_temp_df = pd.merge(left=si_by_so_itemgroup_df, right=cp_by_si_df, how='left',left_on=['si_id'], right_on=['si_id'] )

    # ## Allocate Customer Payment amount by ratio of item group
    # si_sum = comm_temp_df.groupby('si_id')['si_amount_tran'].transform('sum')
    # comm_temp_df['cp_curr_year_amt_tran'] = comm_temp_df.si_amount_tran / si_sum * comm_temp_df.cp_curr_year_amt_tran
    # comm_temp_df['cp_prev_year_amt_tran'] = comm_temp_df.si_amount_tran / si_sum * comm_temp_df.cp_prev_year_amt_tran
    # comm_temp_df = comm_temp_df.drop(columns='si_id').groupby(['so_id','item_group']).sum()

    # ## Merge with Sales Order to get final Commission Table
    # commission_df = pd.merge(left=so_df, right=comm_temp_df, how='left', left_on=['so_id','item_group'], right_on=['so_id','item_group'])

    """### Joining Tables"""

    ## SO Join FX Adjustment
    result_df = pd.merge(left=so_df,     right=sofxadj_df, how='left', left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )

    # ## SO Join SI
    result_df = pd.merge(left=result_df, right=si_by_so_itemgroup_df, how='left',left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )
    result_df = pd.merge(left=result_df, right=si_by_so_df, how='left',left_on=['so_id'], right_on=['so_id'] )

    ## join CN might contain item-group that does not exist in SO
    result_df = pd.merge(left=result_df, right=cn_by_so_itemgroup_df, how='outer',left_on=['so_id','item_group'], right_on=['so_id','item_group'] )
    result_df = pd.merge(left=result_df, right=cn_by_so_df, how='left',left_on=['so_id'], right_on=['so_id'] )

    ## Join The Rest
    result_df = pd.merge(left=result_df, right=rp_tf, how='left',left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )
    result_df = pd.merge(left=result_df, right=tb_df, how='left',left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )
    result_df = pd.merge(left=result_df, right=po_df, how='left',left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )
    result_df = pd.merge(left=result_df, right=cos_tf, how='left',left_on=['so_id'], right_on=['so_id'] )
    result_df = pd.merge(left=result_df, right=adv_tf,how='left',left_on='so_id',right_on='so_id')
    result_df = pd.merge(left=result_df, right=acc_df,how='left',left_on='so_id',right_on='so_id')
    result_df = pd.merge(left=result_df, right=gr_df, how='left',left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )
    result_df = pd.merge(left=result_df, right=gr_loc_tf, how='left',left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )
    result_df = pd.merge(left=result_df, right=do_loc_tf, how='left',left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )
    result_df = pd.merge(left=result_df, right=iadj_df, how='left',left_on=['so_id','item_group'], right_on=['so_id', 'item_group'] )

    """### Calculated Columns

    #### Sales Order
    """

    loc = result_df.columns.get_loc('sofxadj_amount_base')
    result_df.insert(loc,'so_fxadjst_amount_net_notax_base',result_df.so_amount_net_notax_base.fillna(0) + result_df.sofxadj_amount_base.fillna(0))

    """#### Purchase Order"""

    loc = result_df.columns.get_loc('po_amount_open_base')
    result_df.insert(loc+1,'po_pending_amount_base',result_df.so_cost_estimate_base - result_df.po_amount_base)

    """#### Sales Invoice"""

    ## base currency
    loc = result_df.columns.get_loc('si_amount_net_tax_base')
    result_df.insert(loc+1, 'pct_billed_net_notax_base', result_df.si_amount_net_notax_base.div(result_df.so_amount_net_notax_base.replace(0,np.nan)))
    result_df.insert(loc+2, 'unbill_amount_net_notax_base', result_df.so_amount_net_notax_base - result_df.si_amount_net_notax_base)

    ## transaction currency
    loc = result_df.columns.get_loc('si_amount_net_tax_tran')
    result_df.insert(loc+1, 'pct_billed_net_notax_tran',    result_df.si_amount_net_notax_tran.div(result_df.so_amount_net_notax_tran.replace(0,np.nan)))
    result_df.insert(loc+2, 'unbill_amount_net_notax_tran', result_df.so_amount_net_notax_tran - result_df.si_amount_net_notax_tran)

    ## overall - base
    loc = result_df.columns.get_loc('si_overall_amount_net_notax_base')
    result_df.insert(loc+1, 'overall_pct_billed_net_notax_base',    result_df.si_overall_amount_net_notax_base.div(result_df.so_overall_amount_net_notax_base.replace(0,np.nan)))
    result_df.insert(loc+2, 'overall_unbill_amount_net_notax_base', result_df.so_overall_amount_net_notax_base - result_df.si_overall_amount_net_notax_base)

    # ## overall - transaction
    loc = result_df.columns.get_loc('si_overall_amount_net_notax_tran')
    result_df.insert(loc+1, 'overall_pct_billed_net_notax_tran',     result_df.si_overall_amount_net_notax_tran.div(result_df.so_overall_amount_net_notax_tran.replace(0,np.nan)))
    result_df.insert(loc+2, 'overall_unbill_amount_net_notax_tran',  result_df.so_overall_amount_net_notax_tran - result_df.si_overall_amount_net_notax_tran)

    """#### Time Based"""

    from datetime import datetime
    now = pd.Timestamp(datetime.now())
    current_year = datetime.now().year

    start_dates = pd.to_datetime(result_df.tb_pr_start_date)
    end_dates = pd.to_datetime(result_df.tb_pr_end_date)

    ## total from start to end
    total_days = (end_dates-start_dates).dt.days + 1
    loc = result_df.columns.get_loc('tb_pr_end_date')
    result_df.insert(loc+1,'tb_total_days', total_days)

    ## total start till today
    result_df.insert(loc+2,'tb_total_rec_days',0)
    started = start_dates < now
    not_started = start_dates > now
    not_matured = now < end_dates
    matured = now >= end_dates
    days_start_to_now = (now - start_dates).dt.days +1
    days_start_to_end = (end_dates - start_dates).dt.days +1
    result_df.loc[ not_matured & started, 'tb_total_rec_days'] = days_start_to_now
    result_df.loc[ matured & started,     'tb_total_rec_days'] = days_start_to_end
    result_df.loc[ not_started,           'tb_total_rec_days'] = None
    ## YTD
    result_df.insert(loc+3,'tb_ytd_rec_days',0)

    ## insert estimated rec pct
    loc = result_df.columns.get_loc('tb_pr_pct_max')
    result_df.insert(loc+1,'tb_estimated_overall_rec_pct', result_df.tb_total_rec_days.div(result_df.tb_total_days.replace(0,np.nan)))
    result_df.insert(loc+2,'tb_pct_rec_gap', result_df.tb_pr_pct_max - result_df.tb_estimated_overall_rec_pct)

    first_date_of_current_year = pd.Timestamp(datetime(current_year, 1, 1))
    start_prev_year = start_dates <  first_date_of_current_year
    start_this_year = start_dates >= first_date_of_current_year
    end_after_first_day_of_year = (first_date_of_current_year < end_dates)

    #### matured already
    result_df['tb_ytd_rec_days'] = 0
    result_df.loc[not_started, 'tb_ytd_rec_days'] = None
    result_df.loc[end_after_first_day_of_year & start_prev_year & matured ,    'tb_ytd_rec_days'] = (end_dates - first_date_of_current_year).dt.days +1
    result_df.loc[end_after_first_day_of_year & start_prev_year & not_matured, 'tb_ytd_rec_days'] = (now       - first_date_of_current_year).days +1
    result_df.loc[end_after_first_day_of_year & start_this_year & matured ,    'tb_ytd_rec_days'] = (end_dates - start_dates).dt.days +1
    result_df.loc[end_after_first_day_of_year & start_this_year & not_matured, 'tb_ytd_rec_days'] = (now       - start_dates).dt.days +1

    #### ytd revenue
    loc = result_df.columns.get_loc('tb_rp_item_name')
    # result_df.insert(loc,'ytd_tb_revenue', (result_df.so_amount_net_notax_base * result_df.tb_ytd_rec_days).div(result_df.total_days.replace(0,np.nan)))

    """#### Revenue and Backlog"""

    ## insert rp_amount_base, rp_ytd_base
    loc = result_df.columns.get_loc('rp_prev_years')
    result_df.insert(loc+1, 'rp_ytd_base', result_df.filter( like='rp_20',  axis='columns').sum(axis=1, min_count=1))
    result_df.insert(loc, 'rp_amount_base', result_df.rp_ytd_base + result_df.rp_prev_years)

    ## insert backlog, ca, cl
    loc = result_df.columns.get_loc('tb_rp_item_name')

    ## backlog (so + cn + sofxadj - revenue)
    backlog_revenue_base = result_df.so_fxadjst_amount_net_notax_base.fillna(0) + result_df.cn_amount_net_notax_base.fillna(0) - result_df.rp_amount_base.fillna(0)
    result_df.insert(loc, 'backlog_revenue_base', backlog_revenue_base)

    ## contract asset and liability
    ca_or_cl = result_df.rp_amount_base.fillna(0) - result_df.si_amount_net_notax_base.fillna(0)
    result_df.insert(loc+1, 'contract_asset_base', ca_or_cl[ca_or_cl>0])
    result_df.insert(loc+2, 'contract_liability_base', ca_or_cl[ca_or_cl<0]*-1)

    """#### COS"""

    loc = result_df.columns.get_loc('cos_prev_years')
    result_df.insert(loc+1, 'cos_ytd_base', result_df.filter( like='cos_20',  axis='columns').sum(axis=1, min_count=1))
    result_df.insert(loc, 'total_cos_base', result_df.cos_ytd_base + result_df.cos_prev_years)

    """#### Inventory"""

    ## fillna is necessary so that sum won't omit it when encoutering null value
    result_df['total_inventory_base'] = result_df.iadj_amount_base.fillna(0) + result_df.total_do_base.fillna(0) + result_df.total_gr_base.fillna(0)

    """# Final Result

    ## Final Treatment
    """

    ## Closed Sales Order Treatment
    closed_so = result_df.status == 'Closed'
    result_df.loc[closed_so, 'backlog_revenue_base':'contract_liability_base' ] = None ## BL/CA/CL
    result_df.loc[closed_so, 'po_pending_amount_base'] = None  ## PO

    """## Data Cleaning

    ### Treat Empty Values
    """

    ## we don't touch the good data of result_df, make a copy first
    df = result_df.copy()

    ## division of zero gives -np.inf, which will hit error updating googlesheet
    df = df.replace(np.inf,None).replace(-np.inf,None) ## replace infinite as 0
    df = df.replace(0,None)  ## make all 0 cells empty

    """### Remove Duplicates Cells"""

    ## [SO, Item] Level Dups
    ########################
    df['item_group'] = pd.Categorical(df.item_group, categories=['No-Item-Group', 'Turnkey', 'Hardware', 'Software', 'Service', 'Support', 'Maintenance'], ordered=True)
    df = df.sort_values(by=['so_id','item_group'])
    so_item_dups = df.duplicated(subset=['so_id','item_group'])

    ## sales order
    df.loc[so_item_dups, 'item_group': 'so_overall_approved_gpm_pct_tran'] = None
    df.loc[so_item_dups, 'so_id':'contract_liability_base'] = None
    df.loc[so_item_dups, 'total_inventory_base'] = None

    ## credit note
    df.loc[so_item_dups, 'cn_amount_net_notax_base':'cn_amount_net_tax_tran'] = None

    gr_fields = [ c for c in df.columns if 'gr_' in c]
    do_fields = [ c for c in df.columns if 'do_' in c]
    df.loc[so_item_dups, gr_fields] = None
    df.loc[so_item_dups, do_fields] = None

    ## PO
    df.loc[so_item_dups, 'po_amount_base':'po_pending_amount_base'] = None

    ## [SO] Level Dups
    ###################
    so_dups = df.duplicated(subset=['so_id'])

    ## sales order
    df.loc[so_dups, 'so_id':'sales_rep'] = None
    df.loc[so_dups, ['base_curr','tran_curr','exchange_rate']] = None
    df.loc[so_dups, 'so_overall_amount_net_notax_base':'so_overall_gpm_pct_gap'] = None

    ## credit notes
    df.loc[so_dups, 'cn_overall_amount_net_notax_base':'cn_overall_tranid' ] = None

    ## sales invoice
    df.loc[so_dups, 'si_overall_amount_net_notax_base':'si_overall_tranid'] = None

    ## GL Journal
    df.loc[so_dups, ['adv_not_specific','adv_specific','total_adv','accrued_cost']] = None
    cos_fields = [ c for c in df.columns if 'cos_' in c]
    df.loc[so_dups, cos_fields] = None

    """# Update Google Sheet

    ## Connect to Google Sheet API
    """

    ## gspread client
    import gspread, json
    from gspread_dataframe import set_with_dataframe
    credential = json.loads(gcp_sa)
    gc = gspread.service_account_from_dict(credential)

    ## pgsheet client
    import pygsheets
    pgc = pygsheets.authorize(service_account_json=gcp_sa)

    """## Update Development Sheet"""

    ## Update Development Sheet
    print('Updating Dev Googlesheet: ')

    ## get the dev sheet
    dev_sheet = '1F4V21aOGWcABowGWFsayQNUyA5hlKVBZE79Khm4H8Tw'
    dest_spreadsheet = gc.open_by_key(dev_sheet)
    sheet = dest_spreadsheet.worksheet('dev')

    ## Update rows
    set_with_dataframe(sheet, df, include_column_header=True, row=5, col=1, resize=True )

    ## Update header timestamp
    utc_now = datetime.now(pytz.utc)
    sgt_now = utc_now.astimezone(pytz.timezone('Asia/Singapore'))
    datestr = sgt_now.strftime('%d %B %Y, %H:%M')
    ts = 'FRS Report. Updated: ' + datestr
    sheet.update_acell('A1',ts)

    ## Call AppScript to Format The Development Sheet
    params = {'function': 'main_ReformatDevSheet'}
    response = requests.get(appscript_webapp_url, params=params)
    print('GET request response:', response.text)

    """## Update Subsidiary Sheets"""

    dev_mode = False

    if not dev_mode:

        dev_spreadsheet_id = '1F4V21aOGWcABowGWFsayQNUyA5hlKVBZE79Khm4H8Tw'
        drive_id = '0AMDjN1IvNSY_Uk9PVA'
        template_sheet = gc.open_by_key(dev_spreadsheet_id).worksheet('header_template')
        sheets_to_keep = 3

        ## Subsidiary, sheet id, folder id
        subsidiaries = [
            ('NTL','1YakDKRg-Ls_loSLzgEj63aHKI__cvBXx4jYhizXgphM','1MM_omGoIt_IccFdJ2E1xbzMez9hUchXI'),
            ('NNS','19LKYjH0Sb_6QtqHrT0TLar13SLQp668dLlp_N9i8lJU','1OsSmskrj6aDu2ZvR694x5vV8kMZYO7Vz'),
            ('NUA','1OnACuzF0fRUV-q5IQ9TpyhRT__mrTWU88Jy9ZXTZHHE','1jAB9AWmHed_AwOybwne0gvcVePEEl-zm'),
            ('NID','1nid-yaaFdC0oE-4DaLDOHz4Z2FmE6l5IfjHVUablpv4','1s9ekqBf5L8BhIGwr8jmb9C-MDg2tIfnp'),
            ('NNM','1DrLtA56XHJrh5p4zG3V2af2ZkGzFrI2BwQi78NhyUlM','1vjI6XWAhqHtiLirADYWjTB7Qsq8Lk_b6'),
            ('NTH','1qu2CaKDONHWrpP9rNwNKJPs5UZlBzz1ddmrkeVbyqck','1TVo2AjaSFDO0y6B-zbXip15IOLA8yW6S'),
            ('NIF','1aEo2J3JLDZWwyAa_eIq8DvwYGhqimgYRk0jGmAEhMKw','1BNiHGVbwBEtrMX_KLd20mEilOAR34soJ'),
            ('NPA','1UeTXIIRylzlqMKd3VHis4HanoAOy92EAjbIsbBzQesM','1mAzx7Vh0GL1G7grKdal7Hp2RzDlnvJJy'),
            ('NMM','1DyelUFDEizkK_y8pIMV8UAmhqTfWQEyLif7mTN_qvBo','17RLmeYo8dUkXTO-rlu7jPfpFp4m2rud4')
        ]


        ## Delete Duplicate Sheet (with today's date)
        #############################################
        for sub in subsidiaries:
            dest_spreadsheet = gc.open_by_key(sub[1])
            try:
                ## delete sheet with the same name
                old_sheet = dest_spreadsheet.worksheet(today_iso)
                dest_spreadsheet.del_worksheet(old_sheet)
                print(f"Duplicate sheet name '{today_iso}' in {sub[0]} deleted.")
            except: pass
        #time.sleep(10)

        ## Create New Sheet (with today's date)
        #############################################
        for sub in subsidiaries:
            dest_spreadsheet = gc.open_by_key(sub[1])
            ## create new sheet for today
            copy_to_dict = template_sheet.copy_to(dest_spreadsheet.id)
            dest_sheet = dest_spreadsheet.get_worksheet_by_id(copy_to_dict.get('sheetId'))
            dest_sheet.update_title(today_iso)
            dest_sheet.update_index(0)
            print(f"New sheet name '{today_iso}' created in {sub[0]}.")
        #time.sleep(10)

        ## Archive Last Sheets
        #######################
        for sub in subsidiaries:

            ## open subsidiary spreadsheet
            dest_spreadsheet = gc.open_by_key(sub[1])

            ## Archiving Last Sheet
            #######################
            all_worksheets = dest_spreadsheet.worksheets()
            if len(all_worksheets) > sheets_to_keep:

                last_sheet = all_worksheets[-1]

                print(f'Last sheet name {last_sheet.title} in {sub[0]} need archiving.')

                archive_spredsheet = None
                ## Discover spreadsheets inside the subsidiary folder with today's date
                ss_files_in_folder = pd.DataFrame(pgc.drive.list(
                    corpora='drive',
                    driveId=drive_id,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    pageSize=1000,
                    q=f"'{sub[2]}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"))

                ## Delete Archive Sheet With Today's Date
                if (not ss_files_in_folder.empty):
                    found_files = ss_files_in_folder.query(f'name=="{last_sheet.title}"')

                    ## delete sheet in archive spreadsheet  if found, since we are re-creating the sheet
                    if len(found_files)>0:
                        spreadsheet_id = found_files.id.iloc[0]
                        archive_spredsheet = gc.open_by_key(spreadsheet_id)
                        print(f"... archieve spreadhseet '{last_sheet.title}' found.")

                        ## delete sheet with the same name
                        try:
                            archive_sheet = archive_spredsheet.worksheet(last_sheet.title)
                            archive_spredsheet.del_worksheet(archive_sheet)
                            time.sleep(6)
                            print(f"... archive spreadsheet: sheet name '{last_sheet.title}' deleted.")
                        except: pass

                ## Move Last Sheet to Archive Spreadsheet if eixst, else create new archive spread sheet
                if archive_spredsheet:
                    new_spreadsheet = archive_spredsheet
                else:
                    new_spreadsheet = gc.create(title=last_sheet.title, folder_id=sub[2])
                    print(f"... new archive spreadsheet '{last_sheet.title}' created.")

                copy_to_dict = last_sheet.copy_to(new_spreadsheet.id)
                archive_sheet = new_spreadsheet.get_worksheet_by_id(copy_to_dict.get('sheetId'))
                archive_sheet.update_title(last_sheet.title)
                archive_sheet.update_index(0)
                print(f"... last sheet '{last_sheet.title}' copied to archive spreadsheet '{new_spreadsheet.title}'.")

                # Delete the last sheet
                dest_spreadsheet.del_worksheet(last_sheet)
                print(f"... last sheet '{last_sheet.title}' deleted.")

        ## Populate Today's Data
        ########################
        for sub in subsidiaries:
            dest_spreadsheet = gc.open_by_key(sub[1])
            dest_sheet = dest_spreadsheet.worksheet(today_iso)
            is_subsidiary = result_df.subsidiary == sub[0]
            subsidiary_df = df.loc[is_subsidiary, :]
            set_with_dataframe(dest_sheet, subsidiary_df, include_column_header=True, row=5, col=1, resize=True )
            dest_sheet.update_acell('A1',ts)
            print(f'Updated sheet name {today_iso} in {sub[0]}.')

    print('COMPLETED')

    ## Call AppScript to Format The Sheets

    if not dev_mode:
        # Function to make a GET request
        params = {'function': 'main_ReformatCountrySheets'}
        response = requests.get(appscript_webapp_url, params=params)
        print('GET request response:', response.text)

    """# END"""

    ## End Stop Watch
    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes, seconds = divmod(elapsed_time, 60)

    print(f"Elapsed time: {int(minutes)} minutes and {int(seconds)} seconds")




if __name__ == "__main__":
    main()


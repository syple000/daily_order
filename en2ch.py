import pandas as pd
import warnings

# 为了中文易于理解，临时脚本

def detail_en2ch(path: str):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df = pd.read_excel(path, dtype=str)
    df = df.rename(columns={
        'SubTradeId': '子订单编号',
        'TradeId': '订单编号',
        'OrderStatus': '订单状态',
        'Amount': '数量',
        'SkuName': 'Sku名字',
        'LinkId': '链接Id',
        'OrderCreatedTime': '订单创建时间',
        'ExpressNo': '快递号',
        'ShippingStatus': '退款单发货状态',
        'RefundStatus': '退款状态',
        'FakeTrade': '是否刷单',
        'FacPayment': '货款'
    }) 
    df = df[['子订单编号', '订单编号', '订单状态', '数量', 'Sku名字', '链接Id', '订单创建时间', '快递号', '退款单发货状态', '退款状态', '是否刷单', '货款']]
    l = path.split('.')
    df.to_excel('.'.join(l[:len(l)-1]) + 'CN.xlsx', index=False)

def facpayment_en2ch(path: str):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df = pd.read_excel(path, dtype=str)
    df = df.rename(columns={
        'LinkId': '链接Id',
        'SkuName': 'Sku名字',
        'OnRoadFacPayment': '待发货',
        'Count': '数量',
        'FacPayment': '货款',
    }) 
    df = df[['链接Id', 'Sku名字', '待发货', '数量', '货款']]
    l = path.split('.')
    df.to_excel('.'.join(l[:len(l)-1]) + 'CN.xlsx', index=False)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser('')
    parser.add_argument('--detail-file', type=str)
    parser.add_argument('--facpayment-file', type=str)
    args = parser.parse_args()
    detail_en2ch(args.detail_file)
    facpayment_en2ch(args.facpayment_file)
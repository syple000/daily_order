import pandas as pd
import warnings

# 为了中文易于理解，临时脚本
# 同时关联店管家发货数据

def load_target_tradeids(): # 店管家中目标id。手动拷贝
    path = 'output/store_manager.xlsx'
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df = pd.read_excel(path, dtype=str)
    return set(df['订单编号'].to_list())

def detail_en2ch(path: str):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df = pd.read_excel(path, dtype=str)

    target_tradeids = load_target_tradeids()
    df['Retain'] = df['TradeId'].apply(lambda x: x in target_tradeids)
    df = df[df['Retain']]

    # 打印店管家中有，但导出不存在的订单编号
    ids = set(df['TradeId'].to_list())
    for id in target_tradeids:
        if id not in ids:
            print('店管家订单编号：{} 没有找到'.format(id))

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
    df.to_excel('.'.join(l[:len(l)-1]) + '-发货视角.xlsx', index=False)

    total = round(df['货款'].apply(lambda x: float(x)).sum(), 2)
    with open('.'.join(l[:len(l)-1]) + '-发货视角-汇总.txt', 'w') as f:
        f.write('发货总货款: {}'.format(total))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser('')
    parser.add_argument('--detail-file', type=str)
    args = parser.parse_args()
    detail_en2ch(args.detail_file)
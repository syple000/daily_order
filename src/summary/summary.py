import pandas as pd
import numpy as np
import json
import datetime
import subprocess as sub
import os
import warnings

class Summary(object):
    def __init__(self, report_df: pd.DataFrame, ad_charge: float):
        link_sku_table_filepath = 'resource/link_sku_table.csv'
        fake_trade_filepath = 'resource/fake_trade.json'

        self._report_df = report_df
        self._ad_charge = ad_charge

        # 迭代新价格关联，自动计算价格表
        price_filepath = 'resource/price.xlsx'
        link_sheet_filepath = 'resource/link_sheet.json'
        with open(link_sheet_filepath) as f:
            link_sheet_conf = json.load(f)

        # report df的sku名称需要纠正，如果使用v2
        def sku_name(linkid, v1, v2):
            if linkid in link_sheet_conf and 'use_v2' in link_sheet_conf[linkid] and link_sheet_conf[linkid]['use_v2']:
                return v2
            else:
                return v1
        self._report_df['SkuName'] = self._report_df[['LinkId', 'SkuName', 'SkuNameV2']].apply(lambda x: sku_name(x['LinkId'], x['SkuName'], x['SkuNameV2']), axis=1)
        
        dfs = []
        for link in link_sheet_conf.keys():
            start_row = link_sheet_conf[link]['start_row']
            end_row = link_sheet_conf[link]['end_row']
            sheet = link_sheet_conf[link]['sheet']
            df = None
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                df = pd.read_excel(price_filepath, sheet_name=sheet, usecols=[0, 2, 3, 6, 10], dtype=str, skiprows=start_row, nrows=end_row-start_row, header=None)
            df.columns = ['SkuName', 'Cost', 'PostFee', 'Price', 'FacName']
            df[['Cost', 'PostFee', 'Price']] = df[['Cost', 'PostFee', 'Price']].astype(float)
            df['LinkId'] = link
            if df['FacName'].isnull().any(): # 如果有空值，赋值配置中的厂家名
                df['FacName'] = link_sheet_conf[link]['fac_name']
            else:
                print('厂家名已经配置：{}'.format(sheet))
            if 'rename_cols' in link_sheet_conf[link]:
                df['SkuName'] = df['SkuName'].apply(lambda x: link_sheet_conf[link]['rename_cols'][x] if x in link_sheet_conf[link]['rename_cols'] else x)
            if 'replace_char' in link_sheet_conf[link]:
                rm = link_sheet_conf[link]['replace_char']
                for k in rm.keys():
                    df['SkuName'] = df['SkuName'].apply(lambda x: x.replace(k, rm[k]))
            df['SkuName'] = df['SkuName'].apply(lambda x: x.strip())
            dfs.append(df)
        self._link_sku_table = pd.concat(dfs)
       
        if self._link_sku_table.isnull().any().any():
            # 一般是因为index填写错误
            raise Exception('link sku table null data: {}'.format(self._link_sku_table.isnull().any()))
        link_sku_id_df = self._link_sku_table.groupby(['LinkId', 'SkuName']).size().reset_index(name='Count')
        if len(link_sku_id_df[link_sku_id_df['Count'] != 1]) != 0:
            raise Exception('link sku table link sku duplicate: {}'.format(link_sku_id_df[link_sku_id_df['Count'] != 1]))

        with open(fake_trade_filepath) as f:
            self._fake_trade = set(json.load(f))
    
    def dumpLinkSkuTable(self, filepath: str) -> str: # 请不要调用，测试用
        df = self._report_df.groupby(['LinkId', 'SkuName']).agg({'Amount': 'sum'}).reset_index()[['LinkId', 'SkuName']]
        df['Cost'] = 4
        df['PostFee'] = 2
        df['Price'] = 7
        df = df.sort_values(['LinkId', 'SkuName'])
        df.to_csv(filepath, index=False)
        return filepath

    def dumpArchive(self, df: pd.DataFrame, dir: str):
        sub.check_call('mkdir -p {} & rm -rf {}/*'.format(dir, dir), shell=True) 
        # 落详情
        df.to_excel(os.path.join(dir, '详情.xlsx'))
        # 按链接&sku计算货款（排除刷单）
        facpay_count_df = df[df['FakeTrade'] == False].groupby(['LinkId', 'SkuName', 'OnRoadFacPayment']).size().reset_index(name='Count')
        facpay_amount_df = df[df['FakeTrade'] == False].groupby(['LinkId', 'SkuName', 'OnRoadFacPayment']).agg({
            'FacPayment': 'sum',
            'SubActualTotalFee': 'sum',
            'RefundFee': 'sum'
        }).reset_index()
        facpay_df = pd.merge(facpay_count_df, facpay_amount_df, how='left', on=['LinkId', 'SkuName', 'OnRoadFacPayment'])
        facpay_df = facpay_df.sort_values(['OnRoadFacPayment', 'Count', 'LinkId', 'SkuName'], ascending=False)
        facpay_df['FacPayment'] = facpay_df['FacPayment'].apply(lambda x: round(x, 2))
        facpay_df.to_excel(os.path.join(dir, '货款.xlsx'))
        # 按厂家计算货款
        facpay_count_df2 = df[df['FakeTrade'] == False].groupby(['FacName', 'OnRoadFacPayment']).size().reset_index(name='Count')
        facpay_amount_df2 = df[df['FakeTrade'] == False].groupby(['FacName', 'OnRoadFacPayment']).agg({
            'FacPayment': 'sum',
            'SubActualTotalFee': 'sum',
            'RefundFee': 'sum'
        }).reset_index()
        facpay_df2 = pd.merge(facpay_count_df2, facpay_amount_df2, how='left', on=['FacName', 'OnRoadFacPayment'])
        facpay_df2.to_excel(os.path.join(dir, '厂家货款.xlsx'))
        # 总共货款
        with open(os.path.join(dir, '总货款.txt'), 'w') as f:
            f.write('总货款: {}'.format(round(facpay_df['FacPayment'].sum(), 2)))
        # 按链接&sku计算利润/刷单比例/超半数金额退款比例
        # 链接&sku 按是否包含刷单 0. 计数 1. 结算比例（trade done比例）2. 利润 3. 超半额退款比例
        df['TradeDoneCnt'] = df['TradeDone'].apply(lambda x: 1 if x else 0)
        df['HighPctRefundCnt'] = df[['SubActualTotalFee', 'RealRefundFee']].apply(lambda x: 1 if x['SubActualTotalFee'] != 0 and abs(x['RealRefundFee']/x['SubActualTotalFee']) >= 0.5 else 0, axis=1)
        
        df0_0 = df.groupby(['LinkId', 'SkuName']).size().reset_index(name='TotalCnt')
        df0_1 = df[df['FakeTrade'] == False].groupby(['LinkId', 'SkuName']).size().reset_index(name='RealTradeCnt')
        df0 = pd.merge(df0_0, df0_1, on=['LinkId', 'SkuName'], how='left')
        
        df1_0 = df.groupby(['LinkId', 'SkuName']).agg({
            'TradeDoneCnt': 'sum',
            'Profit': 'sum',
            'HighPctRefundCnt': 'sum'
        }).reset_index()
        df1_1 = df[df['FakeTrade'] == False].groupby(['LinkId', 'SkuName']).agg({
            'TradeDoneCnt': 'sum',
            'Profit': 'sum',
            'HighPctRefundCnt': 'sum'
        }).reset_index().rename(columns={'TradeDoneCnt': 'RealTradeDoneCnt', 'Profit': 'RealTradeProfit', 'HighPctRefundCnt': 'RealTradeHighPctRefundCnt'})
        df1 = pd.merge(df1_0, df1_1, on=['LinkId', 'SkuName'], how='left')
        profit_df = pd.merge(df0, df1, on=['LinkId', 'SkuName'], how='left')
        profit_df = profit_df.fillna(0)
        
        pdf = profit_df.sort_values(['LinkId', 'SkuName'], ascending=False)
        pdf['Profit'] = pdf['Profit'].apply(lambda x: round(x, 2))
        pdf.to_excel(os.path.join(dir, '利润.xlsx'))
        # 总利润
        with open(os.path.join(dir, '总利润.txt'), 'w') as f:
            f.write('总利润: {}'.format(round(pdf['Profit'].sum(), 2)))
        # 按链接&sku计算gmv/退款（排除刷单）
        gmv_refund_count_df = df[df['FakeTrade'] == False].groupby(['LinkId', 'SkuName']).size().reset_index(name='Count')
        gmv_refund_df = df[df['FakeTrade'] == False].groupby(['LinkId', 'SkuName']).agg({
            'SubActualTotalFee': 'sum',
            'RefundFee': 'sum'
        }).reset_index()
        grdf = pd.merge(gmv_refund_count_df, gmv_refund_df, how='left', on=['LinkId', 'SkuName'])
        grdf = grdf.sort_values(['LinkId', 'Count', 'SkuName'], ascending=False)
        grdf.to_excel(os.path.join(dir, 'GMV退款.xlsx'))
        # 总gmv/退款
        with open(os.path.join(dir, '总GMV退款.txt'), 'w') as f:
            f.write('总GMV: {}, 总退款: {}'.format(round(grdf['SubActualTotalFee'].sum(), 2), round(grdf['RefundFee'].sum(), 2)))


    def archive(self, start_date: str, end_date: str):
        df = self.calcProfit()

        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        if start > end:
            raise Exception('archive start > end: {}, {}'.format(start_date, end_date))

        l = []
        while True:
            date = start.strftime('%Y-%m-%d')
            subdf = df[df['OrderDate'] == date]
            self.dumpArchive(subdf.copy(), os.path.join('archive', date))
            l.append(subdf)
            start = start + datetime.timedelta(days=1)
            if start > end:
                break
        df = pd.concat(l)
        self.dumpArchive(df.copy(), os.path.join('archive', '{}To{}'.format(start_date, end_date)))
        with open(os.path.join('archive', '{}To{}'.format(start_date, end_date), '广告充值.txt'), 'w') as f:
            f.write('广告充值，利润计算未包含这部分支出：{}'.format(self._ad_charge))
        

    def calcProfit(self) -> pd.DataFrame:
        df = pd.merge(self._report_df, self._link_sku_table, how='left', on=['LinkId', 'SkuName'])
        if df['Cost'].isnull().any():
            raise Exception('link sku not found: {}'.format(df[df['Cost'].isnull()][['LinkId', 'SkuName']].groupby(['LinkId', 'SkuName']).size().reset_index(name='Count')))
        # 计算出平台基础佣金（实际付款-结算金额）
        df['PlatformCommisionFee'] = df['SubActualTotalFee'] - df['RefundFee'] - df['TotalSettleAmount']
        df['PlatformCommisionRate'] = df[['PlatformCommisionFee', 'SubActualTotalFee']].apply(lambda x: str(int(100 * x['PlatformCommisionFee'] / x['SubActualTotalFee']))+'%' if not np.isnan(x['PlatformCommisionFee']) else np.nan, axis=1)

        # 统计刷单
        df['FakeTrade'] = df['TradeId'].apply(lambda x: x in self._fake_trade)

        # 真实折扣计算（最后用户的订单支付/定价/数量）
        # 订单产生就可以计算
        df['RealDiscount'] = round(df['SubActualTotalFee'] / df['Price'] / df['Amount'], 2)

        # 货款计算。cost * amount + postfee
        # 假设多件快递不变
        # 一个大订单拆成多个子单，但子单可以合并发货，快递仍然算多个子单
        # 必须有快递才有货款。如果产生了真实发货，都算货款
        # 刷单不计算货款
        df['OnRoadFacPayment'] = df[['ExpressNo', 'OrderStatus', 'FakeTrade']].apply(lambda x: len(x['ExpressNo']) == 0 and not x['FakeTrade'] and x['OrderStatus'] != '交易关闭' and x['OrderStatus'] != '交易成功', axis=1)
        df['RealRefundFee'] = df[['ExpressNo', 'RefundFee', 'RefundStatus', 'ShippingStatus']].apply(lambda x: x['RefundFee'] if len(x['ExpressNo']) > 0 and x['RefundStatus'] == '退款成功' and x['ShippingStatus'] != '未发货' else 0, axis=1)
        post_ids = set(df['ExpressNo'].tolist())
        def post_fee(id, fee, shipping_status):
            nonlocal post_ids
            if id in post_ids:
                if shipping_status != '未发货':
                    post_ids.remove(id)
                return fee
            else:
                return 0
        df['PostFee'] = df[['ExpressNo', 'PostFee', 'ShippingStatus']].apply(lambda x: post_fee(x['ExpressNo'], x['PostFee'], x['ShippingStatus']), axis=1) # 快递费对订单编号一致的不重复计算
        df['FacPayment'] = df[['Cost', 'Amount', 'PostFee', 'ExpressNo', 'FakeTrade', 'SubActualTotalFee', 'RefundFee', 'RefundStatus', 'ShippingStatus']].apply(lambda x: x['Cost'] * x['Amount'] + x['PostFee'] if len(x['ExpressNo']) > 0 and not x['FakeTrade'] and not (abs(x['SubActualTotalFee'] - x['RefundFee']) < 0.0001 and x['RefundStatus'] == '退款成功' and x['ShippingStatus'] == '未发货') else float(0), axis=1)

        # 交易生命周期是否完成
        df['TradeDone'] = df[['SubActualTotalFee', 'RefundFee', 'TotalSettleAmount', 'OrderStatus']].apply(lambda x: not np.isnan(x['TotalSettleAmount']) or ((x['OrderStatus'] == '交易关闭' or x['OrderStatus'] == '交易成功') and abs(x['SubActualTotalFee'] - x['RefundFee']) < 0.0001), axis=1)

        # 真实利润计算
        # 结算或交易关闭后才可以计算
        # 多个结算产出可能有时间差异，不保证每一次都完全获取到数据
        # 结算价（用户支付-退款-平台佣金 TotalSettleAmount）- 先用后付（CreditBuyFee） - 营销推广（PlanFee）- 运费险（FreightInsuranceFee）- 全站（ASPCommisionFee）- 货款
        def calc_profit(x: pd.Series) -> float:
            if not x['TradeDone']:
                return np.nan
            if x['OnRoadFacPayment']:
                raise Exception('trade done but express on road')
            n = x['TotalSettleAmount']
            if np.isnan(n): # 交易关闭
                n = float(0)
            elif x['FakeTrade']: # 刷单
                n = x['TotalSettleAmount'] - x['SubActualTotalFee'] + x['RefundFee']
            
            if not np.isnan(x['CreditBuyFee']):
                n = n - x['CreditBuyFee']
            if not np.isnan(x['PlanFee']):
                n = n - x['PlanFee']
            if not np.isnan(x['FreightInsuranceFee']):
                n = n - x['FreightInsuranceFee']
            if not np.isnan(x['ASPCommisionFee']):
                n = n - x['ASPCommisionFee']

            n = n - x['FacPayment']
            return round(n, 2)
        df['Profit'] = df[['OrderStatus', 'TotalSettleAmount', 'TradeDone', 'CreditBuyFee', 'PlanFee', 'FreightInsuranceFee', 'ASPCommisionFee', 'FacPayment', 'FakeTrade', 'OnRoadFacPayment', 'SubActualTotalFee', 'RefundFee']].apply(lambda x: calc_profit(x), axis=1)
        return df
    
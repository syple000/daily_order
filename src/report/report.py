import pandas as pd
import warnings
import os
import datetime
import numpy as np

from typing import List, Dict, Any

# 以子订单维度进行聚合

class Reporter(object):
    def __init__(self) -> None: # 预期output下有数据！
        self._dir = 'output'
        self._orders_file = 'orders.xlsx'
        self._refund_orders_file = 'refund_orders.xlsx'
        self._settle_bill_file = 'settle_bill.xlsx'
        self._advertising_charge_file = 'ADVERTISING_CHARGE.xlsx'
        self._all_site_channel_promotion_file = 'ALL_SITE_CHANNEL_PROMOTION.xlsx'
        self._credit_buy_file = 'CREDIT_BUY.xlsx'
        self._freight_insurance_file = 'FREIGHT_INSURANCE.xlsx'
        self._supplier_marketing_file = 'SUPPLIER_MARKETING.xlsx'

    def adCharge(self) -> float:
        df: pd.DataFrame = self.loadXlsx(os.path.join(self._dir, self._advertising_charge_file), usecols=[
            '金额',
            '付款类型'
        ], dtype={
            '金额': float,
            '付款类型': str,
        })
        df = df.rename(columns={
            '金额': 'Cost',
            '付款类型': 'Dir',
        })
        df = df[df['Dir'] == '扣款']
        return round(df['Cost'].sum(), 2)

    def report(self) -> pd.DataFrame:
        orders = self.loadOrders()
        settle = self.loadSettleBill()
        freight_insurance = self.loadFreightInsurance()
        supplier_marketing = self.loadSupplierMarketing()
        all_site = self.loadAllSiteChannelPromotion()
        credit_buy = self.loadCreditBuy()
        refund_orders = self.loadRefundOrders()
        # 先聚合导出看看，我也不清楚后台规则
        df = pd.merge(orders, settle, how='left', on='SubTradeId')
        df = pd.merge(df, freight_insurance, how='left', on='SubTradeId')
        df = pd.merge(df, supplier_marketing, how='left', on='SubTradeId')
        df = pd.merge(df, all_site, how='left', on='SubTradeId')
        df = pd.merge(df, credit_buy, how='left', on='SubTradeId')
        df = pd.merge(df, refund_orders, how='left', on='SubTradeId')
        df = df.sort_values(['OrderCreatedTs', 'LinkId', 'SubTradeId'], ascending=False)
        return df
    
    def loadXlsx(self, path: str, usecols: List[str]=None, dtype: Dict[str, Any]=None) -> pd.DataFrame:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            dfxlsx = pd.read_excel(path, usecols=usecols, dtype=dtype)
            return dfxlsx
        raise Exception('load xlsx fail')

    def loadRefundOrders(self) -> pd.DataFrame: # 退款订单
        df: pd.DataFrame = self.loadXlsx(os.path.join(self._dir, self._refund_orders_file), usecols=[
            '子订单编号',
            '发货状态',
            '退款状态'
        ], dtype={
            '子订单编号': str,
            '发货状态': str,
            '退款状态': str, 
        })
        df = df.rename(columns={
            '子订单编号': 'SubTradeId',
            '发货状态': 'ShippingStatus',
            '退款状态': 'RefundStatus'
        })
        df = df[df['RefundStatus'] == '退款成功']
        if df.isnull().any().any():
            raise Exception('refund order null data: {}'.format(df.isnull().any()))
        id_df = df[['SubTradeId']].groupby('SubTradeId').size().reset_index(name='Count')
        abnormal_id_df = id_df[id_df['Count'] != 1]
        if len(abnormal_id_df) != 0:
            raise Exception('refund order sub trade id duplicated: {}'.format(abnormal_id_df))
        return df

    def loadCreditBuy(self) -> pd.DataFrame: # 先用后付
        df: pd.DataFrame = self.loadXlsx(os.path.join(self._dir, self._credit_buy_file), usecols=[
            '子订单号', # 对应订单的子订单编号
            '结算金额(元)', # 结算金额
        ], dtype={
            '子订单号': str, # 对应订单的子订单编号
            '结算金额(元)': float, # 结算金额
        })
        df = df.rename(columns={
            '子订单号': 'SubTradeId', # 对应订单的子订单编号
            '结算金额(元)': 'CreditBuyFee', # 结算金额
        })
        if df.isnull().any().any():
            raise Exception('credit buy null data: {}'.format(df.isnull().any()))
        id_df = df[['SubTradeId']].groupby('SubTradeId').size().reset_index(name='Count')
        abnormal_id_df = id_df[id_df['Count'] != 1]
        if len(abnormal_id_df) != 0:
            raise Exception('credit buy sub trade id duplicated: {}'.format(abnormal_id_df))
        df = df.groupby('SubTradeId').agg({
            'CreditBuyFee': 'sum'
        }).reset_index()
        return df

    def loadSettleBill(self) -> pd.DataFrame: # 仅考虑结算收益与基础佣金支出
        df: pd.DataFrame = self.loadXlsx(os.path.join(self._dir, self._settle_bill_file), usecols=[
            '交易子单', # 对应订单的子订单编号
            '总结算金额', # 结算金额
        ], dtype={
            '交易子单': str, # 对应订单的子订单编号
            '总结算金额': float, # 结算金额
        })
        df = df.rename(columns={
            '交易子单': 'SubTradeId', # 对应订单的子订单编号
            '总结算金额': 'TotalSettleAmount', # 结算金额
        })
        if df.isnull().any().any():
            raise Exception('settle bill null data: {}'.format(df.isnull().any()))
        df = df.groupby('SubTradeId').agg({
            'TotalSettleAmount': 'sum'
        }).reset_index()
        return df

    def loadSupplierMarketing(self) -> pd.DataFrame: # 活动推广支出
        df: pd.DataFrame = self.loadXlsx(os.path.join(self._dir, self._supplier_marketing_file), usecols=[
            '子订单单号',
            '营销推广套餐ID',
            '营销推广套餐名称',
            '推广费用'
        ], dtype={
            '子订单单号': str,
            '营销推广套餐ID': str,
            '营销推广套餐名称': str,
            '推广费用': float,
        })
        df = df.rename(columns={
            '子订单单号': 'SubTradeId',
            '营销推广套餐ID': 'PlanId',
            '营销推广套餐名称': 'PlanName',
            '推广费用': 'PlanFee',
        })
        if df.isnull().any().any():
            raise Exception('supplier marketing null data: {}'.format(df.isnull().any()))
        def get_plans(plan_names: pd.Series) -> List[str]:
            s = {}
            l = []
            for n in plan_names:
                if n in s:
                    continue
                s[n] = 1
                l.append(n)
            l.sort()
            return l
        trade_plan_df = df.groupby(['SubTradeId']).apply(lambda x: '|'.join(get_plans(x['PlanName'])), include_groups=False).reset_index()
        trade_plan_df.columns = ['SubTradeId', 'PlanNames']
        fee_df = df.groupby(['SubTradeId']).agg({'PlanFee': 'sum'}).reset_index()
        df = pd.merge(trade_plan_df, fee_df, how='outer', on='SubTradeId')
        return df

    def loadFreightInsurance(self): # 运费险
        df: pd.DataFrame = self.loadXlsx(os.path.join(self._dir, self._freight_insurance_file), usecols=[
            '子订单号',
            '类目名称',
            '结算金额(元)',
        ], dtype={
            '子订单号': str,
            '类目名称': str,
            '结算金额(元)': float,
        })
        df = df.rename(columns={
            '子订单号': 'SubTradeId',
            '类目名称': 'CategoryName',
            '结算金额(元)': 'FreightInsuranceFee',
        })
        if df.isnull().any().any():
            raise Exception('freight insurance null data: {}'.format(df.isnull().any()))
        id_df = df[['SubTradeId']].groupby('SubTradeId').size().reset_index(name='Count')
        abnormal_id_df = id_df[id_df['Count'] != 1]
        if len(abnormal_id_df) != 0:
            raise Exception('freight insurance sub trade id duplicated: {}'.format(abnormal_id_df))
        return df

    def loadAllSiteChannelPromotion(self): # 全站
        df: pd.DataFrame = self.loadXlsx(os.path.join(self._dir, self._all_site_channel_promotion_file), usecols=[
            '子订单号',
            '佣金率',
            '结算金额(元)'
        ], dtype={
            '子订单号': str,
            '佣金率': str,
            '结算金额(元)': float 
        })
        df = df.rename(columns={
            '子订单号': 'SubTradeId',
            '佣金率': 'ASPCommisionRate',
            '结算金额(元)': 'ASPCommisionFee'
        })
        if df.isnull().any().any():
            raise Exception('all site channel promotion null data: {}'.format(df.isnull().any()))
        id_df = df[['SubTradeId']].groupby('SubTradeId').size().reset_index(name='Count')
        abnormal_id_df = id_df[id_df['Count'] != 1]
        if len(abnormal_id_df) != 0:
            raise Exception('all site channel promotion sub trade id duplicated: {}'.format(abnormal_id_df))
        return df

    def loadOrders(self): # 下载的订单数据（拿不到子订单定价，交给后台写死上传）
        df: pd.DataFrame = self.loadXlsx(os.path.join(self._dir, self._orders_file), usecols=[
            '子订单编号', 
            '主订单编号', 
            '买家应付货款', # 定价的1.5倍
            '支付单号', # 没有单号说明没有付款就取消了订单，不要统计
            '买家应付邮费', 
            '总金额', # 总金额 = 卖家应付货款 + 卖家应付邮费
            '买家实际支付金额', 
            '子单实际支付金额', # 相同主订单金额应该等于多个子订单金额的和
            '订单状态', 
            '订单创建时间', 
            '订单付款时间', 
            '宝贝数量', 
            '供应商名称', 
            '退款金额',
            '颜色/尺码', # 即sku名
            '商品编码', # 购买的链接
            '物流单号',
        ], dtype={
            '子订单编号': str, 
            '主订单编号': str, 
            '买家应付货款': float, # 定价的1.5倍
            '支付单号': str, # 没有单号说明没有付款就取消了订单，不要统计
            '买家应付邮费': float, 
            '总金额': float, # 总金额 = 卖家应付货款 + 卖家应付邮费
            '买家实际支付金额': float, 
            '子单实际支付金额': float, # 相同主订单金额应该等于多个子订单金额的和
            '订单状态': str, 
            '订单创建时间': str, 
            '订单付款时间': str, 
            '宝贝数量': int, 
            '供应商名称': str, 
            '退款金额': float,
            '颜色/尺码': str, # 即sku名
            '商品编码': str, # 购买的链接
            '物流单号': str, 
        })
        df = df.rename(columns={
            '子订单编号': 'SubTradeId', 
            '主订单编号': 'TradeId', 
            '买家应付货款': 'AuctionTotalFee', # 定价的1.5倍
            '支付单号': 'PaymentId', # 没有单号说明没有付款就取消了订单，不要统计
            '买家应付邮费': 'PostFee', 
            '总金额': 'TotalFee', # 总金额 = 卖家应付货款 + 卖家应付邮费
            '买家实际支付金额': 'ActualTotalFee', 
            '子单实际支付金额': 'SubActualTotalFee', # 相同主订单金额应该等于多个子订单金额的和
            '订单状态': 'OrderStatus', 
            '订单创建时间': 'OrderCreatedTime', 
            '订单付款时间': 'OrderPaidTime', 
            '宝贝数量': 'Amount', 
            '供应商名称': 'SupplierName', 
            '退款金额': 'RefundFee',
            '颜色/尺码': 'SkuName', # 即sku名
            '商品编码': 'LinkId', # 购买的链接
            '物流单号': 'ExpressNo'
        })
        df['SkuName'] = df['SkuName'].apply(lambda x: x[len('颜色分类:'):] if x.startswith('颜色分类:') else x)
        df['OrderDate'] = df['OrderCreatedTime'].apply(lambda x: datetime.datetime.strptime(x.split(' ')[0], '%Y-%m-%d').strftime('%Y-%m-%d'))
        df['OrderCreatedTs'] = df['OrderCreatedTime'].apply(lambda x: int(datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').timestamp()))
        # 检查数据
        # 1. 主键必须唯一
        id_df = df[['SubTradeId']].groupby('SubTradeId').size().reset_index(name='Count')
        abnormal_id_df = id_df[id_df['Count'] != 1]
        if len(abnormal_id_df) != 0:
            raise Exception('sub trade id duplicated: {}'.format(abnormal_id_df))
        # 2. 金额自洽
        fee_df = df[['TradeId', 'AuctionTotalFee', 
                     'PostFee', 'TotalFee', 'ActualTotalFee',
                     'SubActualTotalFee']].copy()
        fee_df['AuctionPostMatched'] = fee_df[['AuctionTotalFee', 'PostFee', 'TotalFee']].apply(lambda x: abs(x['AuctionTotalFee'] + x['PostFee'] - x['TotalFee']) < 0.0001, axis=1)
        abnormal_fee_df = fee_df[fee_df['AuctionPostMatched'] != True]
        if len(abnormal_fee_df) != 0:
            raise Exception('auction + post != total: {}'.format(abnormal_fee_df))
        
        fee_df = fee_df[['TradeId', 'ActualTotalFee', 'SubActualTotalFee']].groupby('TradeId').agg({
            'ActualTotalFee': 'mean',
            'SubActualTotalFee': 'sum',
        })
        fee_df['SubMatched'] = fee_df[['ActualTotalFee', 'SubActualTotalFee']].apply(lambda x: abs(x['ActualTotalFee'] - x['SubActualTotalFee']) < 0.0001, axis=1)
        abnormal_fee_df = fee_df[fee_df['SubMatched'] != True]
        if len(abnormal_fee_df) != 0:
            raise Exception('sub sum != source trade: {}'.format(abnormal_fee_df))
        
        # 关注的数据
        df = df[['SubTradeId',
                 'TradeId',
                 'PaymentId',
                 'SubActualTotalFee', 
                 'RefundFee', 
                 'OrderStatus', 
                 'Amount', 
                 'SkuName', 
                 'LinkId',
                 'OrderCreatedTime', 'OrderCreatedTs', 'OrderDate',
                 'ExpressNo']]
        df['RefundFee'] = df['RefundFee'].fillna(float(0))
        df['ExpressNo'] = df['ExpressNo'].fillna('')
        df = df[df['PaymentId'].isnull() == False]
        # 不允许空数据
        if df.isnull().any().any():
            raise Exception('orders null data: {}'.format(df.isnull().any()))

        return df
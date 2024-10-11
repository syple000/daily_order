import pandas as pd
import numpy as np

class Summary(object):
    def __init__(self, report_df: pd.DataFrame):
        self._report_df = report_df

    def dumpLinkSkuTable(self, filepath: str) -> str: # 请不要调用，测试用
        df = self._report_df.groupby(['LinkId', 'SkuName']).agg({'Amount': 'sum'}).reset_index()[['LinkId', 'SkuName']]
        df['Cost'] = 4
        df['PostFee'] = 2
        df['Price'] = 7
        df = df.sort_values(['LinkId', 'SkuName'])
        df.to_csv(filepath, index=False)
        return filepath

    def calcProfit(self, link_sku_filepath: str) -> pd.DataFrame:
        df = pd.read_csv(link_sku_filepath, usecols=[
            'LinkId',
            'SkuName',
            'Cost',
            'PostFee',
            'Price',
        ], dtype={
            'LinkId': str,
            'SkuName': str,
            'Cost': float,
            'PostFee': float,
            'Price': float,
        })
        if df.isnull().any().any():
            raise Exception('link sku filepath null data: {}'.format(df.isnull().any()))
        df = pd.merge(self._report_df, df, how='left', on=['LinkId', 'SkuName'])
        if df['Cost'].isnull().any():
            raise Exception('link sku not found: {}'.format(df[df['Cost'].isnull()][['LinkId', 'SkuName']]))
        # 真实折扣计算（最后用户的订单支付/定价/数量）订单产生就可以计算
        df['RealDiscount'] = round(df['SubActualTotalFee'] / df['Price'] / df['Amount'], 2)
        # 货款计算。cost * amount + postfee。假设多件快递不变。必须有快递才有货款，一旦发出，即便退货也认为是亏损1
        df['FacPayment'] = df[['Cost', 'Amount', 'PostFee', 'ExpressNo']].apply(lambda x: float(0) if len(x['ExpressNo']) == 0 else x['Cost'] * x['Amount'] + x['PostFee'], axis=1)
        # 真实利润计算。必须结算后才有准确数据。数据产出后会不停纠正
        # 结算价（用户支付-退款-平台佣金TotalSettleAmount）- 先用后付（CreditBuyFee） - 营销推广（PlanFee）- 运费险（FreightInsuranceFee）- 全站（ASPCommisionFee）- 货款
        def calc_profit(x: pd.Series) -> float: # 如果结算，广告佣金等没结算，那么会更新有出入
            n = x['TotalSettleAmount']
            if np.isnan(n):
                if x['OrderStatus'] == '交易关闭':
                    n = float(0)
                else:
                    return np.nan
            if not np.isnan(x['CreditBuyFee']):
                n = n - x['CreditBuyFee']
            if not np.isnan(x['PlanFee']):
                n = n - x['PlanFee']
            if not np.isnan(x['FreightInsuranceFee']):
                n = n - x['FreightInsuranceFee']
            if not np.isnan(x['ASPCommisionFee']):
                n = n - x['ASPCommisionFee']
            n = n - x['FacPayment']
            return n
        df['Profit'] = df[['TotalSettleAmount', 'OrderStatus', 'CreditBuyFee', 'PlanFee', 'FreightInsuranceFee', 'ASPCommisionFee', 'FacPayment']].apply(lambda x: calc_profit(x), axis=1)
        return df
    
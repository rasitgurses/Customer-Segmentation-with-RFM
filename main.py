###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

from datetime import datetime as dt
import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

###############################################################
# GÖREVLER
###############################################################

# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
           # 1. flo_data_20K.csv verisini okuyunuz.
           # 2. Veri setinde
                     # a. İlk 10 gözlem,
                     # b. Değişken isimleri,
                     # c. Betimsel istatistik,
                     # d. Boş değer,
                     # e. Değişken tipleri, incelemesi yapınız.
           # 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
           # alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
           # 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
           # 5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.
           # 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
           # 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
           # 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.


#1-2
df_ = pd.read_csv('flo_data_20k.csv')
df = df_.copy()
df.head(10)
df.columns
df.describe()
df.isnull().sum()
df.dtypes

#3
df['customer_total_spend'] = df['customer_value_total_ever_online'] + df['customer_value_total_ever_offline']
df['customer_total_number_of_purchases'] = df['order_num_total_ever_online'] + df['order_num_total_ever_offline']

#4
df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])
df["first_order_date"] = pd.to_datetime(df["first_order_date"])
df["last_order_date"] = pd.to_datetime(df["last_order_date"])
print(df.dtypes)

#5
df.groupby('order_channel')['master_id'].nunique()
df.groupby('order_channel')['customer_total_spend'].mean()
df.groupby('order_channel')['customer_total_number_of_purchases'].mean()

#6
df['customer_total_spend'].sort_values(ascending=False).head(10)
df['customer_total_number_of_purchases'].sort_values(ascending=False).head(10)
#7
def pre_rfm(dataframe, csv=False):
    dataframe['customer_total_spend'] = dataframe['customer_value_total_ever_online'] + dataframe['customer_value_total_ever_offline']
    dataframe['customer_total_number_of_purchases'] = dataframe['order_num_total_ever_online'] + dataframe['order_num_total_ever_offline']
    dataframe["last_order_date_online"] = pd.to_datetime(dataframe["last_order_date_online"])
    dataframe["last_order_date_offline"] = pd.to_datetime(dataframe["last_order_date_offline"])
    dataframe["first_order_date"] = pd.to_datetime(dataframe["first_order_date"])
    dataframe["last_order_date"] = pd.to_datetime(dataframe["last_order_date"])
    print(dataframe.dtypes)
    dataframe.groupby('order_channel')['master_id'].nunique()
    dataframe.groupby('order_channel')['customer_total_spend'].mean()
    dataframe.groupby('order_channel')['customer_total_number_of_purchases'].mean()
    dataframe['customer_total_spend'].sort_values(ascending=False).head(10)
    dataframe['customer_total_number_of_purchases'].sort_values(ascending=False).head(10)
    print(dataframe.head())

    return()
df2 = pre_rfm(df)


# GÖREV 2: RFM Metriklerinin Hesaplanması

#hangi date daha geç onu bulma
df['total_last_order_date'] = df[['last_order_date', 'last_order_date_online']].max(axis=1)
df['total_last_order_date'].max()
today_date = dt.datetime(2021, 6, 2)
type(today_date)
type(df['total_last_order_date'])
#type değiştirme
from datetime import datetime
df['total_last_order_date'] = pd.to_datetime(df['total_last_order_date'])
df['total_last_order_date'] = df['total_last_order_date'].apply(lambda x: datetime.combine(x.date(), datetime.min.time()))
print(df['total_last_order_date'].dtype)

rfm = df.groupby('master_id').agg({'total_last_order_date': lambda total_last_order_date : (today_date - total_last_order_date.max()).days,
                                     'master_id' : lambda  master_id: master_id.nunique(),
                                     'customer_total_spend':lambda customer_total_spend:customer_total_spend.sum()})
rfm.columns = ['Recency', 'Frequency', 'Monetary']
rfm.head()
rfm = rfm[rfm['Monetary'] > 0]


# GÖREV 3: RF ve RFM Skorlarının Hesaplanması


rfm['Recency_Score'] = pd.qcut(rfm['Recency'], 5 , labels=[5, 4, 3, 2, 1])
rfm['Frequency_Score'] = pd.qcut(rfm['Frequency'].rank(method="first"), 5 , labels=[1, 2, 3, 4, 5])
rfm['Monetary_Score'] = pd.qcut(rfm['Monetary'], 5 , labels=[1, 2, 3, 4, 5])

rfm["RF_SCORE"] = (rfm['Recency_Score'].astype(str) +
                    rfm['Frequency_Score'].astype(str))
rfm.head()



# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}
rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

# GÖREV 5: Aksiyon zamanı!
           # 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

rfm[["segment", "Recency", "Frequency", "Monetary"]].groupby("segment").agg(["mean", "count"])

           # 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.
#a
rfm['segment'].unique()
new_df = pd.concat([rfm[rfm['segment'] == 'loyal_customers'], rfm[rfm['segment'] == 'championship']])
print(new_df)
new_df.to_csv("yeni_marka_hedef_müşteri_id")

#b
new_df2 = pd.concat([rfm[rfm['segment'] == 'about_to_sleep'], rfm[rfm['segment'] == 'promising'], rfm[rfm['segment'] == 'new_customers']])
print(new_df2)
new_df2.to_csv("indirim_hedef_müşteri_ids.csv")

                   # a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
                   # tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
                   # ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına
                   # yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.

                   # b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
                   # alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
                   # olarak kaydediniz.

# GÖREV 6: Tüm süreci fonksiyonlaştırınız.


def flo_rfm(dataframe, csv=False):
    def pre_rfm(dataframe, csv=False):
        dataframe['customer_total_spend'] = dataframe['customer_value_total_ever_online'] + dataframe[
            'customer_value_total_ever_offline']
        dataframe['customer_total_number_of_purchases'] = dataframe['order_num_total_ever_online'] + dataframe[
            'order_num_total_ever_offline']
        dataframe["last_order_date_online"] = pd.to_datetime(dataframe["last_order_date_online"])
        dataframe["last_order_date_offline"] = pd.to_datetime(dataframe["last_order_date_offline"])
        dataframe["first_order_date"] = pd.to_datetime(dataframe["first_order_date"])
        dataframe["last_order_date"] = pd.to_datetime(dataframe["last_order_date"])
        print(dataframe.dtypes)
        dataframe.groupby('order_channel')['master_id'].nunique()
        dataframe.groupby('order_channel')['customer_total_spend'].mean()
        dataframe.groupby('order_channel')['customer_total_number_of_purchases'].mean()
        dataframe['customer_total_spend'].sort_values(ascending=False).head(10)
        dataframe['customer_total_number_of_purchases'].sort_values(ascending=False).head(10)
        print(dataframe.head())

        return ()

df = df_.copy()

flo_rfm(df)




















    # hangi date daha geç onu bulma
    dataframe['total_last_order_date'] = dataframe[['last_order_date', 'last_order_date_online']].max(axis=1)
    dataframe['total_last_order_date'].max()
    today_date = dt.datetime(2021, 6, 2)

    from datetime import datetime
    dataframe['total_last_order_date'] = pd.to_datetime(dataframe['total_last_order_date'])
    dataframe['total_last_order_date'] = dataframe['total_last_order_date'].apply(
        lambda x: datetime.combine(x.date(), datetime.min.time()))


    rfm = df.groupby('master_id').agg(
        {'total_last_order_date': lambda total_last_order_date: (today_date - total_last_order_date.max()).days,
         'master_id': lambda master_id: master_id.nunique(),
         'customer_total_spend': lambda customer_total_spend: customer_total_spend.sum()})
    rfm.columns = ['Recency', 'Frequency', 'Monetary']

    rfm = rfm[rfm['Monetary'] > 0]

    # GÖREV 3: RF ve RFM Skorlarının Hesaplanması

    rfm['Recency_Score'] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm['Frequency_Score'] = pd.qcut(rfm['Frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm['Monetary_Score'] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])

    rfm["RF_SCORE"] = (rfm['Recency_Score'].astype(str) +
                       rfm['Frequency_Score'].astype(str))


    # GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }
    rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)
    print(rfm.head())
    return()

df = df_.copy()

flo_rfm(df)


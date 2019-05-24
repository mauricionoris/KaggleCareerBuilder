
import pandas as pd
import numpy as np

_datafolder = './data/'
df_apps = pd.read_csv(_datafolder  +  'apps.tsv', sep='\t')
df_apps = df_apps.drop(['ApplicationDate'],axis='columns')

mtx = {}
mtxx = {}

for w in range(1,8):
    mtx[w] = df_apps[(df_apps.WindowID == w) & (df_apps.Split == 'Test')]
    mtx[w] = mtx[w].drop(['WindowID', 'Split' ],axis='columns')
    mtxx[w] = mtx[w].groupby(by=['UserID'], as_index=False)['JobID'].count()
    mtxx[w].columns = ['UserID','Apps']

for w in range(1,8):
    print(mtx[w].shape)
    print(mtxx[w].shape)

for w in range(1,8):
    rowid =0
    df = pd.DataFrame([], columns = ['WindowID', 'User_1', 'User_2', 'Apps_1', 'Apps_2', 'Joint_Apps', 'Similarity'])
    print ("window:({})".format(w))
    for u1 in range(0,mtxx[w].shape[0]-1):
        user1 = mtxx[w].iloc[u1]
        apps_u1 = mtx[w][mtx[w]['UserID'] == user1['UserID']]
        for u2 in range(u1+1,mtxx[w].shape[0]):
            user2 = mtxx[w].iloc[u2]
            apps_u2 = mtx[w][mtx[w]['UserID'] == user2['UserID']]
            inner = apps_u1.merge(apps_u2, left_on='JobID', right_on='JobID', how = 'inner', suffixes=('_U1', '_U2'))
            union = apps_u1.merge(apps_u2, left_on='JobID', right_on='JobID', how = 'outer', suffixes=('_U1', '_U2'))
            s = (inner.shape[0] / union.shape[0])
            if s > 0:
                row = [w, user1['UserID'],user2['UserID'],user1['Apps'],user2['Apps'],inner.shape[0], s]
                print(row)
                df.loc[rowid] = row
                rowid += 1

    df.to_csv(_datafolder + 'rec_colab_users_{}.tsv'.format(w), sep='\t')

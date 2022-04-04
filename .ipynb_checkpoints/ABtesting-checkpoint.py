import pandahouse 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

connection = {
'host': 'https://clickhouse.lab.karpov.courses',
'password': 'dpo_python_2020',
'user': 'student',
'database': 'simulator_20220220'
}

q = '''
      Select 
          exp_group,
          user_id,
          countIf(user_id, action = 'like') as likes,
          countIf(user_id, action = 'view') as views,
          likes/views as CTR
      from simulator_20220220.feed_actions
      where toDate(time) >= '2022-02-06' and toDate(time) <= '2022-02-12' 
          and exp_group in (2,3)
      group by exp_group, user_id
     '''

df_AA = pandahouse.read_clickhouse(q, connection = connection)
# Task 1
p_values = []
for _ in range(10000):
  p_values.append(stats.ttest_ind(df_AA[df_AA.exp_group == 2].CTR.sample(500, replace = True), 
                  df_AA[df_AA.exp_group == 3].CTR.sample(500, replace = True),
                  equal_var = False)[1])

sns.set(rc = {'figure.figsize':(15,10)})
AAtest = sns.histplot(p_values)

df_p_values = pd.DataFrame({'p_values':p_values})
procent_of_error = df_p_values[df_p_values.p_values <= 0.05].count()*100/df_p_values.count()

# Task 2

q = '''
      Select 
          exp_group,
          user_id,
          countIf(user_id, action = 'like') as likes,
          countIf(user_id, action = 'view') as views,
          likes/views as CTR
      from simulator_20220220.feed_actions
      where toDate(time) >= '2022-02-13' and toDate(time) <= '2022-02-19' 
          and exp_group in (1,2)
      group by exp_group, user_id
     '''

df_AB = pandahouse.read_clickhouse(q, connection = connection)
# ttest
stats.ttest_ind(df_AB[df_AB.exp_group == 1].CTR, 
                df_AB[df_AB.exp_group == 2].CTR,
                equal_var = False)
# visualisation
groups = sns.histplot(data = df_AB,
                        x='CTR',
                        hue='exp_group',
                        palette = ['r', 'b'],
                        kde=False)
# Manna Whitneyu test
stats.mannwhitneyu(df_AB[df_AB.exp_group == 1].CTR, 
                    df_AB[df_AB.exp_group == 2].CTR,
                    alternative="two-sided")
# Smoothed CTR
Global_CTR_control = df_AB[df_AB.exp_group == 1].likes.sum()/df_AB[df_AB.exp_group == 1].views.sum()
Global_CTR_target = df_AB[df_AB.exp_group == 2].likes.sum()/df_AB[df_AB.exp_group == 2].views.sum()

def Get_Smoothed_CTR(Global_CTR,likes,views,a):
    smoothed_CTR = (likes + a*Global_CTR)/(views + a)
    return smoothed_CTR

df_control = df_AB[df_AB.exp_group == 1]
df_target = df_AB[df_AB.exp_group == 2]

df_control['smoothed_CTR'] = df_control.apply(Get_Smoothed_CTR(Global_CTR_control,df_control.likes,df_control.views,5), axis = 1)
df_target['smoothed_CTR'] = df_target.apply(Get_Smoothed_CTR(Global_CTR_target,df_target.likes,df_target.views,5), axis = 1)

stats.ttest_ind(df_control['smoothed_CTR'], 
                df_target['smoothed_CTR'],
                equal_var = False)

stats.mannwhitneyu(df_control['smoothed_CTR'], 
                    df_target['smoothed_CTR'],
                    alternative="two-sided")

sns.histplot(df_control['smoothed_CTR'])
sns.histplot(df_target['smoothed_CTR'])

# Poasson Butstrap



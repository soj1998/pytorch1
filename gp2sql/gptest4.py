'''
从历史数据里找牛市，牛市判断标准，涨幅在30%，熊市，降幅超过30%，间隔交易日天数

select left(rq1,4) nd, rq1,kp1,kp2,rq2,kp2,sp2,zf,jg,
row_number() over(partition by left(rq1,4) order by rq1) rn
from
(
select a.jyrq rq1,a.kp kp1,a.sp sp1,
b.jyrq rq2,b.kp kp2,b.sp sp2, (b.sp - a.sp ) / a.sp zf ,
b.rn - a.rn jg
from gp_szzs a,gp_szzs b
where a.jyrq < b.jyrq and (b.sp - a.sp ) / a.sp > 1
and a.jyrq > '2010-01-01'
and b.jyrq > '2010-01-01') a ;
'''
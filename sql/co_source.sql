select convert(date,[DO_Created]) as TradingDate, sum(OrderTotal) as 'sum(OrderTotal)', count(*) as 'count(*)'
from CustomerOrder where convert(date,[DO_Created]) = '${GET_DATE(-1)}'
group by convert(date,[DO_Created]) order by convert(date,[DO_Created])
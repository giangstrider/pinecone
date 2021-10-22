select to_date(conv) as "TradingDate", sum(OrderTotal), count(*)
from (select *, do_created as conv from KSFPA.OMS.CUSTOMERORDER
where year(do_created) $TIME{IN(YEAR(GET_DATE(-1)))}
) where  "TradingDate" $TIME{IN(GET_DATE(-1))}
 group by "TradingDate" order by "TradingDate"
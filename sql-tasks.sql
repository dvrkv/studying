/*
Найдите производителей принтеров, которые производят ПК с наименьшим объемом RAM и с самым быстрым процессором среди всех ПК, имеющих наименьший объем RAM. Вывести: Maker
*/

with min_ram as (
    select min(ram) as value
    from pc
)

select p.maker
from pc join product p on pc.model = p.model
where pc.ram = (select value from min_ram)
and pc.speed = (select max(speed) from pc where ram = (select value from min_ram))

/*
Вывести id клиентов, которые покупали молоко, но никогда не покупали сметану
purchases: customer_id | product_name
*/

select distinct customer_id
from purchases
where product_name = 'Молоко'
and customer_id not in (   
            select customer_id
            from purchases
            where product_name = 'Сметана')
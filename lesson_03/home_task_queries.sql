/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT COUNT(film_id) as films_number, c.name
FROM film_category as fc
         JOIN category as c
              ON c.category_id = fc.category_id
GROUP BY c.name
ORDER BY films_number desc;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT concat(a.first_name, ' ', a.last_name) actor_name, count(rental_id) as rental_number
FROM rental
         JOIN film_actor as fa
              on inventory_id = fa.film_id
         JOIN actor as a
              on a.actor_id = fa.actor_id
GROUP BY actor_name
ORDER BY rental_number desc
LIMIT 10;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
SELECT c.name, SUM(p.amount) total_payment
FROM film_category as fc
         JOIN rental as r
              ON r.inventory_id = fc.film_id
         JOIN payment as p
              ON r.rental_id = p.rental_id
         JOIN category as c
              ON c.category_id = fc.category_id
GROUP BY c.name
ORDER BY total_payment desc;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT f.title
FROM film as f
         LEFT JOIN inventory i
                   ON f.film_id = i.film_id
WHERE i.film_id IS NULL;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT concat(a.first_name, ' ', a.last_name) actor_name, sum(fc.film_id) film_number
FROM actor as a
         JOIN film_actor as fa
              ON fa.actor_id = a.actor_id
         JOIN film_category as fc
              ON fc.film_id = fa.film_id
         JOIN category as c
              ON fc.category_id = c.category_id
WHERE c.name = 'Children'
GROUP BY a.actor_id
ORDER BY film_number desc
LIMIT 3;


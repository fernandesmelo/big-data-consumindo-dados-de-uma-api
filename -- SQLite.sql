-- SQLite
SELECT c.name, COUNT(u.id) as total
FROM countries c
LEFT JOIN universities u ON u.country_id = c.id
GROUP BY c.id, c.name
ORDER BY total DESC;

SELECT u.name
FROM universities u
JOIN countries c ON c.id = u.country_id
WHERE c.name = 'Brazil'
ORDER BY u.name;

SELECT u.name, c.name AS country
FROM universities u
JOIN countries c ON c.id = u.country_id
WHERE u.name LIKE '%Tech%'
ORDER BY u.name;
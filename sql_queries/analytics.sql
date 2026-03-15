-- Top 10 most starred repositories

SELECT
    name,
    language,
    stargazers_count
FROM repos_processed
ORDER BY stargazers_count DESC
LIMIT 10;



-- Most popular programming languages

SELECT
    language,
    COUNT(*) AS repository_count
FROM repos_processed
GROUP BY language
ORDER BY repository_count DESC;



-- Average stars by language

SELECT
    language,
    AVG(stargazers_count) AS avg_stars
FROM repos_processed
GROUP BY language
ORDER BY avg_stars DESC;



-- Repositories created per year

SELECT
    substr(created_at,1,4) AS year,
    COUNT(*) AS repo_count
FROM repos_processed
GROUP BY substr(created_at,1,4)
ORDER BY year;



-- Repositories with most forks

SELECT
    name,
    forks_count
FROM repos_processed
ORDER BY forks_count DESC
LIMIT 10;


### Query 1: Count Clubs by Last Season  
This query counts the number of clubs per season in the **ClubDim_csv** table.

```sql
SELECT last_season, COUNT(*) AS num_clubs
FROM club_dim_csv
GROUP BY last_season
ORDER BY last_season;
```

![alt text](LoadDataToHive/Imgs/Img1.png)
---


### Query 2: Count Competitions by Type  
This query groups the competitions in **CompetitionDim_csv** by their type and returns the count for each.

```sql
SELECT type, COUNT(*) AS num_competitions
FROM competition_dim_csv
GROUP BY type;
```
![alt text](LoadDataToHive/Imgs/Img2.png)
---

### Query 3: CRank Clubs by Average Age Using a Window Function
This query uses a window (analytic) function to rank clubs in ClubDim_csv by their average_age in descending order.

```sql
SELECT club_id,
       name,
       average_age,
       RANK() OVER (ORDER BY average_age DESC) AS age_rank
FROM club_dim_csv
LIMIT 10;

```


### Query 4: Subquery to Select Clubs with Above-Average Squad Size
This query uses a subquery to filter clubs that have a squad size greater than the overall average squad size.
```sql
SELECT club_id,
       name,
       squad_size
FROM club_dim_csv
WHERE squad_size > (SELECT AVG(squad_size) FROM club_dim_csv)
LIMIT 10;

```



### Query 5: Use a CASE Expression on CompetitionDim_csv
This query categorizes competitions based on the type column using a CASE expression.

```sql
SELECT competition_sk,
       competition_id,
       name,
       CASE 
           WHEN type = 'domestic_cup' THEN 'Domestic Cup'
           WHEN type = 'international_cup' THEN 'International Cup'
           ELSE 'Other'
       END AS competition_category
FROM competition_dim_csv
LIMIT 10;

```



use movie_pipeline;
select title,rating from movies_data where rating=(select max(rating) from movies_data);

select genres,max(rating) from movies_data group by genres order by max(rating) desc limit 5;

select director,count(*) as total_movies from movies_data where director is not null group by director order by total_movies desc limit 1;

select year(release_date),avg(rating) from movies_data where year(release_date) is not null group by year(release_date);
